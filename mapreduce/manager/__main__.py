"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
import shutil


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        
        self.info = {
            'host': host,
            'port': port
            } # input information

        self.shutdown = False # whether the manager is shutdown

        self.workers = [] # record all registered workers
        self.worker_id = {} # map worker host and worker port to worker id
        self.avail_workers = [] # record all available workers

        self.job_count = 0 # number of total job received
        self.job_queue = [] # queue of job that waits for workers
        self.map_tasks = [] # map tasks for jobs
        self.reduce_tasks = [] # reduce tasks for jobs
        self.job_state = {'executing': False, 'job_type': 'map'} 
  
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # declare all locks
        self.shutdown_lock = threading.Lock()
        self.worker_lock= threading.Lock()
        self.worker_id_lock = threading.Lock()
        self.avail_worker_lock = threading.Lock()
        self.job_lock = threading.Lock()
        self.job_state_lock = threading.Lock()
        self.map_lock = threading.Lock()
        self.reduce_lock = threading.Lock()

        # thread for UDP server to listen for heartbeat
        UDP_server_thread = threading.Thread(target=self.UDP_server)
        UDP_server_thread.start()

        # thread to check whether there are any dead worker
        checkdead_thread = threading.Thread(target=self.checkdead)
        checkdead_thread.start()

        # thread to assign tasks
        assign_thread = threading.Thread(target=self.assign)
        assign_thread.start()

        # main thread to listen for messages
        self.TCP_server()

    def UDP_server(self):
        """Test UDP Socket Server."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as usock:
            # Bind the UDP socket to the server
            usock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            usock.bind((self.info['host'], self.info['port']))
            LOGGER.info('Listening on UDP port %s', self.info['port'])
            usock.settimeout(1)
            # No sock.listen() since UDP doesn't establish connections like TCP
            # Receive incoming UDP messages
            while True:
                print("UDP shutdown lock acquire")
                self.shutdown_lock.acquire()
                if self.shutdown:
                    self.shutdown_lock.release()
                    print("UDP shutdown lock release")
                    break
                self.shutdown_lock.release()
                print("UDP shutdown lock release ...")
                try:
                    message_bytes = usock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                message_type = message_dict.get('message_type')
                if message_type == 'heartbeat':
                    worker_host = message_dict.get('worker_host')
                    worker_port = message_dict.get('worker_port')
                    wkey = worker_host + str(worker_port)
                    self.worker_id_lock.acquire()
                    wid = self.worker_id.get(wkey, -1)
                    self.worker_id_lock.release()
                    if wid == -1:
                        LOGGER.warning('Heartbeat from unregistered worker')
                    else:
                        print("UDP worker lock acquire")
                        self.worker_lock.acquire()
                        self.workers[wid]['hbts'] = time.time()
                        self.worker_lock.release()
                        print("UDP worker lock release")


    def TCP_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tsock:
            # Bind the socket to the server
            tsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tsock.bind((self.info['host'], self.info['port']))
            tsock.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            LOGGER.info('Listening on TCP port %s', self.info['port'])
            tsock.settimeout(1)

            while True:
                print("TCP shutdown lock acquire")
                self.shutdown_lock.acquire()
                if (self.shutdown):
                    self.shutdown_lock.release()
                    print("TCP shutdown lock release")
                    break
                self.shutdown_lock.release()
                print("TCP shutdown lock release ...")
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = tsock.accept()
                except socket.timeout:
                    continue

                # Socket recv() will block for a maximum of 1 second.  If you omit
                # this, it blocks indefinitely, waiting for packets.
                clientsocket.settimeout(1)
                # Receive data, one chunk at a time.  If recv() times out before we
                # can read a chunk, then go back to the top of the loop and try
                # again.  When the client closes the connection, recv() returns
                # empty data, which breaks out of the loop.  We make a simplifying
                # assumption that the client will always cleanly close the
                # connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                message_type = message_dict.get('message_type')
                    
                if message_type == 'shutdown':
                    print("TCP shutdown worker lock acquire")
                    self.worker_lock.acquire()
                    for w in self.workers:
                        if not w['state'] == 'dead':
                            mapreduce.utils.TCP_client(w['host'], w['port'], {'message_type': 'shutdown'})
                    self.worker_lock.release()
                    print("TCP shutdown worker lock release")
                    print("TCP change shutdown lock acquire")
                    self.shutdown_lock.acquire()
                    self.shutdown = True
                    self.shutdown_lock.release()
                    print("TCP change shutdown lock release")
                
                elif message_type == 'register':
                    self.register(message_dict)
                
                elif message_type == 'new_manager_job':
                    self.new_job_request(message_dict)
                
                elif message_type == 'finished':
                    self.finished(message_dict)
        

    def register(self, message_dict):
        # {         
        #     "message_type" : "register",
        #     "worker_host" : string,
        #     "worker_port" : int,
        # }

        # add worker information into self.workers
        worker_host = message_dict.get('worker_host')
        worker_port = message_dict.get('worker_port')
        print("TCP register worker lock acquire")
        self.worker_lock.acquire()
        self.workers.append({'host': worker_host, 'port': worker_port, 'state': 'ready', 
        'hbts': time.time(), 'current_task': None})
        self.worker_lock.release()
        print("TCP register worker lock release")
        wkey = worker_host + str(worker_port)
        wid = len(self.worker_id)
        self.worker_id_lock.acquire()
        self.worker_id[wkey] = wid
        self.worker_id_lock.release()
        print("register avail worker lock acquire")
        self.avail_worker_lock.acquire()
        self.avail_workers.append({'host': worker_host, 'port': worker_port})
        self.avail_worker_lock.release()
        print("register avail worker lock release")
        LOGGER.info('Registered Worker (%s, %s)', worker_host, worker_port)
        # send register acknowledgement to worker
        mapreduce.utils.TCP_client(worker_host, worker_port, 
            {'message_type': 'register_ack', 'worker_host': worker_host, 'worker_port': worker_port})
        

    def new_job_request(self, message_dict):
        # {
        #     "message_type": "new_manager_job",
        #     "input_directory": string,
        #     "output_directory": string,
        #     "mapper_executable": string,
        #     "reducer_executable": string,
        #     "num_mappers" : int,
        #     "num_reducers" : int
        # }
        output_dir = message_dict.get('output_directory')
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.mkdir(output_dir)

        self.job_lock.acquire()
        self.job_queue.append({
            'job_id': self.job_count,
            'input_directory': message_dict.get('input_directory'),
            'output_directory': message_dict.get('output_directory'),
            'mapper_executable': message_dict.get('mapper_executable'),
            'reducer_executable': message_dict.get('reducer_executable'),
            'num_mappers': message_dict.get('num_mappers'),
            'num_reducers': message_dict.get('num_reducers')
        })
        self.job_lock.release()
        self.job_count += 1
        
        self.input_partition(message_dict)
        self.reduce_partition(message_dict)

        
        
        
    def input_partition(self, message_dict):
        # {
        #     "message_type": "new_manager_job",
        #     "input_directory": string,
        #     "output_directory": string,
        #     "mapper_executable": string,
        #     "reducer_executable": string,
        #     "num_mappers" : int,
        #     "num_reducers" : int
        # }
        input_dir = message_dict.get('input_directory')
        num_mappers = message_dict.get('num_mappers')
        files = sorted([f for f in os.listdir(input_dir)])
        after_partition = [{'task_type': 'map', 'task_id': i, 'task_files': []} for i in range(num_mappers)]
        for i in range(len(files)):
            k = i % num_mappers
            after_partition[k]['task_files'].append(files[i])
        self.map_lock.acquire()
        self.map_tasks.append(after_partition)
        self.map_lock.release()

    def reduce_partition(self, message_dict):
        # {
        #     "message_type": "new_manager_job",
        #     "input_directory": string,
        #     "output_directory": string,
        #     "mapper_executable": string,
        #     "reducer_executable": string,
        #     "num_mappers" : int,
        #     "num_reducers" : int
        # }
        num_mappers = message_dict.get('num_mappers')
        num_reducers = message_dict.get('num_reducers')
        after_partition = [{'task_type': 'reduce', 'task_id': i, 'task_files': []} for i in range(num_reducers)]
        for i in range(num_reducers):
            for j in range(num_mappers):
                file_name = 'maptask' + format(j, '05d') + '-part' + format(i, '05d')
                after_partition[i]['task_files'].append(file_name)
        self.reduce_lock.acquire()
        self.reduce_tasks.append(after_partition)
        self.reduce_lock.release()
    
    def assign_task(self, ready_worker, job_type, input_dir, output_dir):
        wkey = ready_worker['host'] + str(ready_worker['port'])
        self.worker_id_lock.acquire()
        wid = self.worker_id[wkey]
        self.worker_id_lock.release()
        print("assign task worker lock acquire")
        self.worker_lock.acquire()
        self.workers[wid]['state'] = 'busy'
        self.worker_lock.release()
        print("assign task worker lock release")
        if job_type == 'map':
            self.job_lock.acquire()
            job = self.job_queue[0]
            self.job_lock.release()
            self.map_lock.acquire()
            task = self.map_tasks[job['job_id']][0]
            self.map_lock.release()
            print("assign map task worker lock acquire")
            self.worker_lock.acquire()
            self.workers[wid]['current_task'] = task
            self.worker_lock.release()
            print("assign map task worker lock release")
            task_message = {
                'message_type': 'new_map_task',
                'task_id': task['task_id'],
                'executable': job['mapper_executable'],
                'input_paths': [os.path.join(input_dir, f) for f in task['task_files']],
                'output_directory': output_dir,
                'num_partitions': job['num_reducers'],
                'worker_host': ready_worker['host'],
                'worker_port': ready_worker['port']
            }
            self.map_lock.acquire()
            self.map_tasks[job['job_id']].pop(0)
            self.map_lock.release()
        else:
            self.job_lock.acquire()
            job = self.job_queue[0]
            self.job_lock.release()
            self.reduce_lock.acquire()
            task = self.reduce_tasks[job['job_id']][0]
            self.reduce_lock.release()
            print("assign reduce task worker lock acquire")
            self.worker_lock.acquire()
            self.workers[wid]['current_task'] = task
            self.worker_lock.release()
            print("assign reduce task worker lock release")
            task_message = {
                'message_type': 'new_reduce_task',
                'task_id': task['task_id'],
                'executable': job['reducer_executable'],
                'input_paths': [os.path.join(input_dir, f) for f in task['task_files']],
                'output_directory': output_dir,
                'worker_host': ready_worker['host'],
                'worker_port': ready_worker['port']
            }
            self.reduce_lock.acquire()
            self.reduce_tasks[job['job_id']].pop(0)
            self.reduce_lock.release()
        mapreduce.utils.TCP_client(ready_worker['host'], ready_worker['port'], task_message)
    
    def assign(self):
        while True:
            time.sleep(0.1)
            print("assign shutdown lock acquire")
            self.shutdown_lock.acquire()
            if self.shutdown:
                self.shutdown_lock.release()
                print("assgin shutdown lock release")
                break
            self.shutdown_lock.release()
            print("assign shutdown lock release ...")
            print("assign job lock acquire")
            self.job_lock.acquire()
            job_len = len(self.job_queue)
            self.job_lock.release()
            print("assign job lock release")
            if job_len != 0:
                print("assign job lock acquire")
                self.job_lock.acquire()
                curr_job = self.job_queue[0]
                self.job_lock.release()
                print("assign job lock release")
                job_id = curr_job['job_id']
                pfx = 'mapreduce-shared-job' + format(job_id, '05d') + '-'
                print("Call tempfile.TemporaryDirectory")
                with tempfile.TemporaryDirectory(prefix=pfx) as tmpdir:
                    LOGGER.info('Created %s', tmpdir)
                    while True:
                        time.sleep(0.1)
                        print("while in the tempfile.TemporaryDirectory")
                        self.shutdown_lock.acquire()
                        if self.shutdown:
                            self.shutdown_lock.release()
                            print("assgin shutdown lock release")
                            break
                        self.shutdown_lock.release()
                        self.map_lock.acquire()
                        map_task_len = len(self.map_tasks[job_id])
                        self.map_lock.release()
                        self.reduce_lock.acquire()
                        reduce_task_len = len(self.reduce_tasks[job_id])
                        self.reduce_lock.release()
                        if  map_task_len != 0:
                            self.job_state_lock.acquire()
                            if self.job_state['executing'] == False:
                                self.job_state['executing'] = True
                                LOGGER.info('Begin Map Stage')
                            self.job_state_lock.release()
                            input_dir = curr_job['input_directory']
                            output_dir = tmpdir
                            print("assign map avail worker lock acquire")
                            self.avail_worker_lock.acquire()
                            if len(self.avail_workers) == 0:
                                self.avail_worker_lock.release()
                                print("assign map avail worker lock release")
                                continue
                            ready_worker = self.avail_workers[0]
                            self.avail_workers.pop(0)
                            self.avail_worker_lock.release()
                            print("assign map avail worker lock release ...")
                            self.assign_task(ready_worker, 'map', input_dir, output_dir)
                        elif (reduce_task_len != 0): 
                            self.job_state_lock.acquire()
                            if self.job_state['job_type'] == 'reduce':
                                if (self.job_state['executing'] == False):
                                    self.job_state['executing'] = True
                                    LOGGER.info('Begin Reduce Stage')
                                self.job_state_lock.release()
                                input_dir = tmpdir
                                output_dir = curr_job['output_directory']
                                print("assign reduce avail worker lock acquire")
                                self.avail_worker_lock.acquire()
                                if len(self.avail_workers) == 0:
                                    self.avail_worker_lock.release()
                                    print("assign reduce avail worker lock release")
                                    continue
                                ready_worker = self.avail_workers[0]
                                self.avail_workers.pop(0)
                                self.avail_worker_lock.release()
                                print("assign reduce avail worker lock release ...")
                                self.assign_task(ready_worker, 'reduce', input_dir, output_dir)
                            else:
                                self.job_state_lock.release()    
                        else:
                            self.job_lock.acquire()
                            new_job_len = len(self.job_queue)
                            self.job_lock.release()
                            if (new_job_len == job_len):
                                continue
                            else:
                                break
                    LOGGER.info("Removed %s", tmpdir)
                    LOGGER.info('Finished job. Output directory: %s', curr_job['output_directory'])

    def finished(self, message_dict):
        LOGGER.debug('received\n%s', json.dumps(message_dict, indent=2))
        worker_host = message_dict.get('worker_host')
        worker_port = message_dict.get('worker_port')
        wkey = worker_host + str(worker_port)
        self.worker_id_lock.acquire()
        wid = self.worker_id[wkey]
        self.worker_id_lock.release()
        print("finish worker lock acquire")
        self.worker_lock.acquire()
        self.workers[wid]['state'] = 'ready'
        self.workers[wid]['current_task'] = None
        self.worker_lock.release()
        print("finish worker lock release")
        print("finish avail worker lock acquire")
        self.avail_worker_lock.acquire()
        self.avail_workers.append({'host': worker_host, 'port': worker_port})
        self.avail_worker_lock.release()
        print("finish avail worker lock release")
        self.job_state_lock.acquire()
        job_state = self.job_state['job_type']
        self.job_state_lock.release()
        self.job_lock.acquire()
        curr_job = self.job_queue[0]
        self.job_lock.release()
        self.map_lock.acquire()
        map_task_len = len(self.map_tasks[curr_job['job_id']])
        self.map_lock.release()
        self.reduce_lock.acquire()
        reduce_task_len = len(self.reduce_tasks[curr_job['job_id']])
        self.reduce_lock.release()
        if (job_state == 'map' and map_task_len == 0):
            print("finish map task worker lock acquire")
            self.worker_lock.acquire()
            for w in self.workers:
                if w['state'] == 'busy':
                    self.worker_lock.release()
                    return
            LOGGER.info('End Map Stage')
            print("finish map task avail worker lock acquire")
            self.avail_worker_lock.acquire()
            self.avail_workers = []
            for w in self.workers:
                if (w['state'] == 'ready'):
                    self.avail_workers.append({'host': w['host'], 'port': w['port']})
            self.avail_worker_lock.release()
            print("finish map task avail worker lock release")
            self.worker_lock.release()
            print("finish map task worker lock release")
            self.job_state_lock.acquire()
            self.job_state['executing'] = False
            self.job_state['job_type'] = 'reduce'
            self.job_state_lock.release()
        elif (job_state  == 'reduce' and reduce_task_len == 0):
            print("finish reduce task worker lock acquire")
            self.worker_lock.acquire()
            for w in self.workers:
                if w['state'] == 'busy':
                    self.worker_lock.release()
                    return
            LOGGER.info('End Reduce Stage')
            print("finish reduce task avail worker lock acquire")
            self.avail_worker_lock.acquire()
            self.avail_workers = []
            for w in self.workers:
                if (w['state'] == 'ready'):
                    self.avail_workers.append({'host': w['host'], 'port': w['port']})
            self.avail_worker_lock.release()
            print("finish reduce task avail worker lock release")
            self.worker_lock.release()
            print("finish reduce task worker lock release")
            self.job_state_lock.acquire()
            self.job_state['executing'] = False
            self.job_state['job_type'] = 'map'
            self.job_state_lock.release()
            self.job_lock.acquire()
            self.job_queue.pop(0)
            self.job_lock.release()
    
    def checkdead(self):
        while True:
            time.sleep(0.1)
            print("checkdead shutdown lock acquire")
            self.shutdown_lock.acquire()
            if self.shutdown:
                self.shutdown_lock.release()
                print("checkdead shutdown lock release")
                break
            self.shutdown_lock.release()
            print("checkdead shutdown lock release ...")
            print("checkdead worker lock acquire")
            self.worker_lock.acquire()
            for w in self.workers:
                print("worker heartbeat", time.time() - w['hbts'], w)
                if time.time() - w['hbts'] > 10:
                    if w['state'] == 'ready':
                        print("checkdead avail worker lock acquire")
                        self.avail_worker_lock.acquire()
                        self.avail_workers.remove({'host': w['host'], 'port': w['port']})
                        self.avail_worker_lock.release()
                        print("checkdead avail worker lock release")
                    elif w['state'] == 'busy':
                        task = w['current_task']
                        w['current_task'] = None
                        print("dead worker", task)
                        if task['task_type'] == 'map':
                            self.job_lock.acquire()
                            curr_job = self.job_queue[0]
                            self.job_lock.release()
                            self.map_lock.acquire()
                            self.map_tasks[curr_job['job_id']].append(task)
                            self.map_lock.release()
                        elif task['task_type'] == 'reduce':
                            self.job_lock.acquire()
                            curr_job = self.job_queue[0]
                            self.job_lock.release()
                            self.reduce_lock.acquire()
                            self.reduce_tasks[curr_job['job_id']].append(task)
                            self.reduce_lock.release()
                    w['state'] = 'dead'
            self.worker_lock.release()
            print("checkdead worker lock release")




@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
