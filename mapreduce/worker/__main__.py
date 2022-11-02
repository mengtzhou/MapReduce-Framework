"""MapReduce framework Worker node."""
import heapq
import os
import logging
import json
import tempfile
import time
from venv import create
import click
import mapreduce.utils
import threading
import socket
import shutil
import subprocess
import hashlib
import contextlib


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self.info = {
            'host': host,
            'port': port,
            'manager_host': manager_host,
            'manager_port': manager_port
            }
        self.shutdown = False
        LOGGER.info("Starting worker:%s", port)
        LOGGER.info("PWD %s", os.getcwd())

        # # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register_ack",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # all lockers needed 
        self.shutdown_lock = threading.Lock()

        # start TCP server
        self.TCP_server()

    def TCP_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tsock:
            # Bind the socket to the server
            tsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tsock.bind((self.info['host'], self.info['port']))
            tsock.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            tsock.settimeout(1)
            mapreduce.utils.TCP_client(
                self.info['manager_host'], 
                self.info['manager_port'], 
                    {
                        'message_type': 'register', 
                        'worker_host': self.info['host'], 
                        'worker_port': self.info['port']
                    }
                )
            while True:
                time.sleep(0.1)
                self.shutdown_lock.acquire()
                if self.shutdown:
                    self.shutdown_lock.release()
                    break
                self.shutdown_lock.release()
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
                    self.shutdown_lock.acquire()
                    self.shutdown = True
                    self.shutdown_lock.release()
                
                elif message_type == 'register_ack':
                    self.hb_thread = threading.Thread(target=self.send_heartbeat, args=(message_dict,))
                    self.hb_thread.start()
                
                elif message_type == 'new_map_task':
                    self.mapping(message_dict)

                elif message_type == 'new_reduce_task':
                    self.reducing(message_dict)
                

    def send_heartbeat(self, message_dict):
        LOGGER.debug('received %s', json.dumps(message_dict, indent=2))
        LOGGER.info('Starting heartbeat thread')
        LOGGER.info('Connected to Manager:%s', self.info['manager_port'])
        while True:
            self.shutdown_lock.acquire()
            if self.shutdown:
                self.shutdown_lock.release()
                break
            self.shutdown_lock.release()
            mapreduce.utils.UDP_client(self.info['manager_host'], self.info['manager_port'], 
            {'message_type': 'heartbeat', 'worker_host': self.info['host'], 'worker_port': self.info['port']})
            print("send heartbeat")
            time.sleep(2)
    
    def mapping(self, message_dict):
        # {
        #     "message_type": "new_map_task",
        #     "task_id": int,
        #     "input_paths": [list of strings],
        #     "executable": string,
        #     "output_directory": string,
        #     "num_partitions": int,
        #     "worker_host": string,
        #     "worker_port": int
        # }
        task_id = message_dict.get('task_id')
        num_partitions = message_dict.get('num_partitions')
        pfx = 'mapreduce-local-task' + format(task_id, '05d') + '-'
        with tempfile.TemporaryDirectory(prefix=pfx) as tmpdir:
            LOGGER.info('Created %s', tmpdir)
            for input_path in message_dict.get('input_paths'):
                LOGGER.info('Executed %s input=%s', message_dict.get('executable'), input_path)
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [message_dict.get('executable')], 
                        stdin=infile, 
                        stdout=subprocess.PIPE,
                        universal_newlines=True
                    ) as map_process:
                        for line in map_process.stdout:
                            key, _ = line.split('\t')
                            hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition = keyhash % num_partitions
                            fname = 'maptask' + format(task_id, '05d') + '-part' + format(partition, '05d')
                            fname = os.path.join(tmpdir, fname)
                            file = open(fname, 'a+')
                            file.write(line)
                            file.close()
            # sort each line in files
            for partition in range(num_partitions):
                fname = 'maptask' + format(task_id, '05d') + '-part' + format(partition, '05d')
                fname = os.path.join(tmpdir, fname)
                data = [(line.strip('\n').split('\t')[0], int(line.strip('\n').split('\t')[1])) for line in open(fname, 'r')]
                sorted_data = sorted(data, key = lambda t: (t[0], t[1]))
                file = open(fname, 'w')
                for pair in sorted_data:
                    file.write(pair[0] + '\t'+str(pair[1]) + '\n')
                file.close()
                LOGGER.info('Sorted %s', fname)
            # move files from tmp dir to manager dir
            for partition in range(num_partitions):
                fname = 'maptask' + format(task_id, '05d') + '-part' + format(partition, '05d')
                old_fname = os.path.join(tmpdir, fname)
                new_fname = os.path.join(message_dict.get('output_directory'), fname)
                shutil.move(old_fname, new_fname)
                LOGGER.info('Moved %s -> %s', old_fname, new_fname)
            LOGGER.info('Removed %s', tmpdir)
        mapreduce.utils.TCP_client(self.info['manager_host'], self.info['manager_port'], 
            {
                'message_type': 'finished',
                'task_id': task_id,
                'worker_host': self.info['host'],
                'worker_port': self.info['port']
            }
        )
                                

    def reducing(self, message_dict):
        # {
        #     "message_type": "new_reduce_task",
        #     "task_id": int,
        #     "executable": string,
        #     "input_paths": [list of strings],
        #     "output_directory": string,
        #     "worker_host": string,
        #     "worker_port": int
        # }
        task_id = message_dict.get('task_id')
        pfx = 'mapreduce-local-task' + format(task_id, '05d') + '-'
        with tempfile.TemporaryDirectory(prefix=pfx) as tmpdir:
            LOGGER.info('Created %s', tmpdir)
            output_path = os.path.join(tmpdir, 'part'+format(task_id, '05d'))
            with contextlib.ExitStack() as stack:
                files = [stack.enter_context(open(fn)) for fn in message_dict.get('input_paths')]
                merge_file = heapq.merge(*files)
                with open(output_path, 'a+') as outfile:
                    with subprocess.Popen(
                        [message_dict.get('executable')],
                        universal_newlines=True,
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                    ) as reduce_process:
                        # Pipe input to reduce_process
                        for line in merge_file:
                            reduce_process.stdin.write(line)
            new_output_path = os.path.join(message_dict.get('output_directory'), 'part-'+format(task_id, '05d'))
            shutil.move(output_path, new_output_path)
            LOGGER.info('Moved %s -> %s', output_path, new_output_path)
            LOGGER.info('Removed %s', tmpdir)
        mapreduce.utils.TCP_client(self.info['manager_host'], self.info['manager_port'], 
            {
                'message_type': 'finished',
                'task_id': task_id,
                'worker_host': self.info['host'],
                'worker_port': self.info['port']
            }
        )
        

        


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
