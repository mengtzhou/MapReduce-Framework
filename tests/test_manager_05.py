"""See unit test function docstring."""

import json
import time
import shutil
import tempfile
import utils
from utils import TESTDATA_DIR
import mapreduce


def worker_message_generator(mock_socket, tmp_path):
    """Fake Worker messages."""
    # Two Workers register
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode("utf-8")
    yield None
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3002,
    }).encode("utf-8")
    yield None

    # User submits new job
    yield json.dumps({
        "message_type": "new_manager_job",
        "input_directory": TESTDATA_DIR/"input",
        "output_directory": tmp_path,
        "mapper_executable": TESTDATA_DIR/"exec/wc_map_slow.sh",
        "reducer_executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "num_mappers": 2,
        "num_reducers": 1
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Manager to create directories
    tmpdir_job0 = \
        utils.wait_for_exists_glob(f"{tmp_path}/mapreduce-shared-job00000-*")

    # Simulate files created by Worker
    shutil.copytree(
        TESTDATA_DIR/"test_manager_05/intermediate/job-0",
        tmpdir_job0,
        dirs_exist_ok=True,
    )

    # Wait for Manager to send two map messages because num_mappers=2
    utils.wait_for_map_messages(mock_socket, num=2)

    # Status finished message from one mapper
    yield json.dumps({
        "message_type": "finished",
        "task_id": 0,
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode("utf-8")
    yield None

    # Wait for Manager to realize that Worker 3002 isn't sending heartbeat
    # messages anymore.  It should then reassign Worker 3002's map task to
    # Worker 3001.
    #
    # We expect a grand total of 3 map messages.  The first two messages are
    # from the Manager assigning two map tasks to two Workers.  The third
    # message is from the reassignment.
    #
    # Transfer control back to solution under test in between each check for
    # map messages.
    for _ in utils.wait_for_map_messages_async(mock_socket, num=3):
        yield None

    # Status finished messages from one mapper.  This Worker was reassigned the
    # task that the dead Worker failed to complete.
    yield json.dumps({
        "message_type": "finished",
        "task_id": 1,
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode("utf-8")
    yield None

    # Wait for Manager to send reduce job message
    utils.wait_for_reduce_messages(mock_socket)

    # Reduce job status finished
    yield json.dumps({
        "message_type": "finished",
        "task_id": 0,
        "worker_host": "localhost",
        "worker_port": 3001,
    }).encode("utf-8")
    yield None

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def worker_heartbeat_generator():
    """Fake heartbeat messages from one good Worker and one dead Worker."""
    # Worker 3002 sends a single heartbeat
    yield json.dumps({
        "message_type": "heartbeat",
        "worker_host": "localhost",
        "worker_port": 3002,
    }).encode("utf-8")

    # Worker 3001 continuously sends, but Worker 3002 stops.  This should cause
    # the Manager to detect Worker 3002 as dead.
    while True:
        yield json.dumps({
            "message_type": "heartbeat",
            "worker_host": "localhost",
            "worker_port": 3001,
        }).encode("utf-8")
        time.sleep(utils.TIME_BETWEEN_HEARTBEATS)


def test_dead_worker(mocker, tmp_path):
    """Verify Manager handles a dead Worker.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.

    Note: 'tmp_path' is a fixture provided by the pytest-mock package.
    This fixture creates a temporary directory for use within this test.

    See https://docs.pytest.org/en/6.2.x/tmpdir.html for more info.
    """
    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch("socket.socket")
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = worker_message_generator(
        mock_socket,
        tmp_path,
    )

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Mock socket library functions to return heartbeat messages
    mock_socket.return_value.__enter__.return_value.recv.side_effect = \
        worker_heartbeat_generator()

    # Set the location where the Manager's temporary directory
    # will be created.
    tempfile.tempdir = tmp_path

    # Spy on tempfile.TemporaryDirectory so that we can determine the name
    # of the directory that was created.
    mock_tmpdir = mocker.spy(tempfile.TemporaryDirectory, "__init__")

    # Run student Manager code.  When student Manager calls recv(), it will
    # return the faked responses configured above.
    try:
        mapreduce.manager.Manager("localhost", 6000)
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify that the correct number of TemporaryDirectories was used.
    assert mock_tmpdir.call_count == 1, \
        "Expected to see call to `tempfile.TemporaryDirectory(...)`"

    # Find the name of the temporary directory.
    tmpdir_job0 = utils.get_tmpdir_name(mock_tmpdir)

    # Verify messages sent by the Manager
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs --log-cli-level=info tests/test_manager_X.py
    messages = utils.get_messages(mock_socket)
    assert messages == [
        {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 3002,
        },
        {
            "message_type": "new_map_task",
            "task_id": 0,
            "executable": str(TESTDATA_DIR/"exec/wc_map_slow.sh"),
            "input_paths": [
                str(TESTDATA_DIR/"input/file01"),
                str(TESTDATA_DIR/"input/file03"),
                str(TESTDATA_DIR/"input/file05"),
                str(TESTDATA_DIR/"input/file07"),
            ],
            "output_directory": tmpdir_job0,
            "num_partitions": 1,
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "new_map_task",
            "task_id": 1,
            "executable": str(TESTDATA_DIR/"exec/wc_map_slow.sh"),
            "input_paths": [
                str(TESTDATA_DIR/"input/file02"),
                str(TESTDATA_DIR/"input/file04"),
                str(TESTDATA_DIR/"input/file06"),
                str(TESTDATA_DIR/"input/file08"),
            ],
            "output_directory": tmpdir_job0,
            "num_partitions": 1,
            "worker_host": "localhost",
            "worker_port": 3002,
        },
        {
            "message_type": "new_map_task",
            "task_id": 1,
            "executable": str(TESTDATA_DIR/"exec/wc_map_slow.sh"),
            "input_paths": [
                str(TESTDATA_DIR/"input/file02"),
                str(TESTDATA_DIR/"input/file04"),
                str(TESTDATA_DIR/"input/file06"),
                str(TESTDATA_DIR/"input/file08"),
            ],
            "output_directory": tmpdir_job0,
            "num_partitions": 1,
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "new_reduce_task",
            "task_id": 0,
            "executable": str(TESTDATA_DIR/"exec/wc_reduce.sh"),
            "input_paths": [
                f"{tmpdir_job0}/maptask00000-part00000",
                f"{tmpdir_job0}/maptask00001-part00000"
            ],
            "output_directory": str(tmp_path),
            "worker_host": "localhost",
            "worker_port": 3001,
        },
        {
            "message_type": "shutdown",
        },
    ]
