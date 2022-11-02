"""See unit test function docstring."""

import json
import shutil
from pathlib import Path
import utils
import mapreduce
from utils import TESTDATA_DIR


def manager_message_generator(mock_socket, tmp_path):
    """Fake Manager messages."""
    # Worker Register
    utils.wait_for_register_messages(mock_socket)
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 6001,
    }).encode("utf-8")
    yield None

    # Reduce task
    yield json.dumps({
        "message_type": "new_reduce_task",
        "task_id": 0,
        "executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "input_paths": [
            f"{tmp_path}/maptask00000-part00000",
            f"{tmp_path}/maptask00001-part00000",
        ],
        "output_directory": tmp_path,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish reduce job
    utils.wait_for_status_finished_messages(mock_socket)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_reduce_two_inputs(mocker, tmp_path):
    """Verify Worker correctly completes a reduce task with two input files.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.

    Note: 'tmp_path' is a fixture provided by the pytest-mock package.
    This fixture creates a temporary directory for use within this test.

    See https://docs.pytest.org/en/6.2.x/tmpdir.html for more info.
    """
    shutil.copyfile(
        TESTDATA_DIR/"test_worker_07/maptask00000-part00000",
        f"{tmp_path}/maptask00000-part00000",
    )
    shutil.copyfile(
        TESTDATA_DIR/"test_worker_07/maptask00001-part00000",
        f"{tmp_path}/maptask00001-part00000",
    )

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch("socket.socket")
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = manager_message_generator(mock_socket,
                                                                  tmp_path)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Run student Worker code.  When student Worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            host="localhost",
            port=6001,
            manager_host="localhost",
            manager_port=6000,
        )
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the Worker
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs --log-cli-level=info tests/test_worker_X.py
    all_messages = utils.get_messages(mock_socket)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 0,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]

    # Verify Reduce Stage output
    with Path(f"{tmp_path}/part-00000").open(encoding="utf-8") \
         as infile:
        reduceout = infile.readlines()
    assert reduceout == [
        "\t2\n",
        "bye\t1\n",
        "hello\t2\n"
    ]
