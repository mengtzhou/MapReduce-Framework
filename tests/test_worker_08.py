"""See unit test function docstring."""

import json
import tempfile
from pathlib import Path
import mapreduce
import utils
from utils import TESTDATA_DIR


def manager_message_generator(mock_socket, tmp_path):
    """Fake Manager messages."""
    # Worker register
    utils.wait_for_register_messages(mock_socket)
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 6001,
    }).encode("utf-8")
    yield None

    # Map task 1
    yield json.dumps({
        "message_type": "new_map_task",
        "task_id": 0,
        "executable": TESTDATA_DIR/"exec/wc_map.sh",
        "input_paths": [
            TESTDATA_DIR/"input/file01",
        ],
        "output_directory": tmp_path,
        "num_partitions": 2,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish map job
    utils.wait_for_status_finished_messages(mock_socket)

    # Map task 2
    yield json.dumps({
        "message_type": "new_map_task",
        "task_id": 1,
        "executable": TESTDATA_DIR/"exec/wc_map.sh",
        "input_paths": [
            TESTDATA_DIR/"input/file02",
        ],
        "output_directory": tmp_path,
        "num_partitions": 2,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish the second map task. There should now be two
    # status=finished messages in total, One from each map task.
    utils.wait_for_status_finished_messages(mock_socket, num=2)

    # Reduce task 1
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

    # Wait for Worker to finish reduce task. There should now be three
    # finished messages in total: two from the map tasks and one
    # from this reduce task.
    utils.wait_for_status_finished_messages(mock_socket, num=3)

    # Reduce task 2
    yield json.dumps({
        "message_type": "new_reduce_task",
        "task_id": 1,
        "executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "input_paths": [
            f"{tmp_path}/maptask00000-part00001",
            f"{tmp_path}/maptask00001-part00001",
        ],
        "output_directory": tmp_path,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish final reduce task.
    utils.wait_for_status_finished_messages(mock_socket, num=4)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_map_reduce(mocker, tmp_path):
    """Verify Worker can map and reduce.

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
    mockclientsocket.recv.side_effect = manager_message_generator(mock_socket,
                                                                  tmp_path)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Spy on tempfile.TemporaryDirectory so that we can ensure that it has
    # the correct number of calls.
    mock_tmpdir = mocker.spy(tempfile.TemporaryDirectory, "__init__")

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

    # Verify calls to tempfile.TemporaryDirectory()
    assert mock_tmpdir.call_count == 4, (
        "Expected 4 calls to tempfile.TemporaryDirectory(...), received "
        f"{mock_tmpdir.call_count}"
    )
    assert mock_tmpdir.call_args_list == [
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00000-"),
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00001-"),
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00000-"),
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00001-"),
    ], "Incorrect calls to tempfile.TemporaryDirectory"

    # Verify messages sent by the Worker
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs --log-cli-level=info tests/test_worker_X.py
    messages = utils.filter_not_heartbeat_messages(
        utils.get_messages(mock_socket)
    )
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
        {
            "message_type": "finished",
            "task_id": 1,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 0,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 1,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]

    # Verify Map Stage output
    with Path(f"{tmp_path}/maptask00000-part00000") \
            .open(encoding="utf-8") as infile:
        mapout01 = infile.readlines()

    with Path(f"{tmp_path}/maptask00000-part00001") \
            .open(encoding="utf-8") as infile:
        mapout02 = infile.readlines()

    assert mapout01 == [
        "\t1\n",
        "bye\t1\n",
        "hello\t1\n",
    ]

    assert mapout02 == [
        "world\t1\n",
        "world\t1\n",
    ]

    with Path(f"{tmp_path}/maptask00001-part00000") \
            .open(encoding="utf-8") as infile:
        mapout03 = infile.readlines()

    with Path(f"{tmp_path}/maptask00001-part00001") \
            .open(encoding="utf-8") as infile:
        mapout04 = infile.readlines()

    assert mapout03 == [
        "\t1\n",
        "hello\t1\n",
    ]

    assert mapout04 == [
        "goodbye\t1\n",
        "hadoop\t1\n",
        "hadoop\t1\n",
    ]

    # Verify Reduce Stage output
    with Path(f"{tmp_path}/part-00000") \
            .open(encoding="utf-8") as infile:
        reduce01out = infile.readlines()
    assert reduce01out == [
        "\t2\n",
        "bye\t1\n",
        "hello\t2\n",
    ]

    with Path(f"{tmp_path}/part-00001") \
            .open(encoding="utf-8") as infile:
        reduce02out = infile.readlines()
    assert reduce02out == [
        "goodbye\t1\n",
        "hadoop\t2\n",
        "world\t2\n",
    ]
