import subprocess
import sys

from dagster._api.list_repositories import sync_list_repositories_grpc
from dagster._grpc.client import DagsterGrpcClient
from dagster._grpc.server import wait_for_grpc_server
from dagster._utils import file_relative_path, find_free_port


def test_load_grpc_server(capfd):
    port = find_free_port()
    python_file = file_relative_path(__file__, "grpc_repo.py")

    subprocess_args = [
        "dagster",
        "api",
        "grpc",
        "--port",
        str(port),
        "--python-file",
        python_file,
    ]

    process = subprocess.Popen(subprocess_args)

    try:

        client = DagsterGrpcClient(port=port, host="localhost")

        wait_for_grpc_server(process, client, subprocess_args)
        assert client.ping("foobar") == "foobar"

        list_repositories_response = sync_list_repositories_grpc(client)
        assert list_repositories_response.entry_point == ["dagster"]
        assert list_repositories_response.executable_path == sys.executable

        subprocess.check_call(["dagster", "api", "grpc-health-check", "--port", str(port)])

        ssl_result = subprocess.run(  # pylint:disable=subprocess-run-check
            ["dagster", "api", "grpc-health-check", "--port", str(port), "--use-ssl"]
        )
        assert ssl_result.returncode == 1

    finally:
        process.terminate()
        process.wait()

    out, _err = capfd.readouterr()

    assert f"Started Dagster code server for file {python_file} on port {port} in process" in out
