import pytest
import asyncio


@pytest.fixture
def cw_client(mocker):
    cw_client = mocker.patch(
        'types_aiobotocore_logs.client.CloudWatchLogsClient', autospec=True
    )

    return cw_client


@pytest.fixture
def docker_client(mocker):
    docker_client = mocker.patch(
        'aiodocker.Docker'
    )
    docker_client.containers = mocker.patch(
        'aiodocker.containers.DockerContainers', autospec=True
    )

    return docker_client


@pytest.fixture()
def docker_container(mocker):
    docker_container = mocker.patch(
        'aiodocker.docker.DockerContainer', autospec=True
    )
    return docker_container
