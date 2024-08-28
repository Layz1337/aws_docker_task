import pytest
import aiodocker

from helpers import run_docker_container, check_container_status


DOCKER_ERROR = aiodocker.exceptions.DockerError(
    '',
    {'message': ''},
    ''
)


@pytest.mark.asyncio
async def test_run_docker_container_success(
        docker_client
    ):
    docker_client.containers.run.return_value = 'some_container'

    result = await run_docker_container(
        docker_client,
        'test_image',
        'echo test'
    )

    assert docker_client.containers.run.call_count == 1
    assert result == 'some_container'


@pytest.mark.asyncio
async def test_run_docker_container_failed(docker_client):
    docker_client.containers.run.side_effect = DOCKER_ERROR

    with pytest.raises(aiodocker.exceptions.DockerError):
        await run_docker_container(
            docker_client,
            'test_image',
            'echo test'
        )


@pytest.mark.parametrize(
    'container_status, expected_result',
    (
            ('running', True),
            ('stopped', False)
    ),
    ids=['container_running', 'container_not_running']
)
@pytest.mark.asyncio
async def test_check_container_status_success(
        docker_container,
        container_status,
        expected_result
):
    docker_container.show.return_value = {
        'State': {
            'Status': container_status
        }
    }

    result = await check_container_status(docker_container)

    assert result is expected_result


@pytest.mark.asyncio
async def test_check_container_status_with_exception(docker_container):
    docker_container.show.side_effect = DOCKER_ERROR

    result = await check_container_status(docker_container)

    assert result is False
