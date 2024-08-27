import logging

import aiodocker


logger = logging.getLogger(__name__)


def init_docker_client() -> aiodocker.Docker:
    """
        Initialize Docker client
    """
    logger.info('Initializing Docker client')
    return aiodocker.Docker()


async def run_docker_container(
        docker_client: aiodocker.Docker,
        image_name: str,
        command_to_run: str
) -> aiodocker.docker.DockerContainer:
    """
        Run a Docker container with the given image and bash command
    """
    logger.info(
        f'Running a Docker container with the image: {image_name}'
    )

    try:
        return await docker_client.containers.run(
            config={
                'Image': image_name,
                'Cmd': command_to_run,
            }
        )
    except aiodocker.exceptions.DockerError as e:
        logger.error(
            f'An error occurred while running a Docker container: {e}',
            exc_info=True
        )
        raise

async def check_container_status(
        container: aiodocker.docker.DockerContainer
) -> bool:
    """
        Check if the container is still running.
    """
    try:
        container_info = await container.show()
        return container_info['State']['Status'] == 'running'
    except aiodocker.exceptions.DockerError as e:
        logger.error(
            f'Docker error while checking container status: {e}'
        )
        return False
