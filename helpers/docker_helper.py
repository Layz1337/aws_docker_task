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
        Run Docker container with the specified image and bash command
    """
    logger.info(
        f'Running Docker container with image {image_name}'
    )

    try:
        return await docker_client.containers.run(
            config = {
                'Image': image_name,
                'Cmd': command_to_run,
            }
        )
    except aiodocker.exceptions.DockerError as e:
        logger.error(
            f'An error occurred while running Docker container: {e}',
            exc_info=True
        )
        raise
