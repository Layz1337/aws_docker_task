#!/usr/bin/env python3

import logging
import argparse
import asyncio

from types_aiobotocore_logs.client import CloudWatchLogsClient
from aiodocker.docker import DockerContainer

from constants import (
    MAX_CONNECTION_RETRIES,
    MAX_MESSAGE_SIZE_BYTES,
    MAX_BATCH_SIZE,
    SEND_LOGS_INTERVAL
)
from helpers import (
    ensure_cw_log_group_and_stream,
    periodic_log_push,
    get_aws_session,
    init_docker_client,
    run_docker_container,
    fill_batch_buffer
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
        Parse command line arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--docker-image',
        type=str,
        help='Docker image name',
        required=True
    )
    parser.add_argument(
        '--bash-command',
        type=str,
        help='Bash command to run in the Docker container',
        required=True
    )
    parser.add_argument(
        '--aws-cloudwatch-group',
        type=str,
        help='AWS CloudWatch group',
        required=True
    )
    parser.add_argument(
        '--aws-cloudwatch-stream',
        type=str,
        help='AWS CloudWatch stream',
        required=True
    )
    parser.add_argument(
        '--aws-access-key-id',
        type=str,
        help='AWS access key ID',
        required=True
    )
    parser.add_argument(
        '--aws-secret-access-key',
        type=str,
        help='AWS secret access key',
        required=True
    )
    parser.add_argument(
        '--aws-region',
        type=str,
        help='AWS region',
        required=True
    )

    args = parser.parse_args()
    return args


async def stream_and_push_logs(
        container: DockerContainer,
        cw_client: CloudWatchLogsClient,
        log_group_name: str,
        log_stream_name: str
) -> None:
    """
        Stream container logs and push them to CloudWatch in batches
    """
    message_buffer = b''
    batch_buffer = []

    push_task = None
    try:
        # Create a task for the periodic log push
        push_task = asyncio.create_task(
            periodic_log_push(
                batch_buffer=batch_buffer,
                cw_client=cw_client,
                log_group_name=log_group_name,
                log_stream_name=log_stream_name,
                max_batch_size=MAX_BATCH_SIZE,
                interval=SEND_LOGS_INTERVAL
            )
        )

        async for line in container.log(
                follow=True,
                stdout=True,
                stderr=True
        ):
            logger.info(line)
            message_buffer += line.encode('utf-8')

            # Continue if the message buffer is not full
            if len(message_buffer) < MAX_MESSAGE_SIZE_BYTES * MAX_BATCH_SIZE:
                continue

            # Fill the batch buffer with the log events
            await fill_batch_buffer(
                batch_buffer=batch_buffer,
                message_buffer=message_buffer,
                message_size_bytes=MAX_MESSAGE_SIZE_BYTES
            )

            # Clear the message buffer
            message_buffer = b''

    except asyncio.exceptions.CancelledError:
        logger.info('The log push task was cancelled')

    finally:
        # Process the remaining log messages
        if message_buffer:
            await fill_batch_buffer(
                batch_buffer=batch_buffer,
                message_buffer=message_buffer,
                message_size_bytes=MAX_MESSAGE_SIZE_BYTES
            )

        # Push the remaining logs if the task exists
        if push_task:
            push_task.cancel()
            await push_task

async def main():
    container = None
    docker_client = None

    try:
        # Parse command line arguments
        args = parse_arguments()

        # Initialize Docker client
        docker_client = init_docker_client()

        # Initialize AWS CloudWatch session
        cw_session = get_aws_session()

        logger.info('Initializing AWS CloudWatch client')
        async with cw_session.create_client(
            service_name='logs',
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region_name=args.aws_region
        ) as cw_client:
            # Ensure the specified CloudWatch log group and stream
            await ensure_cw_log_group_and_stream(
                cw_client,
                args.aws_cloudwatch_group,
                args.aws_cloudwatch_stream
            )

            # Run the Docker container
            container = await run_docker_container(
                docker_client,
                args.docker_image,
                ['/bin/bash', '-c', args.bash_command]  # wrap command using bash -c
            )

            # Stream and push logs to CloudWatch
            await stream_and_push_logs(
                container,
                cw_client,
                args.aws_cloudwatch_group,
                args.aws_cloudwatch_stream
            )

    except Exception as e:
        logger.error(
            f'An error occurred: {e}',
            exc_info=True
        )

    except KeyboardInterrupt:
        logger.info(
            'The process was interrupted by the user'
        )

    finally:
        logging.info('Cleaning up')

        if container is not None:
            await container.stop(
                # Wait for 10 seconds for the container to stop
                t=10
            )
            await container.delete(
                force=True
            )

        # Close the Docker client
        if docker_client is not None:
            await docker_client.close()


if __name__ == "__main__":
    asyncio.run(main())
