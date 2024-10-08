#!/usr/bin/env python3

import logging
import argparse
import asyncio
from typing import List

from aiobotocore.config import AioConfig
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
    init_aws_session,
    init_docker_client,
    run_docker_container,
    check_container_status,
    process_log_messages
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
    parser.add_argument(
        "--log-level",
        type=str,
        default="info",
        choices=[
            "debug",
            "info",
            "warning",
            "error",
            "critical"
        ],
        help="Set the logging level"
    )

    args = parser.parse_args()
    return args


def setup_logging_level(log_level: str) -> None:
    """
        Set the logging level
    """
    logging.basicConfig(
        level=log_level.upper(),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


async def log_stream(
        container: DockerContainer,
        message_buffer: List[bytes]
) -> None:
    """
        Stream container logs
        It will stop when the container stops
    """
    async for line in container.log(follow=True, stdout=True, stderr=True):
        logger.debug(line)
        message_buffer.append(line.encode('utf-8'))

        if not await check_container_status(container):
            logger.info("Container has stopped. Finishing log processing.")
            break


async def stream_and_push_logs(
        container: DockerContainer,
        cw_client: CloudWatchLogsClient,
        log_group_name: str,
        log_stream_name: str
) -> None:
    """
        Stream container logs and push them to CloudWatch in batches
    """
    message_buffer = []
    batch_buffer = []

    push_task = None
    log_process_task = None
    try:
        # Stream the logs from the container
        log_stream_task = asyncio.create_task(
            log_stream(
                container, message_buffer
            )
        )

        # Process the log messages in batches
        log_process_task = asyncio.create_task(
            process_log_messages(
                message_buffer=message_buffer,
                batch_buffer=batch_buffer,
                message_size_bytes=MAX_MESSAGE_SIZE_BYTES,
                interval=SEND_LOGS_INTERVAL
            )
        )

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

        # Wait for the log stream task to finish
        await log_stream_task

    except asyncio.exceptions.CancelledError:
        logger.info('The log push task was cancelled')

    finally:
        # Cancel the log stream task
        if log_stream_task and not log_stream_task.done():
            log_stream_task.cancel()
            await log_stream_task

        # Process the remaining log messages
        if log_process_task and not log_process_task.done():
            log_process_task.cancel()
            await log_process_task

        # Push the remaining logs if the task exists
        if push_task and not push_task.done():
            push_task.cancel()
            await push_task


async def main():
    container = None
    docker_client = None

    try:
        args = parse_arguments()

        setup_logging_level(args.log_level)

        docker_client = init_docker_client()

        cw_session = init_aws_session()

        logger.info('Initializing AWS CloudWatch client')

        aws_config = AioConfig(
            retries={
                'max_attempts': MAX_CONNECTION_RETRIES,
                'mode': 'standard'
            }
        )

        async with cw_session.create_client(
            service_name='logs',
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region_name=args.aws_region,
            config=aws_config
        ) as cw_client:
            await ensure_cw_log_group_and_stream(
                cw_client,
                args.aws_cloudwatch_group,
                args.aws_cloudwatch_stream
            )

            container = await run_docker_container(
                docker_client,
                args.docker_image,
                # wrap command using bash -c
                ['/bin/bash', '-c', args.bash_command]
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

    except asyncio.exceptions.CancelledError:
        logger.info(
            'The process was cancelled'
        )

    finally:
        logger.info('Cleaning up')

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
