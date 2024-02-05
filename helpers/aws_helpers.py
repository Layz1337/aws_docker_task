import logging
import asyncio
from typing import List, Dict

from aiobotocore.session import get_session
from types_aiobotocore_logs.client import (
    CloudWatchLogsClient,
    BotocoreClientError
)


logger = logging.getLogger(__name__)


def init_aws_session():
    """
        Initialize AWS session
    """
    logger.info('Initializing AWS session')
    session = get_session()
    return session


async def ensure_cw_log_group_and_stream(
        cw_client: CloudWatchLogsClient,
        log_group_name: str,
        log_stream_name: str
) -> None:
    """
        Ensure that the specified CloudWatch log group and stream exists
        Retrow the exception if it's not a ResourceAlreadyExistsException
    """
    logger.info(
        f'Ensuring that the CloudWatch log group {log_group_name} and stream '
        f'{log_stream_name} exists'
    )

    try:
        await cw_client.create_log_group(
            logGroupName=log_group_name
        )

    except cw_client.exceptions.ResourceAlreadyExistsException:
        # Group already exists, just continue
        pass
    except BotocoreClientError as e:
        logger.error(
            'An error occurred durning the CloudWatch log '
            f'group ensuring: {e}',
            exc_info=True
        )
        raise

    try:
        await cw_client.create_log_stream(
            logGroupName=log_group_name,
            logStreamName=log_stream_name
        )

    except cw_client.exceptions.ResourceAlreadyExistsException:
        # Stream already exists
        pass
    except BotocoreClientError as e:
        logger.error(
            'An error occurred durning the CloudWatch log '
            f'stream ensuring: {e}',
            exc_info=True
        )
        raise


async def periodic_log_push(
        cw_client: CloudWatchLogsClient,
        log_group_name: str,
        log_stream_name: str,
        batch_buffer: List[Dict[str, str]],
        max_batch_size: int,
        interval: int
):
    """
        Periodically push the log events batches to CloudWatch
    """
    try:
        while True:
            while batch_buffer:
                await cw_client.put_log_events(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name,
                    logEvents=batch_buffer[:max_batch_size]
                )

                # Remove the pushed logs from the buffer
                del batch_buffer[:max_batch_size]

            await asyncio.sleep(interval)

    except asyncio.exceptions.CancelledError:
        if not batch_buffer:
            return

        await cw_client.put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=batch_buffer
        )
