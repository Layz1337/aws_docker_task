import logging
import asyncio
from typing import List, Dict

from aiobotocore.session import get_session
from types_aiobotocore_logs.client import CloudWatchLogsClient


logger = logging.getLogger(__name__)


def get_aws_session():
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
        Ensure that the specified CloudWatch log group and stream exist
        It reraises the exception if it's not a ResourceAlreadyExistsException
    """
    logger.info(
        f'Ensuring that the CloudWatch log group {log_group_name} and stream '
        f'{log_stream_name} exist'
    )

    try:
        await cw_client.create_log_group(
            logGroupName=log_group_name
        )

    except cw_client.exceptions.ResourceAlreadyExistsException as e:
        # Group already exists, just continue
        pass
    except cw_client.exceptions as e:
        logger.error(
            'An error occurred while ensuring the CloudWatch log '
            f'group exist: {e}',
            exc_info=True
        )
        raise

    try:
        await cw_client.create_log_stream(
            logGroupName=log_group_name,
            logStreamName=log_stream_name
        )

    except cw_client.exceptions.ResourceAlreadyExistsException as e:
        # Stream already exists
        pass
    except cw_client.exceptions as e:
        logger.error(
            'An error occurred while ensuring the CloudWatch log '
            f'stream exist: {e}',
            exc_info=True
        )
        raise


async def push_log_events_to_cw(
        cw_client: CloudWatchLogsClient,
        log_group_name: str,
        log_stream_name: str,
        log_events: List[Dict[str, str]],
        max_retries: int = 3
) -> None:
    """
        Push the log event to CloudWatch
    """

    for attempt in range(1, max_retries+1):
        try:
            await cw_client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=log_events
            )
            break
        except cw_client.exceptions.ResourceNotFoundException as e:
            logger.error(
                f'CloudWatch log group or stream not found: {e}'
            )
            # re-raise the exception
            raise
        except cw_client.exceptions.ServiceUnavailableException as e:
            if attempt == max_retries:
                logger.error(
                    f'Max retries reached trying to push the log events to '
                    f'CloudWatch: {e}'
                )
                # re-raise the exception
                raise

            logger.warning(
                f'Failed to push the log events to CloudWatch: {e}. '
                f'Retrying {attempt}/{max_retries}'
            )
            # Exponential backoff
            await asyncio.sleep(2 ** attempt)
            continue


async def periodic_log_push(
        cw_client: CloudWatchLogsClient,
        log_group_name: str,
        log_stream_name: str,
        batch_buffer: List[Dict[str, str]],
        max_batch_size: int,
        interval: int,
        max_retries: int = 3
):
    """
        Periodically push the log events to CloudWatch in batches
    """
    try:
        while True:
            # Push all of the logs to the CloudWatch
            while batch_buffer:
                await push_log_events_to_cw(
                    cw_client=cw_client,
                    log_group_name=log_group_name,
                    log_stream_name=log_stream_name,
                    log_events=batch_buffer[:max_batch_size],
                    max_retries=max_retries
                )
                # Remove the pushed logs from the buffer
                del batch_buffer[:max_batch_size]

            await asyncio.sleep(interval)

    except asyncio.exceptions.CancelledError:
        # If the task is cancelled, push the remaining logs
        await push_log_events_to_cw(
            cw_client=cw_client,
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            log_events=batch_buffer,
            max_retries=max_retries
        )
