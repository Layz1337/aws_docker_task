import logging
import asyncio
from datetime import datetime
from typing import List, Dict


logger = logging.getLogger(__name__)


async def utf8_boundary_iterator(
        string: bytes,
        message_size_bytes: int
):
    """
        Safely split the input string into chunks of the specified size.
        Attempt to split on newline characters
            if they are present within the message size limit.
    """
    start = 0

    while start < len(string):
        end = start + message_size_bytes
        if end >= len(string):
            # If the end of the range is out of the string bounds,
            # yield the rest and break
            yield string[start:]
            break

        # Try to find a newline character as a natural breaking point
        newline_pos = string.rfind(b'\n', start, end)
        if newline_pos != -1:
            yield string[start:newline_pos]
            start = newline_pos
            continue

        # If no suitable newline is found, check if we are
        # in the middle of a multi-byte character
        while (string[end] & 0xC0) == 0x80:
            end -= 1

        if end == start:
            # Prevent infinite loop if for some reason
            # entire message is broken utf-8 string
            # Return it as is and log a warning
            end = start + message_size_bytes
            logger.warning(
                f'Failed to split the message: {string[start:end]}'
            )

        yield string[start:end]
        start = end


async def fill_batch_buffer(
        batch_buffer: List[Dict[str, str]],
        message_buffer: bytes,
        message_size_bytes: int
):
    """
        Split the message string into chunks and fill the batch buffer
    """
    async for chunk in utf8_boundary_iterator(
            string=message_buffer,
            message_size_bytes=message_size_bytes
    ):
        message = chunk.decode('utf-8')

        if not message:
            continue

        log_event = {
            'timestamp': int(datetime.now().timestamp() * 1000),
            'message': message
        }
        batch_buffer.append(log_event)


async def process_log_messages(
        message_buffer: List[bytes],
        batch_buffer: List[Dict[str, str]],
        message_size_bytes: int,
        interval: int
):
    """
        Process the log messages in batches
    """
    try:
        while True:
            while message_buffer:

                await fill_batch_buffer(
                    batch_buffer=batch_buffer,
                    message_buffer=b''.join(message_buffer),
                    message_size_bytes=message_size_bytes
                )

                message_buffer.clear()

            await asyncio.sleep(interval)

    except asyncio.exceptions.CancelledError:
        if not message_buffer:
            return

        await fill_batch_buffer(
            batch_buffer=batch_buffer,
            message_buffer=b''.join(message_buffer),
            message_size_bytes=message_size_bytes
        )
