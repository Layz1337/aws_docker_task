import asyncio
import pytest


from helpers import (
    process_log_messages,
    utf8_boundary_iterator
)


@pytest.mark.asyncio
async def test_process_log_messages(mocker):
    message_buffer = [b'test message 1', b'test message 2']
    batch_buffer = []

    sleep_count = 0

    async def mock_sleep(*args):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:
            raise asyncio.CancelledError()
        await asyncio.sleep(0.01)

    mocker.patch(
        'asyncio.sleep',
        mock_sleep
    )

    try:
        await process_log_messages(
            message_buffer,
            batch_buffer,
            1024,
            0.1
        )
    except asyncio.CancelledError:
        pass

    assert len(batch_buffer) == 1
    assert batch_buffer[0]['message'] == 'test message 1test message 2'

    assert len(message_buffer) == 0

    assert sleep_count == 2


@pytest.mark.asyncio
async def test_process_log_messages_empty_buffer(mocker):
    message_buffer = []
    batch_buffer = []

    sleep_count = 0

    async def mock_sleep(*args):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:
            raise asyncio.CancelledError()
        await asyncio.sleep(0.01)

    mocker.patch(
        'asyncio.sleep',
        mock_sleep
    )
    try:
        await process_log_messages(
            message_buffer,
            batch_buffer,
            1024,
            0.1
        )
    except asyncio.CancelledError:
        pass

    assert len(batch_buffer) == 0

    assert sleep_count >= 2


@pytest.mark.asyncio
async def test_utf8_boundary_iterator():
    test_string = b'Hello\nWorld'
    chunks = [
        chunk async for chunk in utf8_boundary_iterator(test_string, 6)
    ]
    assert chunks == [b'Hello', b'\nWorld']

    test_string = 'Hello 世界'.encode('utf-8')
    chunks = [
        chunk async for chunk in utf8_boundary_iterator(test_string, 7)
    ]
    assert chunks == [b'Hello ', '世界'.encode('utf-8')]
