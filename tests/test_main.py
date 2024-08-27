import logging
import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from helpers import (
    ensure_cw_log_group_and_stream,
    periodic_log_push,
    run_docker_container,
    process_log_messages,
    utf8_boundary_iterator
)


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_ensure_cw_log_group_and_stream():
    mock_client = AsyncMock()
    mock_client.create_log_group = AsyncMock()
    mock_client.create_log_stream = AsyncMock()

    await ensure_cw_log_group_and_stream(
        mock_client,
        'test_group',
        'test_stream'
    )

    mock_client.create_log_group.assert_called_once_with(
        logGroupName='test_group'
    )
    mock_client.create_log_stream.assert_called_once_with(
        logGroupName='test_group',
        logStreamName='test_stream'
    )


@pytest.mark.asyncio
async def test_run_docker_container():
    mock_docker_client = AsyncMock()
    mock_container = AsyncMock()
    mock_docker_client.containers.run.return_value = mock_container

    container = await run_docker_container(
        mock_docker_client,
        'test_image',
        ['echo', 'test']
    )

    mock_docker_client.containers.run.assert_called_once_with(
        config={
            'Image': 'test_image',
            'Cmd': ['echo', 'test'],
        }
    )
    assert container == mock_container


@pytest.mark.asyncio
async def test_process_log_messages():
    message_buffer = [b'test message 1', b'test message 2']
    batch_buffer = []

    sleep_count = 0

    async def mock_sleep(*args):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:
            raise asyncio.CancelledError()
        await asyncio.sleep(0.01)

    with patch('asyncio.sleep', mock_sleep):
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

    assert sleep_count >= 2


@pytest.mark.asyncio
async def test_process_log_messages_empty_buffer():
    message_buffer = []
    batch_buffer = []

    sleep_count = 0

    async def mock_sleep(*args):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:
            raise asyncio.CancelledError()
        await asyncio.sleep(0.01)

    with patch('asyncio.sleep', mock_sleep):
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
async def test_periodic_log_push():
    mock_cw_client = AsyncMock()
    batch_buffer = [
        {'timestamp': 1234567890, 'message': 'test message 1'},
        {'timestamp': 1234567891, 'message': 'test message 2'}
    ]
    original_buffer = batch_buffer.copy()

    sleep_count = 0

    async def mock_sleep(*args):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:
            raise asyncio.CancelledError()
        await asyncio.sleep(0.01)

    with patch('asyncio.sleep', mock_sleep):
        try:
            await periodic_log_push(
                mock_cw_client,
                'test_group',
                'test_stream',
                batch_buffer,
                2,
                0.1
            )
        except asyncio.CancelledError:
            pass

    mock_cw_client.put_log_events.assert_called_once_with(
        logGroupName='test_group',
        logStreamName='test_stream',
        logEvents=original_buffer
    )

    assert sleep_count >= 2
    assert len(batch_buffer) == 0
    assert (
        mock_cw_client.put_log_events.call_args[1]['logEvents'] ==
        original_buffer
    )


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
