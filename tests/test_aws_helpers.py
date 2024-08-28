import pytest
import asyncio

from helpers import ensure_cw_log_group_and_stream, periodic_log_push


@pytest.mark.asyncio
async def test_ensure_cw_log_group_and_stream_without_errors(cw_client):
    await ensure_cw_log_group_and_stream(
        cw_client,
        'test_group',
        'test_stream'
    )

    cw_client.create_log_group.assert_called_once_with(
        logGroupName='test_group'
    )
    cw_client.create_log_stream.assert_called_once_with(
        logGroupName='test_group',
        logStreamName='test_stream'
    )


@pytest.mark.asyncio
async def test_periodic_log_push(mocker, cw_client):
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

    mocker.patch(
        'asyncio.sleep',
        mock_sleep
    )

    try:
        await periodic_log_push(
            cw_client,
            'test_group',
            'test_stream',
            batch_buffer,
            2,
            0.1
        )
    except asyncio.CancelledError:
        pass

    cw_client.put_log_events.assert_called_once_with(
        logGroupName='test_group',
        logStreamName='test_stream',
        logEvents=original_buffer
    )

    assert sleep_count >= 2
    assert len(batch_buffer) == 0
    assert (
        cw_client.put_log_events.call_args[1]['logEvents'] ==
        original_buffer
    )
