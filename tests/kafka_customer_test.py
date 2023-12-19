import pytest
from unittest.mock import AsyncMock, MagicMock, call
from aiokafka import TopicPartition, AIOKafkaConsumer
from src.modules.kafka import receive_messages


@pytest.mark.asyncio
async def test_receive_messages_with_rebalance():
    # Create mock objects

    consumer = MagicMock(spec=AIOKafkaConsumer)
    shutdown_event = MagicMock()
    shutdown_event.is_set = MagicMock(side_effect=[False, True])  # Add this line
    consumer.getmany = AsyncMock(
        side_effect=[
            {
                TopicPartition("test_topic", 0): [
                    MagicMock(value=b"message1"),
                    MagicMock(value=b"message2"),
                ]
            },
            {
                TopicPartition("test_topic", 1): [
                    MagicMock(value=b"message3"),
                    MagicMock(value=b"message4"),
                ]
            },
            Exception("Rebalance occurred"),  # Simulate a rebalance
        ]
    )
    consumer.commit = AsyncMock()
    receive_queue = MagicMock()
    receive_queue.put = AsyncMock()

    # Call the function with the mock objects
    with pytest.raises(Exception, match="Rebalance occurred"):
        await receive_messages(consumer, receive_queue, shutdown_event)

    # Assert that the consumer was used correctly
    consumer.getmany.assert_has_calls(
        [
            call(timeout_ms=1000, max_records=200),
            call(timeout_ms=1000, max_records=200),
        ]
    )
    consumer.commit.assert_has_calls(
        [
            call(),
            call(),
        ]
    )

    # Assert that the messages were put in the queue
    receive_queue.put.assert_has_calls(
        [
            call(b"message1"),
            call(b"message2"),
            call(b"message3"),
            call(b"message4"),
        ]
    )


@pytest.mark.asyncio
async def test_receive_messages():
    # Create mock objects
    consumer = MagicMock()
    shutdown_event = MagicMock()
    shutdown_event.is_set = MagicMock(side_effect=[False, True])  # Add this line
    consumer.getmany = AsyncMock(
        return_value={
            TopicPartition("test_topic", 0): [
                MagicMock(value=b"message1"),
                MagicMock(value=b"message2"),
            ]
        }
    )
    consumer.commit = AsyncMock()
    receive_queue = MagicMock()
    receive_queue.put = AsyncMock()

    # Call the function with the mock objects
    await receive_messages(consumer, receive_queue, shutdown_event)

    # Assert that the consumer was used correctly
    consumer.getmany.assert_called_once_with(timeout_ms=1000, max_records=200)
    consumer.commit.assert_called_once()

    # Assert that the messages were put in the queue
    receive_queue.put.assert_any_call(b"message1")
    receive_queue.put.assert_any_call(b"message2")
