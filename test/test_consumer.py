try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock

import unittest

import mock
from botocore.exceptions import ClientError
from offspring.process import SubprocessLoop

from kinesis.consumer import KinesisConsumer
from kinesis.enhanced_fan_out import EnhancedFanOut
from kinesis.enhanced_shard_reader import EnhancedShardReader


def test_setup_shards(mocker):
    mock_boto3_session = MagicMock()
    mock_shard_reader = mocker.patch("kinesis.consumer.ShardReader")

    consumer = KinesisConsumer("testing", boto3_session=mock_boto3_session)

    mock_boto3_session.client.assert_called_with("kinesis", endpoint_url=None)

    consumer.kinesis_client.describe_stream.return_value = {
        "StreamDescription": {
            "Shards": [
                {
                    "ShardId": "test-shard",
                }
            ]
        }
    }
    consumer.kinesis_client.get_shard_iterator.return_value = {
        "ShardIterator": "test-iter"
    }

    consumer.setup_shards()

    consumer.kinesis_client.describe_stream.assert_called_with(StreamName="testing")
    consumer.kinesis_client.get_shard_iterator.assert_called_with(
        StreamName="testing", ShardId="test-shard", ShardIteratorType="LATEST"
    )

    mock_shard_reader.assert_called_with(
        "test-shard",
        "test-iter",
        consumer.record_queue,
        consumer.error_queue,
        boto3_session=consumer.boto3_session,
        endpoint_url=None,
        sleep_time=consumer.reader_sleep_time,
    )


class TestEnhancedFanOut(unittest.TestCase):
    """Test the EnhancedFanOut utility class"""

    def setUp(self):
        self.kinesis_client = mock.Mock()
        self.stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream"
        self.consumer_name = "test-consumer"

    def test_register_consumer_new(self):
        """Test registering a new consumer"""
        # Mock the describe call to fail with ResourceNotFoundException
        self.kinesis_client.describe_stream_consumer.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "DescribeStreamConsumer",
        )

        # Mock the register call to succeed
        consumer_arn = f"{self.stream_arn}/consumer/{self.consumer_name}"
        self.kinesis_client.register_stream_consumer.return_value = {
            "Consumer": {
                "ConsumerARN": consumer_arn,
                "ConsumerName": self.consumer_name,
                "ConsumerStatus": "CREATING",
            }
        }

        # Call the utility method
        result = EnhancedFanOut.register_consumer(
            self.kinesis_client, self.stream_arn, self.consumer_name
        )

        # Verify the result
        self.assertEqual(result, consumer_arn)
        self.kinesis_client.describe_stream_consumer.assert_called_once_with(
            StreamARN=self.stream_arn, ConsumerName=self.consumer_name
        )
        self.kinesis_client.register_stream_consumer.assert_called_once_with(
            StreamARN=self.stream_arn, ConsumerName=self.consumer_name
        )

    def test_register_consumer_existing(self):
        """Test finding an existing consumer"""
        # Mock the describe call to succeed
        consumer_arn = f"{self.stream_arn}/consumer/{self.consumer_name}"
        self.kinesis_client.describe_stream_consumer.return_value = {
            "ConsumerDescription": {
                "ConsumerARN": consumer_arn,
                "ConsumerName": self.consumer_name,
                "ConsumerStatus": "ACTIVE",
            }
        }

        # Call the utility method
        result = EnhancedFanOut.register_consumer(
            self.kinesis_client, self.stream_arn, self.consumer_name
        )

        # Verify the result
        self.assertEqual(result, consumer_arn)
        self.kinesis_client.describe_stream_consumer.assert_called_once_with(
            StreamARN=self.stream_arn, ConsumerName=self.consumer_name
        )
        self.kinesis_client.register_stream_consumer.assert_not_called()

    def test_deregister_consumer(self):
        """Test deregistering a consumer"""
        # Call the utility method
        EnhancedFanOut.deregister_consumer(
            self.kinesis_client, self.stream_arn, self.consumer_name
        )

        # Verify the call
        self.kinesis_client.deregister_stream_consumer.assert_called_once_with(
            StreamARN=self.stream_arn, ConsumerName=self.consumer_name
        )

    def test_get_starting_position(self):
        """Test converting iterator args to starting position"""
        # Test with no state
        position = EnhancedFanOut.get_starting_position(None, "shard-1")
        self.assertEqual(position, {"Type": "LATEST"})

        # Test with EFO format already
        state = mock.Mock()
        state.get_iterator_args.return_value = {
            "Type": "AT_SEQUENCE_NUMBER",
            "SequenceNumber": "seq-1234",
        }
        position = EnhancedFanOut.get_starting_position(state, "shard-1")
        self.assertEqual(
            position, {"Type": "AT_SEQUENCE_NUMBER", "SequenceNumber": "seq-1234"}
        )

        # Test conversion from standard format
        state.get_iterator_args.return_value = {
            "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
            "StartingSequenceNumber": "seq-5678",
        }
        position = EnhancedFanOut.get_starting_position(state, "shard-1")
        self.assertEqual(
            position, {"Type": "AFTER_SEQUENCE_NUMBER", "SequenceNumber": "seq-5678"}
        )


class TestEnhancedShardReader(unittest.TestCase):
    """Test the EnhancedShardReader class"""

    def setUp(self):
        # Patch SubprocessLoop to avoid actually starting processes
        self.subprocess_patch = mock.patch.object(SubprocessLoop, "start")
        self.subprocess_mock = self.subprocess_patch.start()

        # Create queues and mocks
        self.record_queue = mock.Mock()
        self.error_queue = mock.Mock()
        self.client = mock.Mock()
        self.boto3_patch = mock.patch("boto3.client", return_value=self.client)
        self.boto3_mock = self.boto3_patch.start()

        # Create a reader
        self.shard_id = "shard-0001"
        self.consumer_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream/consumer/test-consumer"
        self.starting_position = {"Type": "LATEST"}

        self.reader = EnhancedShardReader(
            self.shard_id,
            self.consumer_arn,
            self.record_queue,
            self.error_queue,
            starting_position=self.starting_position,
        )

        # Manually set the client (normally done in begin())
        self.reader.client = self.client

    def tearDown(self):
        self.subprocess_patch.stop()
        self.boto3_patch.stop()

    def test_subscription_success(self):
        """Test processing subscription events successfully"""
        # Mock a subscription response with events
        event_stream = [
            {
                "SubscribeToShardEvent": {
                    "Records": [
                        {"Data": b"data-1", "SequenceNumber": "seq-1"},
                        {"Data": b"data-2", "SequenceNumber": "seq-2"},
                    ],
                    "MillisBehindLatest": 0,
                }
            }
        ]
        self.client.subscribe_to_shard.return_value = {"EventStream": event_stream}

        # Call the loop method
        result = self.reader.loop()

        # Verify behavior
        self.client.subscribe_to_shard.assert_called_once_with(
            ConsumerARN=self.consumer_arn,
            ShardId=self.shard_id,
            StartingPosition=self.starting_position,
        )

        self.record_queue.put.assert_called_once()
        args = self.record_queue.put.call_args[0]
        self.assertEqual(args[0], self.shard_id)
        self.assertEqual(len(args[1]["Records"]), 2)
        self.assertEqual(args[1]["MillisBehindLatest"], 0)

        # Should return 1 to indicate short sleep before renewal
        self.assertEqual(result, 1)
        self.assertIsNone(self.reader.subscription)

    def test_subscription_error(self):
        """Test handling subscription errors"""
        # Mock a ResourceNotFoundException in the event stream
        event_stream = [{"ResourceNotFoundException": {"Message": "Shard not found"}}]
        self.client.subscribe_to_shard.return_value = {"EventStream": event_stream}

        # Call the loop method
        result = self.reader.loop()

        # Verify behavior
        self.assertEqual(result, False)  # Should return False to stop
        self.error_queue.put.assert_called_once_with(self.shard_id)


class TestKinesisConsumer(unittest.TestCase):
    # ... existing test setup ...

    def test_init_with_efo(self):
        """Test initializing KinesisConsumer with EFO enabled"""
        # Mock EnhancedFanOut.register_consumer
        with mock.patch(
            "kinesis.consumer.EnhancedFanOut.register_consumer"
        ) as register_mock:
            register_mock.return_value = "mock-consumer-arn"

            # Create consumer with EFO enabled
            consumer = KinesisConsumer(
                stream_name="test-stream",
                use_enhanced_fan_out=True,
                consumer_name="test-consumer",
            )

            # Verify consumer was registered
            self.assertTrue(consumer.use_enhanced_fan_out)
            self.assertEqual(consumer.consumer_name, "test-consumer")
            self.assertEqual(consumer.consumer_arn, "mock-consumer-arn")

    def test_setup_shards_with_efo(self):
        """Test setting up shards with EFO enabled"""
        consumer = KinesisConsumer("test-stream")
        consumer.use_enhanced_fan_out = True
        consumer.consumer_arn = "mock-consumer-arn"

        # Mock the stream description
        consumer.kinesis_client.describe_stream.return_value = {
            "StreamDescription": {
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
                "Shards": [
                    {
                        "ShardId": "shard-0001",
                        "SequenceNumberRange": {"StartingSequenceNumber": "123"},
                    }
                ],
            }
        }

        # Mock EnhancedFanOut.get_starting_position
        with mock.patch(
            "kinesis.consumer.EnhancedFanOut.get_starting_position"
        ) as position_mock:
            position_mock.return_value = {"Type": "LATEST"}

            # Mock EnhancedShardReader
            with mock.patch("kinesis.consumer.EnhancedShardReader") as reader_mock:
                reader_instance = mock.Mock()
                reader_mock.return_value = reader_instance

                # Call setup_shards
                consumer.setup_shards()

                # Verify EFO reader was created
                reader_mock.assert_called_once()
                args, kwargs = reader_mock.call_args
                self.assertEqual(args[0], "shard-0001")
                self.assertEqual(args[1], "mock-consumer-arn")
                self.assertEqual(kwargs["starting_position"], {"Type": "LATEST"})

                # Verify the reader was added to consumer's shards
                self.assertIn("shard-0001", consumer.shards)
                self.assertEqual(consumer.shards["shard-0001"], reader_instance)

    def test_shutdown_with_efo(self):
        """Test shutdown with EFO cleanup"""
        consumer = KinesisConsumer("test-stream")
        consumer.use_enhanced_fan_out = True
        consumer.consumer_name = "test-consumer"
        consumer.consumer_arn = "mock-consumer-arn"

        # Add a mock shard reader
        mock_reader = mock.Mock()
        consumer.shards = {"shard-0001": mock_reader}

        # Mock EnhancedFanOut.deregister_consumer
        with mock.patch(
            "kinesis.consumer.EnhancedFanOut.deregister_consumer"
        ) as deregister_mock:
            # Mock _get_stream_arn
            consumer._get_stream_arn = mock.Mock(return_value="mock-stream-arn")

            # Call shutdown
            consumer.shutdown()

            # Verify reader was shut down
            mock_reader.shutdown.assert_called_once()

            # Verify consumer was deregistered
            deregister_mock.assert_called_once_with(
                consumer.kinesis_client, "mock-stream-arn", "test-consumer"
            )

            # Verify state was cleaned up
            self.assertEqual(consumer.shards, {})
            self.assertIsNone(consumer.stream_data)
            self.assertFalse(consumer.run)


# ... rest of test cases ...
