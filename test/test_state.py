import unittest
from datetime import datetime, timedelta
from kinesis.consumer import DynamoDB

STREAM_RETENTION_PERIOD_IN_HOURS = 24


class TestDynamoDB(unittest.TestCase):
    def test_get_iterator_args_efo(self):
        """Test getting iterator args in EFO format"""
        # Create DynamoDB with EFO enabled
        dynamo = DynamoDB(
            "test-table", "test-consumer", "test-stream", use_enhanced_fan_out=True
        )

        # Test default with no checkpoint
        args = dynamo.get_iterator_args("shard-0001")
        self.assertEqual(args, {"Type": "LATEST"})

        # Add a checkpoint and test
        dynamo.shards = {
            "shard-0001": {
                "checkpoint": "seq-1234",
                "heartbeat": datetime.utcnow().isoformat() + "Z",
            }
        }

        args = dynamo.get_iterator_args("shard-0001")
        self.assertEqual(
            args, {"Type": "AFTER_SEQUENCE_NUMBER", "SequenceNumber": "seq-1234"}
        )

    def test_get_iterator_args_stale_heartbeat_efo(self):
        """Test stale heartbeat handling with EFO enabled"""
        dynamo = DynamoDB(
            "test-table", "test-consumer", "test-stream", use_enhanced_fan_out=True
        )

        # Add a stale checkpoint
        old_time = (
            datetime.utcnow() - timedelta(hours=STREAM_RETENTION_PERIOD_IN_HOURS + 1)
        ).isoformat() + "Z"
        dynamo.shards = {
            "shard-0001": {"checkpoint": "seq-1234", "heartbeat": old_time}
        }

        args = dynamo.get_iterator_args("shard-0001")
        self.assertEqual(args, {"Type": "LATEST"})
