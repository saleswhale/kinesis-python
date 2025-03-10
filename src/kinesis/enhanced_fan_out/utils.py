from __future__ import absolute_import

import logging
import uuid

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


class EnhancedFanOut:
    """Utility class for Enhanced Fan-Out operations"""

    @staticmethod
    def register_consumer(kinesis_client, stream_arn, consumer_name):
        """Register a consumer for Enhanced Fan-Out

        Args:
            kinesis_client: Boto3 Kinesis client
            stream_arn: ARN of the Kinesis stream
            consumer_name: Name for the consumer

        Returns:
            str: ARN of the registered consumer
        """
        try:
            # Try to find an existing consumer
            response = kinesis_client.describe_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            consumer_arn = response["ConsumerDescription"]["ConsumerARN"]
            log.info("Found existing EFO consumer: %s", consumer_name)
            return consumer_arn
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                # Register a new consumer
                response = kinesis_client.register_stream_consumer(
                    StreamARN=stream_arn, ConsumerName=consumer_name
                )
                consumer_arn = response["Consumer"]["ConsumerARN"]
                log.info("Registered new EFO consumer: %s", consumer_name)
                return consumer_arn
            else:
                log.error("Error registering consumer: %s", e)
                raise

    @staticmethod
    def deregister_consumer(kinesis_client, stream_arn, consumer_name):
        """Deregister a consumer for Enhanced Fan-Out

        Args:
            kinesis_client: Boto3 Kinesis client
            stream_arn: ARN of the Kinesis stream
            consumer_name: Name of the consumer to deregister
        """
        try:
            kinesis_client.deregister_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            log.info("Deregistered EFO consumer: %s", consumer_name)
        except Exception as e:
            log.error("Failed to deregister EFO consumer: %s", e)

    @staticmethod
    def get_starting_position(state, shard_id):
        """Convert standard iterator args to EFO starting position format

        Args:
            state: State object with get_iterator_args method
            shard_id: ID of the shard

        Returns:
            dict: Starting position in EFO format
        """
        if not state:
            return {"Type": "LATEST"}

        iterator_args = state.get_iterator_args(shard_id)

        # Convert from standard format to EFO format
        position_type = iterator_args.get("ShardIteratorType", "LATEST")
        starting_position = {"Type": position_type}

        if position_type in ("AFTER_SEQUENCE_NUMBER", "AT_SEQUENCE_NUMBER"):
            starting_position["SequenceNumber"] = iterator_args[
                "StartingSequenceNumber"
            ]
        elif position_type == "AT_TIMESTAMP":
            starting_position["Timestamp"] = iterator_args["Timestamp"]

        return starting_position

    @staticmethod
    def generate_consumer_name(prefix=None):
        """Generate a unique consumer name

        Args:
            prefix: Optional prefix for the consumer name

        Returns:
            str: Unique consumer name
        """
        if prefix:
            return f"{prefix}-{str(uuid.uuid4())[:8]}"
        return f"python-kinesis-{str(uuid.uuid4())[:8]}"
