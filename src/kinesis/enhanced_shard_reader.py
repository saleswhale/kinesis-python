from __future__ import absolute_import

import logging

import boto3
from botocore.exceptions import ClientError
from offspring.process import SubprocessLoop

from .exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


class EnhancedShardReader(SubprocessLoop):
    """Read from a specific shard using Enhanced Fan-Out (HTTP/2 streaming)"""

    DEFAULT_SLEEP_TIME = 1.0

    def __init__(
        self,
        shard_id,
        consumer_arn,
        record_queue,
        error_queue,
        starting_position=None,
        boto3_session=None,
        endpoint_url=None,
    ):
        self.shard_id = shard_id
        self.consumer_arn = consumer_arn
        self.record_queue = record_queue
        self.error_queue = error_queue
        self.starting_position = starting_position or {"Type": "LATEST"}
        self.boto3_session = boto3_session or boto3.Session()
        self.endpoint_url = endpoint_url
        self.sleep_time = self.DEFAULT_SLEEP_TIME
        self.subscription = None

        self.start()

    def begin(self):
        """Begin the EFO shard reader main loop"""
        log.info("Enhanced Fan-Out reader for %s starting", self.shard_id)
        self.client = self.boto3_session.client(
            "kinesis", endpoint_url=self.endpoint_url
        )
        self.retries = 0

    def loop(self):
        """Each loop iteration for EFO reader"""
        try:
            if not self.subscription:
                # Subscribe to the shard
                self.subscription = self.client.subscribe_to_shard(
                    ConsumerARN=self.consumer_arn,
                    ShardId=self.shard_id,
                    StartingPosition=self.starting_position,
                )

            # Process events from the subscription
            records = []
            millis_behind_latest = 0

            # Process all events in the event stream
            for event in self.subscription["EventStream"]:
                if "SubscribeToShardEvent" in event:
                    event_data = event["SubscribeToShardEvent"]
                    if "Records" in event_data:
                        records.extend(event_data["Records"])
                    if "MillisBehindLatest" in event_data:
                        millis_behind_latest = event_data["MillisBehindLatest"]
                elif "ResourceNotFoundException" in event:
                    log.error(
                        "Shard %s not found: %s",
                        self.shard_id,
                        event["ResourceNotFoundException"],
                    )
                    self.error_queue.put(self.shard_id)
                    return False
                elif "ResourceInUseException" in event:
                    log.error(
                        "Shard %s in use: %s",
                        self.shard_id,
                        event["ResourceInUseException"],
                    )
                    self.error_queue.put(self.shard_id)
                    return False
                elif any(k for k in event.keys() if k.endswith("Exception")):
                    # Any other exception
                    exception_key = next(
                        k for k in event.keys() if k.endswith("Exception")
                    )
                    log.error(
                        "Error in EFO subscription for shard %s: %s",
                        self.shard_id,
                        event[exception_key],
                    )
                    self.error_queue.put(self.shard_id)
                    return False

            # Put all records on the queue
            if records:
                log.info(
                    "Received %d records from shard %s", len(records), self.shard_id
                )
                self.record_queue.put(
                    (
                        self.shard_id,
                        {
                            "Records": records,
                            "MillisBehindLatest": millis_behind_latest,
                            "NextShardIterator": "EFO-subscription-active",
                        },
                    )
                )

            # The subscription naturally ends after ~5 minutes
            # Set to None so we'll create a new one on the next loop
            self.subscription = None
            return 1  # Short sleep before renewing subscription

        except ClientError as exc:
            if exc.response["Error"]["Code"] in RETRY_EXCEPTIONS:
                # Handle retries
                self.retries += 1
                log.info(
                    "Retrying EFO subscription for shard %s (attempt %d): %s",
                    self.shard_id,
                    self.retries,
                    exc,
                )
                self.subscription = None
                return min(30, self.retries * 2)  # Back off with increasing delay
            else:
                log.error(
                    "Client error for EFO subscription to shard %s: %s",
                    self.shard_id,
                    exc,
                )
                self.error_queue.put(self.shard_id)
                return False
        except Exception as e:
            log.error(
                "Unexpected error in EFO subscription to shard %s: %s", self.shard_id, e
            )
            self.error_queue.put(self.shard_id)
            return False

    def end(self):
        """End of the main loop"""
        log.info("Enhanced Fan-Out reader for %s stopping", self.shard_id)
        self.subscription = None
