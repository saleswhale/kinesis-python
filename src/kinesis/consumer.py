from __future__ import absolute_import

import logging
import multiprocessing
import time

import boto3
import six.moves.queue
from botocore.exceptions import ClientError
from offspring.process import SubprocessLoop

from .exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


class ShardReader(SubprocessLoop):
    """Read from a specific shard, passing records and errors back through queues"""

    # how long we sleep between calls to get_records
    # this follow these best practices: http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
    # this can be influeced per-reader instance via the sleep_time arg
    DEFAULT_SLEEP_TIME = 1.0

    def __init__(
        self,
        shard_id,
        shard_iter,
        record_queue,
        error_queue,
        boto3_session=None,
        sleep_time=None,
        endpoint_url=None,
    ):
        self.shard_id = shard_id
        self.shard_iter = shard_iter
        self.record_queue = record_queue
        self.error_queue = error_queue
        self.boto3_session = boto3_session or boto3.Session()
        self.endpoint_url = endpoint_url
        self.sleep_time = sleep_time or self.DEFAULT_SLEEP_TIME
        self.start()

    def begin(self):
        """Begin the shard reader main loop"""
        log.info("Shard reader for %s starting", self.shard_id)
        self.client = self.boto3_session.client(
            "kinesis", endpoint_url=self.endpoint_url
        )
        self.retries = 0

    def loop(self):
        """Each loop iteration - returns a sleep time or False to stop the loop"""
        # by default we will sleep for our sleep_time each loop
        loop_status = self.sleep_time

        try:
            resp = self.client.get_records(ShardIterator=self.shard_iter)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in RETRY_EXCEPTIONS:
                # sleep for 1 second the first loop, 1 second the next, then 2, 4, 6, 8, ..., up to a max of 30 or
                # until we complete a successful get_records call
                loop_status = min((30, (self.retries or 1) * 2))
                log.debug(
                    "Retrying get_records (#%d %ds): %s",
                    self.retries + 1,
                    loop_status,
                    exc,
                )
            else:
                log.error("Client error occurred while reading: %s", exc)
                loop_status = False
        else:
            if not resp["NextShardIterator"]:
                # the shard has been closed
                log.info("Our shard has been closed, exiting")
                return False

            self.shard_iter = resp["NextShardIterator"]
            self.record_queue.put((self.shard_id, resp))
            self.retries = 0

        return loop_status

    def end(self):
        """End of the main loop"""
        log.info("Shard reader for %s stopping", self.shard_id)


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

            # Process the subscription event stream
            for event in self.subscription["EventStream"]:
                if "SubscribeToShardEvent" in event:
                    # Got records from the subscription
                    records_event = event["SubscribeToShardEvent"]
                    records = records_event.get("Records", [])

                    if records:
                        # Format response similar to get_records to maintain compatibility
                        resp = {
                            "Records": records,
                            "MillisBehindLatest": records_event.get(
                                "MillisBehindLatest", 0
                            ),
                        }
                        self.record_queue.put((self.shard_id, resp))

                elif (
                    "ResourceNotFoundException" in event
                    or "ResourceInUseException" in event
                ):
                    # Handle errors
                    log.error("Error in EFO stream: %s", event)
                    self.error_queue.put(self.shard_id)
                    return False

            # When we get here, the subscription has ended (they typically last ~5 minutes)
            self.subscription = None
            return 1  # Short sleep before renewing subscription

        except ClientError as exc:
            if exc.response["Error"]["Code"] in RETRY_EXCEPTIONS:
                loop_status = min((30, (self.retries or 1) * 2))
                log.debug(
                    "Retrying EFO subscription (#%d %ds): %s",
                    self.retries + 1,
                    loop_status,
                    exc,
                )
                self.retries += 1
                return loop_status
            else:
                log.error(
                    "AWS service error in EFO for shard %s: %s", self.shard_id, exc
                )
                self.error_queue.put(self.shard_id)
                return False
        except Exception as e:
            log.error(
                "Error in Enhanced Fan-Out reader for shard %s: %s", self.shard_id, e
            )
            self.subscription = None
            self.error_queue.put(self.shard_id)
            return False

    def end(self):
        """End of the main loop"""
        log.info("Enhanced Fan-Out reader for %s stopping", self.shard_id)
        self.subscription = None


class EnhancedFanOut:
    """Utility class for Enhanced Fan-Out functionality"""

    @staticmethod
    def register_consumer(kinesis_client, stream_arn, consumer_name):
        """Register this application as an EFO consumer"""
        log.info("Checking if consumer '%s' already exists", consumer_name)

        try:
            # First try to describe the consumer directly
            response = kinesis_client.describe_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            consumer_arn = response["ConsumerDescription"]["ConsumerARN"]
            log.info(
                "Consumer '%s' already exists with ARN: %s", consumer_name, consumer_arn
            )
            return consumer_arn
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                # Register a new consumer
                log.info("Registering new consumer '%s'", consumer_name)
                response = kinesis_client.register_stream_consumer(
                    StreamARN=stream_arn, ConsumerName=consumer_name
                )
                consumer_arn = response["Consumer"]["ConsumerARN"]
                log.info("Successfully registered consumer with ARN: %s", consumer_arn)
                return consumer_arn
            raise

    @staticmethod
    def deregister_consumer(kinesis_client, stream_arn, consumer_name):
        """Deregister an EFO consumer"""
        try:
            kinesis_client.deregister_stream_consumer(
                StreamARN=stream_arn, ConsumerName=consumer_name
            )
            log.info("Deregistered EFO consumer: %s", consumer_name)
            return True
        except Exception as e:
            log.error("Failed to deregister EFO consumer: %s", e)
            return False

    @staticmethod
    def get_starting_position(state, shard_id):
        """Convert iterator args to EFO starting position format"""
        if not state:
            return {"Type": "LATEST"}

        iterator_args = state.get_iterator_args(shard_id)

        # Already in EFO format
        if "Type" in iterator_args:
            return iterator_args

        # Convert standard format to EFO format
        position_type = iterator_args.get("ShardIteratorType", "LATEST")
        starting_position = {"Type": position_type}

        if position_type in ("AFTER_SEQUENCE_NUMBER", "AT_SEQUENCE_NUMBER"):
            starting_position["SequenceNumber"] = iterator_args[
                "StartingSequenceNumber"
            ]
        elif position_type == "AT_TIMESTAMP":
            starting_position["Timestamp"] = iterator_args["Timestamp"]

        return starting_position


class KinesisConsumer(object):
    """Consume from a kinesis stream

    A process is started for each shard we are to consume from.  Each process passes messages back up to the parent,
    which are returned via the main iterator.
    """

    LOCK_DURATION = 30

    def __init__(
        self,
        stream_name,
        boto3_session=None,
        state=None,
        reader_sleep_time=None,
        endpoint_url=None,
        use_enhanced_fan_out=False,
        consumer_name=None,
    ):
        self.stream_name = stream_name
        self.error_queue = multiprocessing.Queue()
        self.record_queue = multiprocessing.Queue()

        self.boto3_session = boto3_session or boto3.Session()
        self.endpoint_url = endpoint_url
        self.kinesis_client = self.boto3_session.client(
            "kinesis", endpoint_url=endpoint_url
        )

        self.state = state

        self.reader_sleep_time = reader_sleep_time

        # Enhanced Fan-Out settings
        self.use_enhanced_fan_out = use_enhanced_fan_out
        self.consumer_name = consumer_name
        self.consumer_arn = None
        self.stream_data = None

        self.shards = {}
        self.run = True

        # Register EFO consumer if needed
        if self.use_enhanced_fan_out:
            self._register_efo_consumer()

    def _register_efo_consumer(self):
        """Register this application as an Enhanced Fan-Out consumer"""
        stream_arn = self._get_stream_arn()
        self.consumer_arn = EnhancedFanOut.register_consumer(
            self.kinesis_client, stream_arn, self.consumer_name
        )

    def _get_stream_arn(self):
        """Get the ARN of the Kinesis stream"""
        if not self.stream_data:
            self.stream_data = self.kinesis_client.describe_stream(
                StreamName=self.stream_name
            )
        return self.stream_data["StreamDescription"]["StreamARN"]

    def shutdown_shard_reader(self, shard_id):
        try:
            self.shards[shard_id].shutdown()
            del self.shards[shard_id]
        except KeyError:
            pass

    def setup_shards(self):
        log.debug("Describing stream")
        self.stream_data = self.kinesis_client.describe_stream(
            StreamName=self.stream_name
        )
        # XXX TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

        setup_again = False
        for shard_data in self.stream_data["StreamDescription"]["Shards"]:
            # see if we can get a lock on this shard id
            shard_id = shard_data["ShardId"]
            try:
                shard_locked = self.state.lock_shard(shard_id, self.LOCK_DURATION)
            except AttributeError:
                # no self.state
                shard_locked = True
                pass

            if not shard_locked:
                # if we currently have a shard reader running we stop it
                if shard_id in self.shards:
                    log.warn(
                        "We lost our lock on shard %s, stopping shard reader", shard_id
                    )
                    self.shutdown_shard_reader(shard_id)

                # since we failed to lock the shard we just continue to the next one
                continue

            # we should try to start a shard reader if the shard id specified isn't in our shards
            if shard_id not in self.shards:
                if "EndingSequenceNumber" in shard_data["SequenceNumberRange"]:
                    log.debug(
                        "Shard %s closed, skipping shard reader creation...", shard_id
                    )
                    continue

                log.info("Shard reader for %s does not exist, creating...", shard_id)

                if self.use_enhanced_fan_out:
                    # Create EFO shard reader with appropriate starting position
                    starting_position = EnhancedFanOut.get_starting_position(
                        self.state, shard_id
                    )
                    log.info(
                        "%s EFO starting position: %s", shard_id, starting_position
                    )

                    self.shards[shard_id] = EnhancedShardReader(
                        shard_id,
                        self.consumer_arn,
                        self.record_queue,
                        self.error_queue,
                        starting_position=starting_position,
                        boto3_session=self.boto3_session,
                        endpoint_url=self.endpoint_url,
                    )
                else:
                    # Create standard shard reader
                    try:
                        iterator_args = self.state.get_iterator_args(shard_id)
                    except AttributeError:
                        # no self.state
                        iterator_args = dict(ShardIteratorType="LATEST")

                    log.info("%s iterator arguments: %s", shard_id, iterator_args)

                    # get our initial iterator
                    shard_iter = self.kinesis_client.get_shard_iterator(
                        StreamName=self.stream_name, ShardId=shard_id, **iterator_args
                    )

                    self.shards[shard_id] = ShardReader(
                        shard_id,
                        shard_iter["ShardIterator"],
                        self.record_queue,
                        self.error_queue,
                        boto3_session=self.boto3_session,
                        sleep_time=self.reader_sleep_time,
                        endpoint_url=self.endpoint_url,
                    )
            else:
                log.debug(
                    "Checking shard reader %s process at pid %d",
                    shard_id,
                    self.shards[shard_id].process.pid,
                )

                if not self.shards[shard_id].process.is_alive():
                    self.shutdown_shard_reader(shard_id)
                    setup_again = True
                else:
                    log.debug("Shard reader %s alive & well", shard_id)

        if setup_again:
            self.setup_shards()

    def shutdown(self):
        for shard_id in list(self.shards.keys()):
            log.info("Shutting down shard reader for %s", shard_id)
            self.shutdown_shard_reader(shard_id)

        # Deregister the EFO consumer if we created one
        if self.use_enhanced_fan_out and self.consumer_arn:
            EnhancedFanOut.deregister_consumer(
                self.kinesis_client, self._get_stream_arn(), self.consumer_name
            )

        self.stream_data = None
        self.shards = {}
        self.run = False

    def __iter__(self):
        try:
            # use lock duration - 1 here since we want to renew our lock before it expires
            lock_duration_check = self.LOCK_DURATION - 1
            while self.run:
                last_setup_check = time.time()
                self.setup_shards()

                while (
                    self.run and (time.time() - last_setup_check) < lock_duration_check
                ):
                    try:
                        shard_id, resp = self.record_queue.get(block=True, timeout=0.25)
                    except six.moves.queue.Empty:
                        pass
                    else:
                        for item in resp["Records"]:
                            if not self.run:
                                break

                            log.debug(item)
                            yield item

                            try:
                                self.state.checkpoint(shard_id, item["SequenceNumber"])
                            except AttributeError:
                                # no self.state
                                pass
                            except Exception:
                                log.exception(
                                    "Unhandled exception check pointing records from %s at %s",
                                    shard_id,
                                    item["SequenceNumber"],
                                )
                                self.shutdown_shard_reader(shard_id)
                                break

                    shard_id = None
                    try:
                        while True:
                            shard_id = self.error_queue.get_nowait()
                            log.error("Error received from shard reader %s", shard_id)
                            self.shutdown_shard_reader(shard_id)
                    except six.moves.queue.Empty:
                        pass

                    if shard_id is not None:
                        # we encountered an error from a shard reader, break out of the inner loop to setup the shards
                        break
        except (KeyboardInterrupt, SystemExit):
            self.run = False
        finally:
            self.shutdown()
