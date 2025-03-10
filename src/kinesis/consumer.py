from __future__ import absolute_import

import logging
import multiprocessing
import time

import boto3
import six.moves.queue

from .enhanced_fan_out import EnhancedFanOut
from .enhanced_shard_reader import EnhancedShardReader
from .shard_reader import ShardReader

log = logging.getLogger(__name__)


class KinesisConsumer(object):
    # These control how the lock updating and/or releasing behaves
    LOCK_DURATION = 30
    # This controls how often we update locks.  By default, every 1/3rd of the lock duration.
    LOCK_UPDATE_PERIOD = LOCK_DURATION / 3

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
        """
        Create a kinesis consumer.

        Args:
            stream_name (str): name of kinesis stream
            boto3_session (boto3.Session): session object to create boto clients with
            state (DynamoDB): state instance for tracking shard checkpoints/ownership
            reader_sleep_time (float): sleep time between calls for each reader
            endpoint_url (str): url endpoint for Kinesis API
            use_enhanced_fan_out (bool): Whether to use Enhanced Fan-Out (EFO)
            consumer_name (str): Optional name for EFO consumer. If not provided, a unique name will be generated.
        """
        self.run = True
        self.stream_name = stream_name
        self.boto3_session = boto3_session or boto3.Session()
        self.kinesis_client = self.boto3_session.client(
            "kinesis", endpoint_url=endpoint_url
        )
        self.state = state
        self.endpoint_url = endpoint_url
        self.reader_sleep_time = reader_sleep_time
        self.error_queue = multiprocessing.Queue()
        self.record_queue = multiprocessing.Queue()
        self.shards = {}
        self.stream_data = None

        # Enhanced Fan-Out related fields
        self.use_enhanced_fan_out = use_enhanced_fan_out
        self.consumer_name = consumer_name
        self.consumer_arn = None

        if self.use_enhanced_fan_out:
            self._setup_enhanced_fan_out()

    def _setup_enhanced_fan_out(self):
        """Set up Enhanced Fan-Out consumer registration"""
        if not self.consumer_name:
            self.consumer_name = EnhancedFanOut.generate_consumer_name()

        stream_arn = self._get_stream_arn()
        self.consumer_arn = EnhancedFanOut.register_consumer(
            self.kinesis_client, stream_arn, self.consumer_name
        )
        log.info("Using Enhanced Fan-Out with consumer: %s", self.consumer_name)

    def _get_stream_arn(self):
        """Get the ARN of the stream"""
        if not self.stream_data:
            self.stream_data = self.kinesis_client.describe_stream(
                StreamName=self.stream_name
            )
        return self.stream_data["StreamDescription"]["StreamARN"]

    def shutdown_shard_reader(self, shard_id):
        try:
            if shard_id in self.shards:
                self.shards[shard_id].shutdown()
                del self.shards[shard_id]
        except Exception:
            log.exception(
                "Exception occurred shutting down shard reader for %s", shard_id
            )

    def setup_shards(self):
        # list the shards for the current stream
        if not self.stream_data:
            self.stream_data = self.kinesis_client.describe_stream(
                StreamName=self.stream_name
            )

        if not self.run:
            return

        setup_again = False
        for shard_data in self.stream_data["StreamDescription"]["Shards"]:
            shard_id = shard_data["ShardId"]

            # check if we've already started a reader for this shard
            if shard_id in self.shards:
                continue

            # if we're using dynamo for state, we need a lock
            if hasattr(self.state, "lock_shard"):
                if not self.state.lock_shard(shard_id, self.LOCK_DURATION):
                    log.info("Unable to get lock for shard {}".format(shard_id))
                    continue

            # Create appropriate reader based on consumption mode
            if self.use_enhanced_fan_out:
                # Get starting position for EFO
                starting_position = EnhancedFanOut.get_starting_position(
                    self.state, shard_id
                )
                log.info("%s EFO starting position: %s", shard_id, starting_position)

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

        # check to see if any of our shard readers have exit.  If they have, then let's restart them.
        for shard_id in list(self.shards.keys()):
            if shard_id not in self.shards:
                # this shard id was deleted in the inner loop via a shutdown_shard_reader call.
                # we should schedule an immediate restart, so we don't need to check it.
                setup_again = True
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
