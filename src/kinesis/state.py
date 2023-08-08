from __future__ import absolute_import
from datetime import datetime, timedelta

import logging
import socket
import time

import boto3
from botocore.exceptions import ClientError

from .exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)

STREAM_RETENTION_PERIOD_IN_HOURS = 24

class DynamoDB(object):
    def __init__(self, table_name, consumer_name, stream_name, boto3_session=None, endpoint_url=None):
        self.boto3_session = boto3_session or boto3.Session()

        self.dynamo_resource = self.boto3_session.resource('dynamodb', endpoint_url=endpoint_url)
        self.dynamo_table = self.dynamo_resource.Table(table_name)
        self.key = {
            "consumerGroup": consumer_name,
            "streamName": stream_name
        }

        self.shards = {}

    def get_iterator_args(self, shard_id):
        iterator_args = {'ShardIteratorType': 'LATEST'}

        if shard_id not in self.shards:
            return iterator_args

        heartbeat = self.shards[shard_id].get('heartbeat')
        last_sequence_number = self.shards[shard_id].get('checkpoint')

        if not heartbeat or not last_sequence_number:
            return iterator_args

        try:
            heartbeat_time = datetime.strptime(heartbeat, '%Y-%m-%dT%H:%M:%S.%fZ')
            heartbeat_diff = datetime.utcnow() - heartbeat_time
        except ValueError:
            heartbeat_diff = timedelta.max

        if heartbeat_diff > timedelta(hours=1 * STREAM_RETENTION_PERIOD_IN_HOURS):
            log.info({
                'message': 'Heartbeat is stale, defaulting to LATEST',
                'sequence_number': last_sequence_number,
                'heartbeat': heartbeat
            })
        else:
            iterator_args = {
                'ShardIteratorType': 'AFTER_SEQUENCE_NUMBER',
                'StartingSequenceNumber': last_sequence_number
            }

        return iterator_args

    def checkpoint(self, shard_id, seq):
        heartbeat = datetime.utcnow().isoformat() + 'Z'

        try:
            # update the seq attr in our item
            # ensure our fqdn still holds the lock and the new seq is bigger than what's already there
            self.dynamo_table.update_item(
                Key=self.key,
                UpdateExpression="""
                    SET
                        shards.#shard_id.checkpoint = :seq,
                        shards.#shard_id.heartbeat = :heartbeat
                """,
                ConditionExpression="""
                    attribute_not_exists(shards.#shard_id.checkpoint) OR
                    shards.#shard_id.checkpoint < :seq
                """,
                ExpressionAttributeValues={
                    ":heartbeat": heartbeat,
                    ":shard_id": shard_id,
                    ":seq": seq,
                }
            )
        except ClientError as exc:
            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                log.warn("Throttled while trying to read lock table in Dynamo: %s", exc)
                time.sleep(1)

            # for all other exceptions (including condition check failures) we just re-raise
            raise

    def lock_shard(self, shard_id, expires):
        fqdn = socket.getfqdn()
        now = time.time()
        expires = int(now + expires)  # dynamo doesn't support floats

        try:
            # Do a consistent read to get the current document for our shard id
            resp = self.dynamo_table.get_item(Key=self.key, ConsistentRead=True)
            self.shards = resp['Item']['shards']
        except KeyError:
            # if there's no Item in the resp then the document didn't exist
            pass
        except ClientError as exc:
            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                log.warn("Throttled while trying to read lock table in Dynamo: %s", exc)
                time.sleep(1)
                return self.lock_shard(shard_id)

            # all other client errors just get re-raised
            raise
        else:
            if fqdn != self.shards[shard_id]['fqdn'] and now < self.shards[shard_id]['expires']:
                # we don't hold the lock and it hasn't expired
                log.debug("Not starting reader for shard %s -- locked by %s until %s",
                          shard_id, self.shards[shard_id]['fqdn'], self.shards[shard_id]['expires'])
                return False

        try:
            # Try to acquire the lock by setting our fqdn and calculated expires.
            # We add a condition that ensures the fqdn & expires from the document we loaded hasn't changed to
            # ensure that someone else hasn't grabbed a lock first.
            self.dynamo_table.update_item(
                Key=self.key,
                UpdateExpression="""
                    SET
                        shards.#shard_id.fqdn = :new_fqdn,
                        shards.#shard_id.expires = :new_expires
                """,
                ConditionExpression="""
                    shards.#shard_id.fqdn = :current_fqdn AND
                    shards.#shard_id.expires = :current_expires
                """,
                ExpressionAttributeValues={
                    ':new_fqdn': fqdn,
                    ':new_expires': expires,
                    ':current_fqdn': self.shards[shard_id]['fqdn'],
                    ':current_expires': self.shards[shard_id]['expires'],
                    ':shard_id': shard_id
                }
            )
            self.shards[shard_id].update({
                'fqdn': fqdn,
                'expires': expires
            })
        except KeyError:
            # No previous lock - this occurs because we try to reference the shard info in the attr values but we don't
            # have one.  Here our condition prevents a race condition with two readers starting up and both adding a
            # lock at the same time.
            self.dynamo_table.update_item(
                Key=self.key,
                UpdateExpression="SET shards = :empty",
                ConditionExpression="attribute_not_exists(shards)",
                ExpressionAttributeValues={
                    ':empty': {}
                },
            )
            shard = {
                'fqdn': fqdn,
                'expires': expires
            }
            self.dynamo_table.update_item(
                Key=self.key,
                UpdateExpression="SET shards.#shard_id = :shard",
                ConditionExpression="attribute_not_exists(shards.#shard_id)",
                ExpressionAttributeValues={
                    ':shard': shard
                },
                ExpressionAttributeNames={
                    # 'shard' is a reserved word in expressions so we need to use a bound name to work around it
                    '#shard_id': shard_id,
                },
                ReturnValues='ALL_NEW'
            )
            self.shards[shard_id] = shard
        except ClientError as exc:
            if exc.response['Error']['Code'] == "ConditionalCheckFailedException":
                # someone else grabbed the lock first
                return False

            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                log.warn("Throttled while trying to write lock table in Dynamo: %s", exc)
                time.sleep(1)
                return self.should_start_shard_reader(shard_id)

        # we now hold the lock (or we don't use dynamo and don't care about the lock)
        return True
