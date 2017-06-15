"""Google Cloud Pub/Sub."""
from __future__ import absolute_import, unicode_literals

import fnmatch
import logging
import json
import os

from kombu.five import Empty
from kombu.transport import base, virtual

from google.cloud import pubsub

## The JSON file comes from Google and has the credentials. The Google api
## likes to use the ENV variable
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/bglass/src/bglass-sandbox/develop-b3efa4ff17aa.json'

log = logging.getLogger(__name__)


class Channel(virtual.Channel):
    """Google Cloud Pub/Sub Channel."""

    do_restore = False
    supports_fanout = False

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self._broker = pubsub.Client()
        log.debug('Pubsub channel created.')

    def _get_queue(self, queue):
        # pubsub doesn't allow multiple bindings, so we grab the first one.
        log.debug('Fetching queue.')

        binding = next(self.state.queue_bindings(queue))

        topic = self._broker.topic(binding.routing_key)
        return topic.subscription(queue)

    def _has_queue(self, queue, **kwargs):
        log.debug('Checking for queue existence.')
        return self._get_queue(queue).exists()

    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        log.debug('Binding Queue "%s" to topic: "%s".', queue, routing_key)

        super(Channel, self).queue_bind(queue, exchange, routing_key, **kwargs)

        topic = self._broker.topic(routing_key)
        if not topic.exists():
            topic.create()
            log.info('Created topic: "%s".', topic.name)

        q = topic.subscription(queue)
        if not q.exists():
            q.create()
            log.info('Created queue (Google Pubsub subscription): "%s".', q.name)

    def _get(self, queue, timeout=None):
        log.debug('Pulling from queue.')

        q = self._get_queue(queue)
        results = q.pull()
        if not results:
            raise Empty()

        ack_id, message = results[0]

        serialized_payload = message.data.decode('utf-8')
        payload = json.loads(serialized_payload)

        # Cache the ack_id and queue
        payload['properties']['delivery_info'].update({
            'ack_id': ack_id,
            'queue': queue,
        })

        log.info('Successfully pulled from queue.')
        return payload

    def basic_ack(self, delivery_tag, multiple=False):
        message = self.qos.get(delivery_tag).delivery_info

        try:
            q = self._get_queue(message['queue'])
            q.acknowledge([message['ack_id']])
            log.debug('Message was acked.')
        except KeyError:
            pass

        super(Channel, self).basic_ack(delivery_tag)

    def _put(self, routing_key, message, **kwargs):
        topic = self._broker.topic(routing_key)
        payload = json.dumps(message).encode('utf-8')
        topic.publish(payload)
        log.debug('Message was published to topic: "%s".', routing_key)

    def _lookup(self, exchange, routing_key):
        log.debug('looking up queues bound to topic: "%s".', routing_key)

        if set('[*?') & set(routing_key):
            # This is a hack to provide wildcards
            for topic in self._broker.list_topics():
                if fnmatch.fnmatch(topic.name, routing_key):
                    yield topic.name
        else:
            # We take a shortcut here since we have no wildcard characters in
            # the routing_key
            topic = self._broker.topic(routing_key)
            if topic.exists():
                yield topic.name

    def _delete(self, queue, *args, **kwargs):
        log.debug('deleting queue.')
        self._get_queue(queue).delete()

    def _purge(self, queue):
        log.info('Purging queue: "%s".', queue)

        q = self._get_queue(queue)

        # Delete and recreate the queue.
        # See https://stackoverflow.com/questions/39398173/best-practices-for-draining-or-clearing-a-google-cloud-pubsub-topic
        q.delete()
        q.create()

        # pubsub doesn't have a way to find out how big the queue is, so we
        # return 0
        return 0

    def _poll(self, cycle, callback, timeout=None):
        """Poll a list of queues for available messages."""
        log.debug('Running _poll')
        return super(Transport, self)._poll(cycle, callback, timeout=timeout)

    def drain_events(self, timeout=None, callback=None):
        log.debug('Running drain_events')
        return super(Transport, self).drain_events(timeout=timeout, callback=callback)

    def close(self):
        log.debug('Closing Pubsub channel.')

        super(Channel, self).close()

        # The pubsub client doesn't have way to explicitly
        # close(). We'll just hope Python cleans it up.
        delattr(self, '_broker')

    def after_reply_message_received(self, queue):
        pass


class Transport(virtual.Transport):
    """Google Cloud Pub/Sub Transport."""

    Channel = Channel

    state = virtual.BrokerState()

    implements = virtual.Transport.implements.extend(
        async=True,
        exchange_type=frozenset(['topic'])
    )

    driver_type = 'gps'
    driver_name = 'gps'

    def driver_version(self):
        return '0.1'

    def create_channel(self, connection):
        try:
            return self._avail_channels.pop()
        except IndexError:
            channel = self.Channel(connection)
            self.channels.append(channel)
            return channel

    def establish_connection(self):
        # creates channel to verify connection.
        # this channel is then used as the next requested channel.
        # (returned by ``create_channel``).
        self._avail_channels.append(self.create_channel(self))
        return self     # for drain events
