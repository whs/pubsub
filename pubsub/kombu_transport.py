"""Google Cloud Pub/Sub."""
from __future__ import absolute_import, unicode_literals

import json
import os

from kombu.five import Queue, values
from kombu.transport import base, virtual
from kombu.utils.objects import cached_property

from google.cloud import pubsub

# The JSON file comes from Google and has the credentials. The Google api
# likes to use the ENV variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/bglass/src/bglass-sandbox/develop-b3efa4ff17aa.json'


class Channel(virtual.Channel):
    """In-memory Channel."""

    acknowledgements = {}
    do_restore = False
    supports_fanout = True
    default_topic = 'my-new-topic'

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)

        # Prepares the new topic
        topic = self.gps.topic(self.default_topic)

        # Creates the new topic
        if not topic.exists():
            topic.create()

    @cached_property
    def gps(self):
        return pubsub.Client()

    def _has_queue(self, queue, **kwargs):
        subscription = pubsub.Subscription(queue, client=self.gps)
        return subscription.exists()

    def _new_queue(self, queue, **kwargs):
        topic = self.gps.topic(self.default_topic)
        queue = topic.subscription('test-queue')
        if not queue.exists():
            queue.create()

    def _get(self, queue, timeout=None):
        results = self._queue_for(queue).pull()
        ack_id, message = results[0]
        serialized_payload = message.data.decode('utf-8')
        payload = json.loads(serialized_payload)
        delivery_tag = payload['properties']['delivery_tag']
        self.acknowledgements[delivery_tag] = ack_id, queue
        return payload

    def _queue_for(self, queue):
        topic = self.gps.topic(self.default_topic)
        queue = topic.subscription(queue)
        return queue

    def _queue_bind(self, *args):
        pass

    def basic_ack(self, delivery_tag, multiple=False):
        ack_id, queue = self.acknowledgements[delivery_tag]
        topic = self.gps.topic(self.default_topic)
        queue = topic.subscription(queue)
        queue.acknowledge([ack_id])
        del self.acknowledgements[delivery_tag]
        super(Channel, self).basic_ack(delivery_tag)

    #def _put_fanout(self, exchange, message, routing_key=None, **kwargs):
    #    for queue in self._lookup(exchange, routing_key):
    #        self._queue_for(queue).put(message)

    def _put(self, queue, message, **kwargs):
        topic = self.gps.topic(self.default_topic)
        payload = json.dumps(message).encode('utf-8')
        topic.publish(payload)

    #def _size(self, queue):
    #    return self._queue_for(queue).qsize()

    #def _delete(self, queue, *args, **kwargs):
    #    self.queues.pop(queue, None)

    #def _purge(self, queue):
    #    q = self._queue_for(queue)
    #    size = q.qsize()
    #    q.queue.clear()
    #    return size

    #def close(self):
    #    super(Channel, self).close()
    #    for queue in values(self.queues):
    #        queue.empty()
    #    self.queues = {}

    def after_reply_message_received(self, queue):
        pass


class Transport(virtual.Transport):
    """In-memory Transport."""

    Channel = Channel

    #: memory backend state is global.
    state = virtual.BrokerState()

    implements = base.Transport.implements

    driver_type = 'gps'
    driver_name = 'gps'

    def driver_version(self):
        return 'N/A'
