"""Google Cloud Pub/Sub."""
from __future__ import absolute_import, unicode_literals

import json
import queue

from kombu.five import Empty
from kombu.log import get_logger
from kombu.transport import virtual
from kombu.utils.objects import cached_property

from google.cloud import pubsub
from google.api_core import exceptions

log = get_logger(__name__)

MAX_QUEUE_SIZE = 10


class Channel(virtual.Channel):
    """Google Cloud Pub/Sub Channel."""

    do_restore = False
    supports_fanout = False

    def __init__(self, *args, **kwargs):
        # A Connection represents a real TCP connection to the message broker,
        # whereas with AMQP, a Channel is a virtual connection (AMPQ
        # connection) inside it.  The intention is that you can use as many
        # (virtual) connections as you want inside your application without
        # overloading the broker with TCP connections. Google Cloud Pubsub
        # doesn't have a separate channel concept, so we model the connection
        # (client) at the Channel level. This should ensure thread-safety.

        super(Channel, self).__init__(*args, **kwargs)
        self._queues = {}

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Consume from `queue`."""

        def _callback(raw_message):
            serialized_payload = raw_message.data.decode('utf-8')
            payload = json.loads(serialized_payload)
            message = self.Message(payload, channel=self)

            # Remember the ack_id and queue
            if not no_ack:
                message.properties['delivery_info'].update({
                    'raw_message': raw_message,
                })
                self.qos.append(message, message.delivery_tag)

            return callback(message)

        subscription_path = self.subscriber.subscription_path(self.google_project_id, queue)
        self.subscriber.subscribe(subscription_path, callback=_callback)

    @property
    def google_project_id(self):
        return self.connection.client.transport_options['project_id']

    @cached_property
    def publisher(self):
        return pubsub.PublisherClient()

    @cached_property
    def subscriber(self):
        return pubsub.SubscriberClient()

    def _has_queue(self, queue_name, **kwargs):
        subscription_path = self.subscriber.subscription_path(self.google_project_id, queue_name)
        try:
            self.subscriber.get_subscription(subscription_path)
        except exceptions.GoogleAPICallError:
            return False
        else:
            return True

    def _delete(self, queue_name, *args, **kwargs):
        subscription_path = self.subscriber.subscription_path(self.google_project_id, queue_name)
        try:
            del self._queues[subscription_path]
            self.subscriber.delete_subscription(subscription_path)
        except Exception:
            log.exception('Error deleting queue %s', queue_name)

    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        super(Channel, self).queue_bind(queue, exchange, routing_key, **kwargs)

        topic_path = self.publisher.topic_path(self.google_project_id, routing_key)
        try:
            self.publisher.create_topic(topic_path)
        except Exception:
            log.warn('Unable to create topic.')

        subscription_path = self.subscriber.subscription_path(self.google_project_id, queue)
        try:
            self.subscriber.create_subscription(subscription_path, topic_path)
        except Exception:
            log.warn('Unable to create subscription.')

    def queue_unbind(self, queue, exchange=None, routing_key='', arguments=None, **kwargs):
        super(Channel, self).queue_unbind(queue, exchange=exchange, routing_key=routing_key, arguments=arguments, **kwargs)
        self._delete(queue)

    def _get(self, queue_name, timeout=None):
        # This method must perform a non-blocking pull from the queue.  In order to
        # make that happen, we create a python Queue and shove stuff into it
        # from inside the callback. Queue has a non-blocking 'get' method.
        # Hopefully someone doesn't try to use basic_consume() and _get() at
        # the same time...

        def callback(message):
            q.put(message)

        q = self._get_queue(queue_name)
        subscription_path = self.subscriber.subscription_path(self.google_project_id, queue_name)
        if subscription_path in self._queues:
            q = self._queues[subscription_path]
        else:
            q = queue.Queue(maxsize=MAX_QUEUE_SIZE)
            self._queues[subscription_path] = q
            self.subscriber.subscribe(subscription_path, callback=callback)

        try:
            raw_message = q.get(block=False)
        except queue.Empty:
            raise Empty

        serialized_payload = raw_message.data.decode('utf-8')
        payload = json.loads(serialized_payload)

        # Remember the ack_id and queue
        payload['properties']['delivery_info'].update({
            'raw_message': raw_message,
        })

        return payload

    def basic_ack(self, delivery_tag, multiple=False):
        info = self.qos.get(delivery_tag).delivery_info

        try:
            message = info['raw_message']
            message.ack()
        except KeyError:
            pass

        super(Channel, self).basic_ack(delivery_tag)

    def _put(self, routing_key, message, **kwargs):
        payload = json.dumps(message).encode('utf-8')
        topic_path = self.publisher.topic_path(self.google_project_id, routing_key)
        self.publisher.publish(topic_path, payload)

    def _size(self, queue):
        # Google doesn't provide a way to check the size or even emptiness of the
        # queue.  It seems best to always assume something is in the queue.
        return 1

    def _lookup(self, exchange, routing_key):
        # This method needs to return a list of things that _put can be run on.
        # We can't _put directly to a queue, so we just return the routing key,
        # which directs the _put to the topic. We double check to make sure the
        # topic exists.
        topic_path = self.publisher.topic_path(self.google_project_id, routing_key)
        try:
            self.publisher.get_topic(topic_path)
        except exceptions.GoogleAPICallError:
            raise StopIteration
        else:
            yield routing_key

    def _purge(self, queue_name):
        subscription_path = self.subscriber.subscription_path(self.google_project_id, queue_name)
        subscription = self.subscriber.get_subscription(subscription_path)
        topic_path = subscription.topic

        # Delete and recreate the queue. This seems like a bad idea, but Google
        # doesn't have a method for purging a subscription.
        self._delete(queue_name)
        try:
            self.subscriber.create_subscription(subscription_path, topic_path)
        except Exception:
            log.warn('Unable to create subscription.')


class Transport(virtual.Transport):
    """Google Cloud Pub/Sub Transport."""

    Channel = Channel

    state = virtual.BrokerState()

    implements = virtual.Transport.implements.extend(
        async=True,
        exchange_type=frozenset(['topic']),
        heartbeats=False,
    )

    driver_type = 'gps'
    driver_name = 'gps'

    def driver_version(self):
        return '0.1'
