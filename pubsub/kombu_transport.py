"""Google Cloud Pub/Sub."""
from __future__ import absolute_import, unicode_literals

import fnmatch
import json
import queue

from kombu.five import Empty
from kombu.log import get_logger
from kombu.transport import virtual
from kombu.utils.objects import cached_property

from google.cloud import pubsub
from google.api_core import exceptions

log = get_logger(__name__)


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
        log.debug('Pubsub channel created.')

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Consume from `queue`."""

        log.debug('Calling basic_consume: %s, %s, %s %s.', queue, no_ack, callback, consumer_tag)

        def _callback(raw_message):
            serialized_payload = raw_message.data.decode('utf-8')
            payload = json.loads(serialized_payload)
            message = self.Message(payload, channel=self)
            log.debug('Got payload: %s', message)

            # Remember the ack_id and queue
            if not no_ack:
                message.properties['delivery_info'].update({
                    'raw_message': raw_message,
                })
                self.qos.append(message, message.delivery_tag)

            return callback(message)

        subscription_path = self.subscriber.subscription_path(self.project_id, queue)
        self.subscriber.subscribe(subscription_path, callback=_callback)

    @property
    def project_id(self):
        return self.connection.client.transport_options['project_id']

    @cached_property
    def publisher(self):
        return pubsub.PublisherClient()

    @cached_property
    def subscriber(self):
        return pubsub.SubscriberClient()

    def _has_queue(self, queue_name, **kwargs):
        subscription_path = self.subscriber.subscription_path(self.project_id, queue_name)
        try:
            self.subscriber.get_subscription(subscription_path)
        except exceptions.GoogleAPICallError:
            return False
        else:
            return True

    def _delete(self, queue_name, *args, **kwargs):
        subscription_path = self.subscriber.subscription_path(self.project_id, queue_name)
        try:
            self.subscriber.delete_subscription(subscription_path)
        except Exception:
            log.exception('Error deleting queue %s', queue_name)

    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        super(Channel, self).queue_bind(queue, exchange, routing_key, **kwargs)

        topic_path = self.publisher.topic_path(self.project_id, routing_key)
        try:
            self.publisher.create_topic(topic_path)
        except Exception:
            log.warn('Unable to create topic.')

        subscription_path = self.subscriber.subscription_path(self.project_id, queue)
        try:
            self.subscriber.create_subscription(subscription_path, topic_path)
        except Exception:
            log.warn('Unable to create subscription.')

    def _get(self, queue_name, timeout=None):
        # This should perform a non-blocking pull from the queue.

        q = self._get_queue(queue_name)

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

        log.info('Successfully pulled from queue.')
        return payload

    def basic_ack(self, delivery_tag, multiple=False):
        info = self.qos.get(delivery_tag).delivery_info

        try:
            message = info['raw_message']
            message.ack()
            log.debug('Message was acked.')
        except KeyError:
            pass

        super(Channel, self).basic_ack(delivery_tag)

    def _put(self, routing_key, message, **kwargs):
        payload = json.dumps(message).encode('utf-8')
        topic_path = self.publisher.topic_path(self.project_id, routing_key)
        self.publisher.publish(topic_path, payload)

    #def _size(self, queue):
    #    return 1

    def _lookup(self, exchange, routing_key):
        log.debug('looking up topics: "%s" on exchange: "%s".', routing_key, exchange)

        if set('[*?') & set(routing_key):
            # This is a hack to provide wildcards
            for topic in self.publisher.list_topics():
                topic_path = self.publisher.topic_path(self.project_id, routing_key)
                if fnmatch.fnmatch(topic.name, topic_path):
                    yield self.publisher.match_topic_from_topic_name(topic.name)
        else:
            # We take a shortcut here since we have no wildcard characters in
            # the routing_key
            topic_path = self.publisher.topic_path(self.project_id, routing_key)
            try:
                self.publisher.get_topic(topic_path)
            except exceptions.GoogleAPICallError:
                raise StopIteration
            else:
                yield routing_key

    def _get_queue(self, queue_name, **kwargs):
        # This is local to this code and isn't called outside
        subscription_path = self.subscriber.subscription_path(self.project_id, queue_name)
        if subscription_path in self._queues:
            return self._queues[subscription_path]

        q = queue.Queue()

        def callback(message):
            q.put(message)

        self._queues[subscription_path] = q
        self.subscriber.subscribe(subscription_path, callback=callback)

        return q

    def _purge(self, queue_name):
        log.info('Purging queue: "%s".', queue)

        subscription_path = self.subscriber.subscription_path(self.project_id, queue_name)
        subscription = self.subscriber.get_subscription(subscription_path)
        topic_path = subscription.topic

        # Delete and recreate the queue.
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
