"""Google Cloud Pub/Sub."""
from __future__ import absolute_import, unicode_literals

import fcntl
import fnmatch
import json
import logging
import os
import socket

from kombu.five import Empty
from kombu.transport import virtual
from kombu.utils.objects import cached_property

from google.cloud import pubsub

from django.conf import settings

log = logging.getLogger(__name__)

PROJECT_ID = settings.PROJECT_ID


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
        log.debug('Pubsub channel created.')

    @cached_property
    def publisher(self):
        return pubsub.PublisherClient()

    @cached_property
    def subscriber(self):
        return pubsub.SubscriberClient()

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        log.debug('Calling basic_consume: %s, %s, %s %s.', queue, no_ack, callback, consumer_tag)

        # return super(Channel, self).basic_consume(queue, no_ack, callback, consumer_tag, **kwargs)

        self._tag_to_queue[consumer_tag] = queue
        self._active_queues.append(queue)

        def _callback(raw_message):
            log.debug('Calling _callback.')
            message = self.Message(raw_message, channel=self)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
            return callback(message)

        self.connection._callbacks[queue] = _callback
        self._consumers.add(consumer_tag)

        log.debug('Resetting cycle.')
        self._reset_cycle()
        log.debug('Finished calling basic_consume.')

    def _get_queue(self, queue, **kwargs):
        log.debug('Fetching queue.')

        subscription_path = self.subscriber.subscription_path(PROJECT_ID, queue)
        return self.subscriber.get_subscription(subscription_path)

    #def _has_queue(self, queue, **kwargs):
    #    #log.debug('Checking for queue existence.')
    #    #return self._get_queue(queue).exists()
    #    return True


    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        log.debug('Binding Queue "%s" to topic: "%s" on exchange "%s".', queue, routing_key, exchange)

        super(Channel, self).queue_bind(queue, exchange, routing_key, **kwargs)

        topic_path = self.publisher.topic_path(PROJECT_ID, routing_key)
        try:
            self.publisher.create_topic(topic_path)
        except Exception as e:
            log.warn('Unable to create topic.')

        subscription_path = self.subscriber.subscription_path(PROJECT_ID, queue)
        try:
            self.subscriber.create_subscription(subscription_path, topic_path)
        except Exception as e:
            log.warn('Unable to create subscription.')

        #topic = self._broker.topic(routing_key)
        #if not topic.exists():
        #    topic.create()
        #    log.info('Created topic: "%s".', topic.name)

        #q = topic.subscription(queue)
        #if not q.exists():
        #    q.create()
        #    log.info('Created queue (Google Pubsub subscription): "%s".', q.name)

    def _get(self, queue, timeout=None):
        # This should perform a non-blocking pull from the queue.
        log.debug('Pulling from queue.')

        q = self._get_queue(queue)
        subscription = self.subscriber.subscribe(q.name)

        # q._request_queue is an instance of queue.Queue
        # This is a bit of a hack because the client library doesn't support
        # this type of polling.
        the_message = None
        def handler(message):
            log.info('Running the get handler.')
            the_message = message

        subscription.open(handler)
        from queue import Empty as QueueEmpty
        import time
        try:
            time.sleep(10)
            result = subscription._request_queue.get(block=False)
        except QueueEmpty:
            raise Empty
        finally:
            #subscription.close()
            pass

        #ack_id, message = results[0]

        serialized_payload = message.data.decode('utf-8')
        payload = json.loads(serialized_payload)

        # Remember the ack_id and queue
        payload['properties']['delivery_info'].update({
            'ack_id': ack_id,
            'queue': queue,
        })

        log.info('Successfully pulled from queue.')
        return payload

    def basic_ack(self, delivery_tag, multiple=False):
        message = self.qos.get(delivery_tag).delivery_info

        try:
            #q = self._get_queue(message['queue'])
            #q.acknowledge([message['ack_id']])
            message.ack()
            log.debug('Message was acked.')
        except KeyError:
            pass

        super(Channel, self).basic_ack(delivery_tag)

    def _put(self, routing_key, message, **kwargs):
        #topic = self._broker.topic(routing_key)
        payload = json.dumps(message).encode('utf-8')
        #topic.publish(payload)

        topic_path = self.publisher.topic_path(PROJECT_ID, routing_key)
        #topic = self.publisher.topic(topic_path)
        self.publisher.publish(topic_path, payload)
        log.debug('Message was published to topic: "%s".', routing_key)

    def _size(self, queue):
        return 1

    def _lookup(self, exchange, routing_key):
        log.debug('looking up topics: "%s" on exchange: "%s".', routing_key, exchange)

        if set('[*?') & set(routing_key):
            # This is a hack to provide wildcards
            for topic in self.publisher.list_topics():
                topic_path = self.publisher.topic_path(PROJECT_ID, routing_key)
                if fnmatch.fnmatch(topic.name, topic_path):
                    yield self.publisher.match_topic_from_topic_name(topic.name)
        else:
            # We take a shortcut here since we have no wildcard characters in
            # the routing_key
            topic_path = self.publisher.topic_path(PROJECT_ID, routing_key)
            try:
                topic = self.publisher.get_topic(topic_path)
            except Exception:
                # Topic does not exist
                pass
            else:
                yield self.publisher.match_topic_from_topic_name(topic.name)

    '''
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
    '''

    def _poll(self, cycle, callback, timeout=None):
        """Poll a list of queues for available messages."""
        log.debug('*********************Running _poll*********************')
        return super(Channel, self)._poll(cycle, callback, timeout=timeout)

    def _get_and_deliver(self, queue, callback):
        log.debug('*********************Running _get_and_deliver *********************')
        return super(Channel, self)._get_and_deliver(queue, callback)

    def drain_events(self, timeout=None, callback=None):
        log.debug('Running drain_events')
        return super(Channel, self).drain_events(timeout=timeout, callback=callback)

    def close(self):
        log.debug('Closing Pubsub channel: %s.', self._active_queues)

        super(Channel, self).close()

        # The pubsub client doesn't have a way to explicitly
        # close(). We'll just hope Python cleans it up.
        # delattr(self, '_broker')

    def after_reply_message_received(self, queue):
        pass


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

    def drain_events(self, connection, timeout=None):
        log.debug('***************Running drain_events**************')
        return super(Transport, self).drain_events(connection, timeout=timeout)

    def _drain_channel(self, channel, callback, timeout=None):
        log.debug('***************Running _drain_channel**************')
        return super(Transport, self)._drain_channel(channel, callback, timeout=timeout)

    '''
    def register_with_event_loop(self, connection, loop):
        """Register a file descriptor and callback with the loop.

        Register the callback self.on_readable to be called when an
        external epoll loop sees that the file descriptor registered is
        ready for reading. The file descriptor is created by this Transport,
        and is written to when a message is available.

        Because supports_ev == True, Celery expects to call this method to
        give the Transport an opportunity to register a read file descriptor
        for external monitoring by celery using an Event I/O notification
        mechanism such as epoll. A callback is also registered that is to
        be called once the external epoll loop is ready to handle the epoll
        event associated with messages that are ready to be handled for
        this Transport.

        The registration call is made exactly once per Transport after the
        Transport is instantiated.

        :param connection: A reference to the connection associated with
            this Transport.
        :type connection: kombu.transport.qpid.Connection
        :param loop: A reference to the external loop.
        :type loop: kombu.async.hub.Hub

        """
        self.r, self._w = os.pipe()
        if fcntl is not None:
            fcntl.fcntl(self.r, fcntl.F_SETFL, os.O_NONBLOCK)
        self.use_async_interface = True
        loop.add_reader(self.r, self.on_readable, connection, loop)

    def on_readable(self, connection, loop):
        """Handle any messages associated with this Transport.

        This method clears a single message from the externally monitored
        file descriptor by issuing a read call to the self.r file descriptor
        which removes a single '0' character that was placed into the pipe
        by the Qpid session message callback handler. Once a '0' is read,
        all available events are drained through a call to
        :meth:`drain_events`.

        The file descriptor self.r is modified to be non-blocking, ensuring
        that an accidental call to this method when no more messages will
        not cause indefinite blocking.

        Nothing is expected to be returned from :meth:`drain_events` because
        :meth:`drain_events` handles messages by calling callbacks that are
        maintained on the :class:`~kombu.transport.qpid.Connection` object.
        When :meth:`drain_events` returns, all associated messages have been
        handled.

        This method calls drain_events() which reads as many messages as are
        available for this Transport, and then returns. It blocks in the
        sense that reading and handling a large number of messages may take
        time, but it does not block waiting for a new message to arrive. When
        :meth:`drain_events` is called a timeout is not specified, which
        causes this behavior.

        One interesting behavior of note is where multiple messages are
        ready, and this method removes a single '0' character from
        self.r, but :meth:`drain_events` may handle an arbitrary amount of
        messages. In that case, extra '0' characters may be left on self.r
        to be read, where messages corresponding with those '0' characters
        have already been handled. The external epoll loop will incorrectly
        think additional data is ready for reading, and will call
        on_readable unnecessarily, once for each '0' to be read. Additional
        calls to :meth:`on_readable` produce no negative side effects,
        and will eventually clear out the symbols from the self.r file
        descriptor. If new messages show up during this draining period,
        they will also be properly handled.

        :param connection: The connection associated with the readable
            events, which contains the callbacks that need to be called for
            the readable objects.
        :type connection: kombu.transport.qpid.Connection
        :param loop: The asynchronous loop object that contains epoll like
            functionality.
        :type loop: kombu.async.Hub

        """
        os.read(self.r, 1)
        try:
            self.drain_events(connection)
        except socket.timeout:
            pass
    '''
