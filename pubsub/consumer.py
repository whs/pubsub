import logging
import os

from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pubsub.settings')

MESSAGE_PREFETCH_COUNT = 1

exchange = Exchange('foobar-exchange', type='topic')

log = logging.getLogger(__name__)


class MessageConsumer(ConsumerMixin):
    queue = Queue('test-queue-2', exchange=exchange, routing_key='foobar2')

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        consumer = Consumer([self.queue], callbacks=[self.handle_message], accept=['json'])

        consumer.qos(prefetch_count=MESSAGE_PREFETCH_COUNT)
        return [consumer]

    def handle_message(self, body, message):
        print(body)
        message.ack()
        #self.should_stop = True
        #bound_queue = self.queue(self.connection)
        #bound_queue.delete()


if __name__ == '__main__':
    with Connection(transport='kombu_transport:Transport') as connection:
        MessageConsumer(connection).run()
