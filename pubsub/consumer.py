import logging

from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

# This is required to get the GCE credentials
import local_settings

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
        log.debug('Received message "%s".', body)
        print()
        print(body)
        print()
        message.ack()
        #self.should_stop = True
        #bound_queue = self.queue(self.connection)
        #bound_queue.delete()


if __name__ == '__main__':
    #from kombu.utils.debug import setup_logging
    #setup_logging(loglevel='DEBUG', loggers=[''])

    options = {'project_id': local_settings.PROJECT_ID}
    with Connection(transport='kombu_transport:Transport', transport_options=options) as connection:
        MessageConsumer(connection).run()
