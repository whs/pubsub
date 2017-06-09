import logging
import os

from django.conf import settings
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pubsub.settings')

# The JSON file comes from Google and has the credentials. The Google api
# likes to use the ENV variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/bglass/src/bglass-sandbox/develop-b3efa4ff17aa.json'

MESSAGE_PREFETCH_COUNT = 1

#exchange = Exchange(settings.EVENT_EXCHANGE, type='topic')
#usage_queue = Queue(settings.USAGE_QUEUE, exchange=exchange, routing_key=settings.USAGE_ROUTING_KEY, auto_delete=True)
usage_queue = Queue('test-queue', routing_key='test-topic', auto_delete=True)

log = logging.getLogger(__name__)


class UsageConsumer(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        consumer = Consumer([usage_queue], callbacks=[self.handle_message], accept=['json'])
        consumer.qos(prefetch_count=MESSAGE_PREFETCH_COUNT)
        return [consumer]

    def handle_message(self, body, message):
        print(body)
        message.ack()


if __name__ == '__main__':
    with Connection(transport='kombu_transport:Transport') as connection:
        UsageConsumer(connection).run()
