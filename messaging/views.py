from django.shortcuts import render

from django.conf import settings
from kombu import Connection, Exchange, Queue, pools

from .tasks import say_something

TRANSPORT = 'pubsub.kombu_transport:Transport'
TRANSPORT_OPTIONS = {'project_id': settings.PROJECT_ID}

exchange = Exchange('foobar-exchange', type='topic')
queue = Queue('test-queue-2', exchange=exchange, routing_key='foobar2')

# This view tess the simplest approach to sending a message
def send_message(request):
    with Connection(transport=TRANSPORT, transport_options=TRANSPORT_OPTIONS) as connection:
        producer = connection.Producer(exchange=exchange)
        #producer = connection.Producer()
        payload = {'foo': 'bar'}
        topic = 'foobar2'
        producer.publish(payload, routing_key=topic, declare=[queue])
        return render(request, 'send_message.html')

# Here we use Kombu pools to makes sure the transport will work in the context
# of the message sending machinery we use internally.
connection = Connection(transport=TRANSPORT, transport_options=TRANSPORT_OPTIONS)
connection_pool = connection.Pool(limit=100)
producer_pool = pools.ProducerPool(connection_pool, limit=100)

def send_message_pools(request):
    with producer_pool.acquire() as producer:
        payload = {'foo': 'bar'}
        topic = 'foobar2'
        producer.publish(payload, routing_key=topic, declare=[queue])
        return render(request, 'send_message.html')

# Let's see if we can get a celery task to work.
def run_task(request):
    say_something.delay('Twas brillig and the slithy toves.')
    return render(request, 'send_message.html')
