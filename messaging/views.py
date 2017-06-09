import os

from django.shortcuts import render

from kombu import Connection, transport

#transport.DEFAULT_TRANSPORT = 'pubsub.kombu_transport:Transport'

# The JSON file comes from Google and has the credentials. The Google api
# likes to use the ENV variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/bglass/src/bglass-sandbox/develop-b3efa4ff17aa.json'

def send_message(request):
    with Connection(transport='pubsub.kombu_transport:Transport') as connection:
        producer = connection.Producer()
        payload = {'foo': 'bar'}
        topic = 'test-topic'
        producer.publish(
            payload,
            routing_key=topic,
            #headers=headers,
            #serializer=serializer,
            #**kwargs
        )
        return render(request, 'send_message.html')
