# pubsub
Prototype Kombu transport for Google Cloud Pub/Sub
--------------------------------------------------

This is a simple Django prototype project with a Kombu consumer. The key piece is the Kombu transport in
[kombu_transport.py](pubsub/kombu_transport.py). There are a few views that
send messages in one way or another. There is also a consumer that pulls those messages.

To install:

    virtualenv -v python3.5 ve
    . ve/bin/activate
    pip install -r requirements.txt

Copy [pubsub/local_settings.py.example](pubsub/local_settings.py.example) to pubsub/local_settings.py and edit the values appropriately.
To authenticate with Google, acquire a GCE credentials json file and store it
locally. Change the GOOGLE_APPLICATION_CREDENTIALS value in
local_settings.py. Also create an obscure secret for the local_settings.py.

To run the consumer:

    python pubsub/consumer.py

To run a celery worker:

    celery -A pubsub worker -l debug

To run the web server:

    ./manage.py runserver

To send a message, browse to any of the following:
* http://localhost:8000/send-message/
* http://localhost:8000/send-message-pools/
* http://localhost:8000/run-task/
