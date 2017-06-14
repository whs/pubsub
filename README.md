# pubsub
Prototype Kombu transport for Google pubsub
-------------------------------------------

This is a simple Django app with a Kombu consumer. The consumer pulls messages from Google pubsub. 
The only view publishes messages to Google pubsub. The key piece is the Kombu transport in pubsub/kombu_transport.py.
