import logging

from pubsub.celery import app

log = logging.getLogger(__name__)

@app.task
def flubber_dubber(stuff):
    print(stuff)
    log.info(stuff)
