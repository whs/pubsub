import shlex
import subprocess

from celery.utils.log import get_task_logger

from pubsub.celery import app

log = get_task_logger(__name__)

@app.task(acks_late=True, ignore_result=True, max_retries=10)
def say_something(something):
    log.info('We got: %s', something)

    # This assumes we are developing on a Mac
    command = 'say {}'.format(something)
    subprocess.call(shlex.split(command))
