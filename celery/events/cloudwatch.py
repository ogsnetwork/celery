import os
import ast
import sys
import time
import threading

import ujson as json
from datetime import datetime

from celery.five import items
from celery.app import app_or_default

from boto.logs import connect_to_region
from boto.logs.exceptions import ResourceNotFoundException

TASK_FAILED = 'task-failed'
TASK_STARTED = 'task-started'
TASK_RETRIED = 'task-retried'
WORKER_LOST = 'worker-offline'
TASK_RECEIVED = 'task-received'
TASK_SUCCEEDED = 'task-succeeded'
TASK_STATUSES = [
    TASK_FAILED,
    TASK_RETRIED,
    TASK_RECEIVED,
    TASK_SUCCEEDED,
    TASK_STARTED,
]
WORKER_STATUSES = [WORKER_LOST]
CLOUDWATCH_NAMESPACE = '{}-{}'.format(
    os.getenv('ENVIRONMENT_NAME', 'dev').lower(),
    os.getenv('CELERY_CLOUDWATCH_NAMESPACE', 'celery').lower()
)
MISSING_LOG_GROUP = 'log group does not exist'
MISSING_LOG_STREAM = 'log stream does not exist'
CLOUDWATCH_DEFAULT_REGION = os.getenv('CLOUDWATCH_DEFAULT_REGION', 'us-east-1')
CLOUDWATCH_AWS_ACCESS_KEY_ID = os.getenv(
    'CLOUDWATCH_AWS_ACCESS_KEY_ID', os.getenv('AWS_ACCESS_KEY_ID'))
CLOUDWATCH_AWS_SECRET_ACCESS_KEY = os.getenv(
    'CLOUDWATCH_AWS_SECRET_ACCESS_KEY', os.getenv('AWS_SECRET_ACCESS_KEY'))
tasklist = os.getenv('CLOUDWATCH_TASKS_LIST', '')
CLOUDWATCH_TASKS_LIST = tasklist.split(',') if tasklist != '' else []
CLOUDWATCH_LOG_WORKER_FAILURE = os.getenv(
    'CLOUDWATCH_LOG_WORKER_FAILURE', '0') == '1'

aws_credentials = {
    'region_name': CLOUDWATCH_DEFAULT_REGION,
    'aws_access_key_id': CLOUDWATCH_AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': CLOUDWATCH_AWS_SECRET_ACCESS_KEY
}
client = connect_to_region(**aws_credentials)


def print_msg(msg):
    if os.getenv('CLOUDWATCH_MONITOR_VERBOSE', '0') == '1':
        sys.stderr.write(msg)


class CursesCloudWatchMonitor(object):

    screen_delay = 10

    def __init__(self, state, app):
        self.app = app
        self.state = state
        self.lock = threading.RLock()

    def log(self):
        with self.lock:
            print_msg('==> Celery MonitorThread: heartbeat...\n')

    def nap(self):
        time.sleep(self.screen_delay)

    @property
    def tasks(self):
        return list(self.state.tasks_by_time(limit=self.limit))

    @property
    def workers(self):
        return [hostname for hostname, w in items(self.state.workers)
                if w.alive]

    def get_task(self, event):
        """
        Get the task state from the propagated event

        :param event: celery event object
        :type event: dict

        :return: celery task state object
        :rtype: celery task state

        """
        self.state.event(event)
        return self.state.tasks.get(event.get('uuid'))

    def get_worker(self, event):
        """
        Get the worker state from the propagated event

        :param event: celery event object
        :type event: dict

        :return: celery worker state object
        :rtype: celery worker state
        """
        self.state.event(event)
        return self.state.workers.get(event.get('hostname'))

    def get_task_dimensions(self, task, event, reason=None):
        dimensions = {
            'hostname': task.worker.hostname,
            'task_name': task.name or event.get('name', ''),
            'state': task.state,
            'timestamp': task.timestamp,
            'type': event['type'],
            'task_id': event['uuid']
        }

        if reason == TASK_FAILED or reason == TASK_RETRIED:
            dimensions.update({
                'exception': task.exception,
                'traceback': task.traceback
            })
        elif reason == TASK_SUCCEEDED:
            dimensions.update({
                'runtime': task.runtime
            })
        elif reason == TASK_RECEIVED:
            l_args = event.get('args', '[None]')
            l_args = ast.literal_eval(l_args) if isinstance(
                l_args, basestring) else l_args
            if len(l_args) > 0:
                l_args = l_args[0]
            dimensions.update({
                'job_id': l_args
            })

        return dimensions

    def get_worker_dimensions(self, worker, event, reason=None):
        dimensions = {
            'hostname': worker.hostname,
            'processed': worker.processed,
            'active': worker.active,
            'id': worker.id,
            'type': event.get('type', reason)
        }
        return dimensions

    def monitor_enabled_for_obj(self, obj, task_status):
        if not task_status:
            return False

        if task_status in TASK_STATUSES:
            if len(CLOUDWATCH_TASKS_LIST) > 0:
                return obj.name in CLOUDWATCH_TASKS_LIST
            return True
        return CLOUDWATCH_LOG_WORKER_FAILURE and task_status in WORKER_STATUSES

    def record_metric(self, obj, event, dim_func, task_status):

        def _record(namespace, log_stream, events, token=''):
            print_msg(
                'Recording worker log event: {} - {}\n'.format(
                    namespace, log_stream))
            args = [namespace, log_stream, events]
            if token:
                args.append(token)
            client.put_log_events(*args)

        if self.monitor_enabled_for_obj(obj, task_status):
            dimensions = dim_func(obj, event, reason=task_status)
            task_name = dimensions.get('task_name', '')
            log_stream = '{}-{}{}'.format(
                dimensions['type'], dimensions['hostname'],
                '-{}'.format(task_name) if task_name else '')
            events = [{
                'timestamp': int(time.time() * 1000),
                'message': json.dumps({
                    'occured_at': datetime.fromtimestamp(
                        obj.timestamp if hasattr(
                            obj, 'timestamp') else event['timestamp']),
                    'dimensions': dimensions
                })
            }]

            try:
                token = ''
                streams = client.describe_log_streams(
                    CLOUDWATCH_NAMESPACE, log_stream)
                streams = streams.get('logStreams', [])
                if streams:
                    token = streams[0]['uploadSequenceToken']
                _record(CLOUDWATCH_NAMESPACE, log_stream, events, token)
            except ResourceNotFoundException as e:
                if MISSING_LOG_GROUP in e.body.get('message', ''):
                    client.create_log_group(CLOUDWATCH_NAMESPACE)
                elif MISSING_LOG_STREAM in e.body.get('message', ''):
                    client.create_log_stream(CLOUDWATCH_NAMESPACE, log_stream)

                try:
                    _record(
                        CLOUDWATCH_NAMESPACE,
                        log_stream,
                        events
                    )
                except ResourceNotFoundException as e:
                    if MISSING_LOG_GROUP in e.body.get('message', ''):
                        client.create_log_group(CLOUDWATCH_NAMESPACE)
                    elif MISSING_LOG_STREAM in e.body.get('message', ''):
                        client.create_log_stream(
                            CLOUDWATCH_NAMESPACE, log_stream)

                    try:
                        _record(
                            CLOUDWATCH_NAMESPACE,
                            log_stream,
                            events
                        )
                    except Exception as e:
                        sys.stderr.write(
                            'Failed to record worker event: {} - {}. Error: '
                            '{}\n'.format(CLOUDWATCH_NAMESPACE, log_stream, e))
                        pass

    def on_failure(self, event):
        if os.getenv('CLOUDWATCH_ENABLE_FAILED_TASK_LOG', '0') == '1':
            print_msg('Task {} failed\n'.format(event.get('uuid', '')))
            task = self.get_task(event)
            task_name = event.get('name', task.name)
            if not task_name:
                return

            self.record_metric(
                task, event, self.get_task_dimensions, TASK_FAILED)
        return

    def on_start(self, event):
        if os.getenv('CLOUDWATCH_ENABLE_START_TASK_LOG', '0') == '1':
            print_msg('Task {} started\n'.format(event.get('uuid', '')))
            task = self.get_task(event)
            task_name = event.get('name', task.name)
            if not task_name:
                return

            self.record_metric(
                task, event, self.get_task_dimensions, TASK_STARTED)
        return

    def on_retry(self, event):
        if os.getenv('CLOUDWATCH_ENABLE_RETRY_TASK_LOG', '0') == '1':
            print_msg('Task {} retried\n'.format(event.get('uuid', '')))
            task = self.get_task(event)
            task_name = event.get('name', task.name)
            if not task_name:
                return

            self.record_metric(
                task, event, self.get_task_dimensions, TASK_RETRIED)
        return

    def on_receipt(self, event):
        if os.getenv('CLOUDWATCH_ENABLE_RECEIPT_TASK_LOG', '0') == '1':
            print_msg('Task {} received\n'.format(event.get('uuid', '')))
            task = self.get_task(event)
            task_name = event.get('name', task.name)
            if not task_name:
                return

            self.record_metric(
                task, event, self.get_task_dimensions, TASK_RECEIVED)
        return

    def on_success(self, event):
        if os.getenv('CLOUDWATCH_ENABLE_SUCCESS_TASK_LOG', '0') == '1':
            print_msg('Task {} succeeded\n'.format(event.get('uuid', '')))
            task = self.get_task(event)
            task_name = event.get('name', task.name)
            if not task_name:
                return

            self.record_metric(
                task, event, self.get_task_dimensions, TASK_SUCCEEDED)
        return

    def on_worker_loss(self, event):
        if os.getenv('CLOUDWATCH_ENABLE_WORKER_LOSS_TASK_LOG', '0') == '1':
            print_msg(
                'Worker {} lost\n'.format(event.get('hostname', '')))
            worker = self.get_worker(event)
            if worker.alive:
                return

            self.record_metric(
                worker, event, self.get_worker_dimensions, WORKER_LOST)
        return


class MonitorThread(threading.Thread):

    def __init__(self, monitor):
        self.monitor = monitor
        self.shutdown = False
        threading.Thread.__init__(self)
        self.daemon = True

    def run(self):
        while not self.shutdown:
            self.monitor.log()
            self.monitor.nap()


def capture_events(app, state, monitor):

    def conn_error(exc, interval):
        sys.stderr.write('Connection Error: {O!r}, Retry in {1}s.\n'.format(
            exc, interval))

    while True:
        sys.stderr.write('==> evtop: starting to capture events...\n')
        with app.connection() as conn:
            try:
                conn.ensure_connection(
                    conn_error, app.conf.BROKER_CONNECTION_MAX_RETRIES)
                recv = app.events.Receiver(conn, handlers={
                    'task-started': monitor.on_start,
                    'task-failed': monitor.on_failure,
                    'task-retried': monitor.on_retry,
                    'task-received': monitor.on_receipt,
                    'task-succeeded': monitor.on_success,
                    'worker-offline': monitor.on_worker_loss
                })
                recv.capture()
            except conn.connection_errors + conn.channel_errors as exc:
                sys.stderr.write('Connection lost: {0!r}.\n'.format(exc))


def evtop(
        app=None, maxrate=None, loglevel=0,
        logfile=None, pidfile=None, timer=None):

    app = app_or_default(app)
    state = app.events.State()
    monitor = CursesCloudWatchMonitor(state, app)
    refresher = MonitorThread(monitor)
    refresher.start()

    try:
        capture_events(app, state, monitor)
    except Exception:
        refresher.shutdown = True
        refresher.join()
        raise
    except (KeyboardInterrupt, SystemExit):
        refresher.shutdown = True
        refresher.join()


if __name__ == '__main__':
    evtop()
