# gunicorn_config.py
import os

workers_number = int(os.environ['GUNICORN_WORKER_NUM'] if "GUNICORN_WORKER_NUM" in os.environ else "3")

bind = '0.0.0.0:56789'
keepalive = 75
workers = workers_number
worker_connections = 1024
backlog = 100
timeout = 30
threads = 1
worker_class = 'gevent'
loglevel = os.environ['LOG_LEVEL'] if "LOG_LEVEL" in os.environ else "info"