from subprocess import Popen, PIPE

import pika

from log import tolog
from config import *


class RabbitProducer(object):

    def __init__(self, host, queue_dict):
        self.queue_dict = queue_dict
        self.channel = self.connect_host(host)

    def connect_host(self, host):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = self.connection.channel()
        channel.exchange_declare(exchange='direct', exchange_type='direct')
        for queue, ttl in self.queue_dict.items():
            channel.queue_declare(queue=queue, arguments={'x-message-ttl' : ttl})
            tolog('Declared queue {} with TTL {}'.format(queue, ttl))

        return channel

    def send(self, message, filename):
        self.channel.basic_publish(exchange='direct', routing_key='csv', body=message,
                                   properties=pika.BasicProperties(headers={'filename': filename}))

    def check_consumers(self, queue):
        p = Popen(['rabbitmqctl', 'list_consumers'], stdout=PIPE)
        return queue in str(p.communicate())

    def close_connection(self):
        self.connection.close()
