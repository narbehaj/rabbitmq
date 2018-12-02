from time import sleep

import pika

from log import tolog
from config import *


class RabbitConsumer(object):

    def __init__(self, queue):
        self.queue = queue
        self.channel = self.connect_host(rmp_host)

    def connect_host(self, host):
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
                channel = self.connection.channel()
                channel.queue_declare(exclusive=True)
                channel.exchange_declare(exchange='direct', exchange_type='direct')
                channel.queue_bind(exchange='direct', queue=self.queue, routing_key='csv')
                break
            except pika.exceptions.ChannelClosed:
                print('Queue {} not exists. Waiting...'.format(self.queue))
                tolog('Queue {} not exists. Waiting...'.format(self.queue), 'warn')
                self.close_connection()
                sleep(10)  # Sleep and wait for the client

        return channel

    def get_channel(self):
        return self.channel

    def remove_queue(self, queue_name):
        self.channel.queue_delete(queue=queue_name)

    def close_connection(self):
        self.connection.close()
