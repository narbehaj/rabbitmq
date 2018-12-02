from pika.exceptions import ConnectionClosed

from log import tolog
from master_server import RabbitConsumer
from config import *


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    file_name = properties.headers['filename']
    with open('{}/{}'.format(csv_store_path, file_name), 'wb') as write_csv:
        write_csv.write(body)

    tolog('[Server] {} {} got message...'.format(pg_queue_name, file_name))


def get_data(channel):
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    except ConnectionClosed:
        print('[Server] Crashed. Reconnecting...')
        tolog('[Server] Crashed. Reconnecting...', 'warn')
        main()


def main():
    postgres_server = RabbitConsumer(pg_queue_name)
    channel = postgres_server.get_channel()

    tolog('[Server] {} starting...'.format(pg_queue_name))
    channel.basic_consume(callback, queue=pg_queue_name)
    get_data(channel)
    postgres_server.close_connection()


if __name__ == '__main__':
    main()
