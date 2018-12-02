from datetime import datetime

from redis.utils import HIREDIS_AVAILABLE

from master_server import RabbitConsumer
from master_redis import RedisObj
from log import tolog
from config import *


# Signs
NIP = 0
DIP = 1


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    insert_redis(create_dict(body, 0), 0)
    insert_redis(create_dict(body, 1), 1)

    tolog('[Server] {} got message...'.format(redis_queue_name))


def insert_redis(ip_dict, ip_type):
    now = mod_checker()

    if ip_type == 0:
        for ip, count in ip_dict.items():
            ip_key = '{}_{}'.format(ip, now)
            redis_result = redis_nip.redis_conn.incrby(ip_key, count)
            if redis_result == count:
                redis_nip.redis_conn.expire(ip_key, redis_ttl)

    elif ip_type == 1:
        for ip, count in ip_dict.items():
            ip_key = '{}_{}'.format(ip, now)
            redis_result = redis_dip.redis_conn.incrby(ip_key, count)
            if redis_result == count:
                redis_dip.redis_conn.expire(ip_key, redis_ttl)


def create_dict(body, ip_type):
    counter_dict = {}
    index = get_ip_index(ip_type)

    for row in parse_body(body):
        temp_list = row.split(',')
        if len(temp_list) > 1:
            counter_dict[temp_list[index]] = counter_dict.setdefault(temp_list[index], 0) + 1

    return counter_dict


def mod_checker():
    now = datetime.now()
    if now.minute % 5 == 0:
        time_postfix = datetime.strftime(now, '%H%M')
    else:
        time_postfix = '{}{}'.format(datetime.strftime(now, '%H'), now.minute - (now.minute % 5))

    return time_postfix


def sort_dict(ip_dict):
    return sorted(ip_dict.items(), key=lambda x: x[1], reverse=True)


def parse_body(body):
    return [row for row in body.decode().split('\n')]


def get_ip_index(ip_type):
    return 4 if ip_type == 0 else 6


def get_data(channel):
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()


def hiredis_checker():
    if not HIREDIS_AVAILABLE:
        print('hiredis is not available. Better to install it.')
        tolog('hiredis is not available. Better to install it.', 'warn')


def main():
    hiredis_checker()
    postgres_server = RabbitConsumer(redis_queue_name)
    channel = postgres_server.get_channel()

    tolog('[Server] {} starting...'.format(redis_queue_name))
    channel.basic_consume(callback, queue=redis_queue_name)
    get_data(channel)
    postgres_server.close_connection()


if __name__ == '__main__':
    redis_nip = RedisObj(redis_host, redis_port, 0)
    redis_dip = RedisObj(redis_host, redis_port, 1)
    main()
