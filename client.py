from os import listdir
from time import sleep

from master_client import RabbitProducer
from log import tolog
from config import *


def read_file(file):
    with open(file, 'rb') as csv_file:
        return csv_file.read()


def find_csv_filenames(csv_path, suffix='.csv'):
    filenames = listdir(csv_path)
    return [filename for filename in filenames if filename.endswith(suffix)]


def send_data(rabclient):
    while True:
        if rabclient.check_consumers('postgres'):
            for file in find_csv_filenames(csv_path):
                rabclient.send(read_file(file), file)
                print('[x] Sent %r' % file)
                tolog('[Client] Message sent')
        else:
            print('Consumer is not running!')
            tolog('[Client] Consumer is not running!', 'warn')

        sleep(sleep_time)

    rabclient.close_connection()


if __name__ == '__main__':
    rabclient = RabbitProducer(host, queue_dict)
    send_data(rabclient)
