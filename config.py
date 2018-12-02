#################################
# RMQ Global Configuration File #
#################################

# System Logging
log_path = '/var/log/rabbit.log'

# Client
host = '127.0.0.1'
csv_path = '/home/narbeh/Desktop/rabbitmq'
queue_dict = {'redis': 120000, 'postgres': 600000}
sleep_time = 5

# Master Server
rmp_host = '127.0.0.1'

# PostgreSQL Consumer
pg_queue_name = 'postgres'
csv_store_path = '/tmp/'

# Redis Consumer
redis_queue_name = 'redis'
redis_host = '192.168.0.157'
redis_port = '6379'
redis_ttl = 60
