from __future__ import print_function
__author__ = 'JosePlascencia'

import argparse
import multiprocessing

import kafka_worker
from kafka_worker.app import KafkaWorker
from kafka_worker.logger import get_logger
from kafka_worker.configuration import Configuration

configuration = Configuration()
logger = get_logger(
    logging_level=configuration.get_logging_level(),
    logs_output_file_path=configuration.get_logs_output_file_path(),
    logs_rotate_when=configuration.get_logs_rotate_when(),
    logs_rotate_backup_count=configuration.get_logs_rotate_backup_count()
)


def get_configs():
    kafka_broker_url = configuration.get_broker_url()
    kafka_topic = configuration.get_topic()
    kafka_results_topic = configuration.get_results_topic()
    kafka_group_id = configuration.get_group_id()
    airflow_parallelism = configuration.get_parallelism_max()
    airflow_loc = configuration.airflow_executable_path

    return kafka_broker_url, kafka_topic, kafka_results_topic, \
        kafka_group_id, airflow_parallelism, airflow_loc


def version(args):
    """
    Version of the Airflow Kafka Version
    """
    print("\nKafka Worker Version: {0}\n".format(str(kafka_worker.__version__)))


def init(args):
    """
    Adds [AirKafka Worker] seciton to airflow.cfg, this section is primarily
    adding info and settings for the "logger"
    :param args:
    :return:
    """
    configuration.add_default_kafka_worker_configs_to_airflow_configs()
    print \
        ("Finished Initializing Configurations to allow Kafka Worker to run.\n"
         "Please update the airflow.cfg with your desired configurations.")


def start(args):
    """
    Start the AirKafka Worker
    """
    kafka_broker_url, kafka_topic,\
        kafka_results_topic, kafka_group_id, airflow_parallelism, airflow_loc = get_configs()

    kafkaworker = KafkaWorker()
    kafkaworker.main(
        broker_url=kafka_broker_url,
        topic=kafka_topic,
        results_topic=kafka_results_topic,
        group_id=kafka_group_id,
        parallelism=int(airflow_parallelism),
        logger=logger,
        airflow_loc=airflow_loc
    )


def get_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')
    subparsers.required = True

    ht = "Prints out the version of the Kafka Worker"
    parser_logs = subparsers.add_parser('version', help=ht)
    parser_logs.set_defaults(func=version)

    ht = "Initialize Configurations to allow Kafka Worker to run"
    parser_logs = subparsers.add_parser('init', help=ht)
    parser_logs.set_defaults(func=init)

    ht = "Start the Kafka Worker"
    parser_logs = subparsers.add_parser('start', help=ht)
    parser_logs.set_defaults(func=start)

    return parser
