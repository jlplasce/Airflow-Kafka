#!/usr/local/bin/python2.7
from __future__ import print_function
__author__ = 'JosePlascencia'
import os
import ConfigParser
import sys
import logging
import subprocess


def get_airflow_home_dir():
    return os.environ['AIRFLOW_HOME'] if "AIRFLOW_HOME" in os.environ else os.path.expanduser("~/airflow")


def get_airflow_loc():
    try:
        airflow_loc_command = "exec bash -c 'which airflow'"
        airflow_loc = subprocess \
            .check_output(airflow_loc_command, shell=True, close_fds=True) \
            .replace('airflow', '') \
            .replace('\n', '')
    except Exception:
        print("Cannot find path for Airflow Executable!!!")
        sys.exit(1)

    return airflow_loc


DEFAULT_AIRFLOW_HOME_DIR = get_airflow_home_dir()
DEFAULT_AIRFLOW_EXECUTABLE_PATH = get_airflow_loc()
DEFAULT_BROKER_URL = "localhost:9092"
DEFAULT_TOPIC = "AirflowKafkaTopic"
DEFAULT_GROUP_ID = "AirflowGroupId"
DEFAULT_PARALLELISM_MAX = 16
DEFAULT_LOGGING_LEVEL = "INFO"
DEFAULT_LOGS_ROTATE_WHEN = "midnight"
DEFAULT_LOGS_ROTATE_BACKUP_COUNT = 7

DEFAULT_AIRKAFKA_WORKER_CONFIGS = """
[AirKafka_worker]
# Logging Level. Choices include:
# NOTSET, DEBUG, INFO, WARN, ERROR, CRITICAL
logging_level = """ + str(DEFAULT_LOGGING_LEVEL) + """
# Log Directory Location
logging_dir =  """ + str(DEFAULT_AIRFLOW_HOME_DIR) + """/logs/AirKafka_worker/
# Log File Name
logging_file_name = kafka_worker.log
# When the logs should be rotated.
# Documentation: https://docs.python.org/2/library/logging.handlers.html#logging.handlers.TimedRotatingFileHandler
logs_rotate_when = """ + str(DEFAULT_LOGS_ROTATE_WHEN) + """
# How many times the logs should be rotate before you clear out the old ones
logs_rotate_backup_count = """ + str(DEFAULT_LOGS_ROTATE_BACKUP_COUNT) + """

"""


class Configuration(object):
    """
    TODO
    """
    def __init__(self, airflow_home_dir=None, airflow_config_file_path=None, airflow_executable_path=None):
        if airflow_home_dir is None:
            airflow_home_dir = DEFAULT_AIRFLOW_HOME_DIR

        if airflow_config_file_path is None:
            airflow_config_file_path = airflow_home_dir + "/airflow.cfg"

        if airflow_executable_path is None:
            airflow_executable_path = DEFAULT_AIRFLOW_EXECUTABLE_PATH

        self.airflow_home_dir = airflow_home_dir
        self.airflow_config_file_path = airflow_config_file_path
        self.airflow_executable_path = airflow_executable_path

        if not os.path.isfile(airflow_config_file_path):
            print("Cannot find Airflow Configuration file at '" + str(airflow_config_file_path) + "'!!!")
            sys.exit(1)

        self.conf = ConfigParser.RawConfigParser()
        self.conf.read(airflow_config_file_path)

    def get_parallelism_max(self):
        return self.get_core_configs("parallelism", DEFAULT_PARALLELISM_MAX)

    def get_broker_url(self):
        return self.get_kafka_configs("broker_url", DEFAULT_BROKER_URL)

    def get_topic(self):
        return self.get_kafka_configs("topic", DEFAULT_TOPIC)

    def get_results_topic(self):
        return self.get_kafka_configs("results_topic", DEFAULT_TOPIC)

    def get_group_id(self):
        return self.get_kafka_configs("group_id", DEFAULT_GROUP_ID)

    def get_airflow_home_dir(self):
        return self.airflow_home_dir

    def get_airflow_executable_path(self):
        return self.airflow_executable_path

    def get_airflow_config_file_path(self):
        return self.airflow_config_file_path

    def get_config(self, section, option, default=None):
        try:
            config_value = self.conf.get(section, option)
            return config_value if config_value is not None else default
        except:
            pass
        return default

    def get_kafka_configs(self, option, default=None):
        return self.get_config("kafka", option, default)

    def get_airkafka_worker_configs(self, option, default=None):
        return self.get_config("AirKafka_worker", option, default)

    def get_core_configs(self, option, default=None):
        return self.get_config("core", option, default)

    def get_logging_level(self):
        return logging.getLevelName(self.get_airkafka_worker_configs("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL))

    def get_logs_output_file_path(self):
        logging_dir = self.get_airkafka_worker_configs("LOGGING_DIR")
        logging_file_name = self.get_airkafka_worker_configs("LOGGING_FILE_NAME")
        return logging_dir + logging_file_name if logging_dir is not None and logging_file_name is not None else None

    def get_logs_rotate_when(self):
        return self.get_airkafka_worker_configs("LOGS_ROTATE_WHEN", DEFAULT_LOGS_ROTATE_WHEN)

    def get_logs_rotate_backup_count(self):
        return int(self.get_airkafka_worker_configs("LOGS_ROTATE_BACKUP_COUNT", DEFAULT_LOGS_ROTATE_BACKUP_COUNT))

    def add_default_kafka_worker_configs_to_airflow_configs(self):
        with open(self.airflow_config_file_path, 'r') as airflow_config_file:
            if "[AirKafka_worker]" not in airflow_config_file.read():
                print \
                    ("Adding Kafka Worker configs to Airflow config file...\n")
                with open(self.airflow_config_file_path, "a") as airflow_config_file_to_append:
                    airflow_config_file_to_append.write(DEFAULT_AIRKAFKA_WORKER_CONFIGS)
                    print \
                        ("Finished adding AirKafka Worker configs to Airflow config file.\n\n")
            else:
                print \
                    ("[AirKafka_worker] section already exists. Skipping adding AirKafka Worker configs.\n\n")
