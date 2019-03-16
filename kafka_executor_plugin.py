#!/usr/local/bin/python2.7
__author__ = 'JosePlascencia'

"""
For more about Airflow, visit:
`https://github.com/apache/incubator-airflow`
Any internal Airflow changes that were made to accommodate this Executor can be found at:
`https://github.com/teamclairvoyant/Airflow-Kafka-Integration`
"""

import json
from ast import literal_eval as make_tuple
import datetime

from airflow.executors.base_executor import BaseExecutor
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow import configuration

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition

from kafka_worker.app import KafkaWorker


class KafkaExecutor(BaseExecutor, LoggingMixin):
    """
    KafkaExecutor executes tasks in parallel using the multiprocessing Python Library
    and queues to parallelize the execution of tasks. Here, the Key/Command come from
    the base_executor into the "execute_async" and are sent to the Kafka Topic for the
    AirKafka Worker to pick up and execute. The "sync" function connects to a separate
    Kafka Topic for the results and updates the tasks instance state.

    This is implemented as a plugin.
    """

    @staticmethod
    def _format_key_to_send(key):
        """
        Want to send the command along with the key, however, the key
        must be byte encoded, so the Tuple is converted to a string.
        Additionally, since the Key contains a "Datetime.Datetime" object
        it is converted to a string first in order to avoid errors
        :param key: Tuple that must be encoded to send to Kafka
        :type key: Tuple
        :return: Encoded Key as a String
        """
        format_tuple = tuple((key[0], key[1], key[2].__str__()))
        formatted_key = format_tuple.__str__().encode('utf-8')

        return formatted_key

    @staticmethod
    def _format_key_for_state(key):
        """
        Although we needed the key in byte form to send to Kafka,
        the Base Executor and rest of Airflow requires the Key to
        be a tuple. This just re-converts the encoded string back
        to a Tuple.
        Note: the Datetime.Datetime object was encoded into a
        string itself, so it needs to be re-created to a
        Datetime.Datetime object again
        :param key: Encoded String retrieved from Kafka
        :return: Key back in Tuple form
        """
        tuple_key = make_tuple(key)
        try:
            formatted_datetime_object = datetime.datetime.strptime(tuple_key[2], "%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            formatted_datetime_object = datetime.datetime.strptime(tuple_key[2], "%Y-%m-%d %H:%M:%S")
        formatted_key = tuple((tuple_key[0], tuple_key[1], formatted_datetime_object))

        return formatted_key

    @staticmethod
    def _get_consumer(host, topic):
        consumer = KafkaConsumer(enable_auto_commit=True,
                                 auto_offset_reset='latest',
                                 consumer_timeout_ms=10000,
                                 bootstrap_servers=host
                                 )
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        return consumer

    @property
    def producer(self):
        """
        :return: Kafka Producer Object
        """
        return self._producer

    @producer.setter
    def producer(self, value):
        self._producer = value

    @property
    def broker_url(self):
        """
        :return: Kafka Broker_Url
        """
        self._broker_url = configuration.get("kafka", "broker_url").replace('"', "")
        return self._broker_url

    @property
    def group_id(self):
        """
        :return: Kafka Group Id
        """
        self._group_id = configuration.get("kafka", "group_id").replace('"', "")
        return self._group_id

    @property
    def topic(self):
        """
        :return: Kafka Topic
        """
        self._topic = configuration.get("kafka", "topic").replace('"', "")
        return self._topic

    @property
    def results_topic(self):
        """
        :return: Kafka Topic
        """
        self._results_topic = configuration.get("kafka", "results_topic").replace('"', "")
        return self._results_topic

    @property
    def consumer(self):
        return self._consumer

    @consumer.setter
    def consumer(self, value):
        self._consumer = value

    def __init__(self):
        super(KafkaExecutor, self).__init__()
        self._topic = None
        self._group_id = None
        self._producer = None
        self._broker_url = None
        self._results_topic = None
        self._consumer = None
        self.kw = KafkaWorker()

    def start(self):
        """
        Gets the Kafka Producer Object, Kafka Topic, Kafka Broker_url,
        Kafka Group_Id, and Airflow Executable Location to start the
        Kafka Worker in the background
        """
        # Don't want to accidentally create multiple producers, so it is manually set
        # in the start method which is only run once
        self.producer = KafkaProducer(bootstrap_servers=str(self.broker_url))
        self.consumer = self._get_consumer(self.broker_url, self.results_topic)

    def execute_async(self, key, command, queue=None):
        """
        Will receive tasks from the base_executor and send them to Kafka Topic.
        This Kafka Topic is monitored by the AirKafka Worker and will execute the
        actual work.

        :param queue: None
        :param key: unique key of DAG using it's task ID, DAG name, and time of execution
        :type key: dict
        :param command: the actual "airflow run ..." command that would be executed in command line
        :type command: str
        """
        # Encode command and key to send to Kafka
        command = json.dumps(command).encode('utf-8')
        formatted_key = self._format_key_to_send(key)
        # Send command and key to Kafka
        try:
            self.producer.send(self.topic, key=formatted_key, value=command)
            self.producer.flush()
            self.log.info("Command {} sent to Kafka Topic {}".format(command, self.topic))
        except Exception as e:
            self.log.error("Could not send command to Kafka!")
            self.log.exception(e)

    def sync(self):
        """
        Used to update task state.
        Consumes from the results Kafka Topic for updates
        """
        for i in range(self.parallelism):
            try:
                message = self.consumer.next()
            except Exception:
                break
            if message.key is not None:
                self.log.debug("Updating Key-{}\tto State-{}".format(message.key, message.value))
                # Format from string back to tuple
                formatted_key = self._format_key_for_state(message.key)
                self.change_state(formatted_key, message.value)

    def end(self):
        """
        Joins task queue and waits for them to finish and
        also adds "None" to the task queue which will close workers
        (Note the "if _ is None" checks above which would respond
        to this.
        """
        self.kw.ending()
# End of Kafka Executor Class


class KafkaExecutorPlugin(AirflowPlugin):
    name = "kafka_executor_plugin"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = [KafkaExecutor]
    admin_views = []
    menu_links = []
