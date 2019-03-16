#!/usr/local/bin/python2.7
__author__ = 'JosePlascencia'

import multiprocessing
import time

from kafka import KafkaConsumer
from kafka import KafkaProducer

from kafka_worker.configuration import Configuration
from kafka_worker.worker import AirKafkaWorkers

configuration = Configuration()


def _get_consumer(host, topic, group_id):
    """
    :return: Kafka Consumer Object
    """
    consumer = KafkaConsumer(group_id=group_id,
                             enable_auto_commit=True,
                             auto_offset_reset='latest',
                             consumer_timeout_ms=10000,
                             bootstrap_servers=host
                             )

    consumer.subscribe(topic)
    return consumer


def _get_producer(host):
    """
    :return: Kafka Producer Object
    """
    producer = KafkaProducer(bootstrap_servers=str(host))
    return producer


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


class KafkaWorker(object):

    def __init__(self):
        super(KafkaWorker, self).__init__()
        self.results = multiprocessing.Queue()
        self.tasks = multiprocessing.JoinableQueue()
        self.broker_url = None
        self.topic = None
        self.results_topic = None
        self.group_id = None
        self.airflow_loc = None
        self.parallelism = None
        self.consumer = None
        self.producer = None
        self.message = None
        self.log = None
        self.heartbeat_trigger = False
        self.heartbeat_threshold_secs = 30
        self.kw_start = time.time()
        self.workers = []

    def main(self, broker_url, topic, results_topic, group_id, parallelism, logger, airflow_loc):
        """

        :param broker_url: Kafka Broker_URL
        :param topic: Kafka Topic used for task passing
        :param results_topic: Kafka Topic used for Result passing
        :param group_id: Kafka Group_ID used for task passing
        :param parallelism: Airflow parallelism defined in airflow.cfg
        :param logger: Airflow Kafka Worker logger
        :param airflow_loc: Location of "airflow" executable
        """
        # Start Message
        print("\nAirKafka Worker Starting Up!\n")

        self.broker_url = broker_url.replace('"', "")
        self.topic = topic.replace('"', "")
        self.results_topic = results_topic.replace('"', "")
        self.group_id = group_id.replace('"', "")
        self.airflow_loc = airflow_loc.replace('"', "")
        self.parallelism = parallelism
        self.log = logger

        # Start workers
        self.log.info("Starting up {} workers...\n".format(self.parallelism))
        self.start_workers()

        while 1:
            # Get consumer
            self.log.info("Getting Consumer connected to:{}\nTopic:{}\nGroup_id: {}"
                          .format(self.broker_url, self.topic, self.group_id))
            self.consumer = _get_consumer(self.broker_url, self.topic, self.group_id)
            self.log.info("Kafka Worker Created Consumer")

            # Get Producer
            self.log.info("Getting Producer connected to:{}"
                          .format(self.broker_url))
            self.producer = _get_producer(self.broker_url)
            self.log.info("Kafka Worker created producer to send results back")

            self.heartbeat_trigger = False
            # Open consumer thread to consume messages
            self.log.info("\n\nKafka Worker Waiting for Messages ...\n\n")
            while not self.heartbeat_trigger:
                for message in self.consumer:
                    self.log.info("Message Consumed - {}".format(message))
                    self.tasks.put(message)
                # Meant to execute once the consumer hasn't received messages
                # for 5 seconds, sleep and retry consuming
                self.log.info("Consumer didn't consume anything for 10 seconds...\nHeartbeating...\n")
                # Heartbeat - An attempt to reconnect the consumer when
                # it hangs. Sometimes this solves things, other times it
                # doesn't do anything ...
                if self.tasks.empty():
                    if (time.time()-self.kw_start) > self.heartbeat_threshold_secs:
                        self.log.debug("Heartbeat [boom]")
                        self.heartbeat()

    def heartbeat(self):
        """
        Runs every 10 seconds and used to refresh the consumer and gather
        results. Helps avoid any hangs and control the number of tasks that need to be
        updated. Might become problematic in large production settings if tasks are *always*
        running. That would mean the heartbeat would never really get a chance to run.
        :return:
        """
        self.consumer.close()
        time.sleep(1)
        self.log.info("Gathering Results...\n")
        self.send_session_results()
        self.log.debug("Resetting heartbeat timer\n")
        self.kw_start = time.time()
        self.heartbeat_trigger = True

    def send_session_results(self):
        """
        Sends results from the "result_ queue" back to another Kafka Topic to be received
        by the executor and used to update the task state
        """
        while not self.results.empty():
            key, state = self.results.get()
            if key is not None:
                self.log.info("Sending key {} with state {} back to Kafka topic {}"
                              .format(key, state, self.results_topic))
                formatted_key = _format_key_to_send(key)
                self.producer.send(self.results_topic, key=formatted_key, value=state)
                self.producer.flush()
        else:
            self.log.debug("\nResult_Queue Reported as empty when trying to gather results\n")

    def start_workers(self):
        """
        Start "self.parallelism"-many workers on "workers.py". These are the workers that
        actually execute the "airflow run..." command through shell.
        """
        self.workers = [
            AirKafkaWorkers(self.tasks, self.results, self.log, self.airflow_loc)
            for _ in range(self.parallelism/2)
        ]
        for w in self.workers:
            w.start()

    def ending(self):
        """
        Called by the "end" function of the base executor and just used to join tasks
        once the caller has finished and is just waiting synchronously. See the "end"
        function of the base_executor for more info.
        """
        for _ in self.workers:
            self.tasks.put(None)
        self.tasks.join()
