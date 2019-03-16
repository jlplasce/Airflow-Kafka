#!/usr/local/bin/python2.7
__author__ = 'JosePlascencia'

import subprocess
import datetime
import time
import logging.handlers
import multiprocessing
from ast import literal_eval as make_tuple

from kafka_worker.configuration import Configuration

configuration = Configuration()


def format_key_for_state(key):
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


class AirKafkaWorkers(multiprocessing.Process):
    """
    This Class will run in parallel by the multiprocess library and has tasks being
    queued by the KafkaWorker class above. This is also the class that actually executes
    the commands as a subprocess in the shell and will report task states back to
    Airflow.
    """

    def __init__(self, task_queue, result_queue, logger, airflow_loc):
        super(AirKafkaWorkers, self).__init__()
        self.task_queue = task_queue
        self.daemon = True
        self.result_queue = result_queue
        self.airflow_loc = airflow_loc
        self.state = None
        self.log = logger

    def run(self):
        """
        TODO
        :return:
        """
        while True:
            message = self.task_queue.get()
            if message is None:
                self.task_queue.task_done()
                break
            self.log.info("\n{} executing work on {}\n".format(self.name, message))
            self.execute_work(message)
            self.task_queue.task_done()
            time.sleep(1)

    def execute_work(self, message):
        """
        This method is called on by the multiprocessor and run in parallel.
        to execute the commands consumed by Kafka in the KafkaWorker class above.
        Will attempt to execute the commands and report their success to the result_queue
        for Airflow to update task states with.
        :return: An updated "result_queue" list which is used to update the state of tasks.
        """
        command = "exec bash -c '{0}'".format(message.value.replace('"', ''))
        formatted_message_key = format_key_for_state(message.key)
        self.log.debug("Command {} formatted key back to {}"
                       .format(command, formatted_message_key))
        try:
            subprocess.check_call(command, shell=True, close_fds=True)
            self.state = "success"
        except subprocess.CalledProcessError as e:
            self.state = "failed"
            self.log.error("Failed to execute task %s.", str(e))

        self.log.debug("Subprocess has declared Key - {} to State - {}"
                       .format(message.key, self.state))

        self.result_queue.put((formatted_message_key, self.state))
