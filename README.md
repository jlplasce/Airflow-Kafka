# AirKafka

The AirKafka Worker is being created to work independently from the Kafka Executor in order to consume the messages that are coming into the Airflow Kafka Topic and executing the consumed commands in parallel and reporting the success back to Airflow.

Once the AirKafka Worker has executed the tasks and logged their success, it will push the key and state back to another Kafka Topic. This Kafka Topic in turn is monitored by the executors "sync" function which is used to udpate the task states.

Versions
============
  Airflow 1.9.0
  
  Kafka-Python 1.3.5
  
  Kafka Parcel - 3.0.0-1.3.0.0.p0.40
  
Deployment Steps
============

kafka_executor_plugin.py goes into {AIRFLOW HOME}/plugins directory and then the airflow.cfg file is updated as:

executor = kafka_executor_plugin.KafkaExecutor

AirKafka Worker deployment steps under the KafkaWorker folder.


Bug List/Dev Notes
============
  Kafka requires as many partitions as consumers you expect to connect to it. Thus, if a person wants 3 kafka_workers to be waiting on Airflow commands to execute, the topic needs to have at least 3 partitions or else you will have idle workers taking up resources.
    Still need to test creating multiple head "workers". In this case, it would need to be multiple "kafka_worker start" instances and make sure they can communicate properly.

  Needs to be tested more robustly to ensure production level environment does not break the executor.
  
  The "heartbeat" method of the worker might be problematic in larger productions as task joining and result gathering technically only occur after the consumer has not consumed new messages for 10 seconds. If a production has DAGS running all the time, this method may be very rarely reached.
  
  The "airflow_loc" was implemented because there was a local error where the "airflow" executable could not be found and was creating problems. This might not be necessary in the future, but it doesn't hurt to have and to path the "airflow run" command absolutely instead of relying on the executable being global
  
  For whatever reason, once "airflow run" commands are executed in command line/shell, the metastore seems to be automatically updated of the tasks state. In fact, if the "airflow run" command is stored as "command" and the code "subprocess.check_call(command)" is run - it will always return a success. For example, if a new DAG is created with one task and that one task is a BashOperator with "exit 127", that task can be queued and executed manually via command line and will give "return code 1" - but if it is run via Python "subprocess" the subprocess will return "exit code 0" and claim it was successful. *not sure if this is a local problem, but testing has proven this continues to be the case with different executors, fresh installs, different airflow version, etc*. So, looking at the logs, any task that is executed will claim that it was successful, but the metastore seems to be getting the right state - again, not sure how and could be local.
  
  


