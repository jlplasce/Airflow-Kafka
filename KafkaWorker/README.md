# AirKafka Worker

## Installation

### Local Development

In case you want to do development work on the project

1. Clone the project from GitHub to your local machine

        git clone https://github.com/teamclairvoyant/Airflow-Kafka-Integration/

2. Run pip install

        cd {AIRKAFKA WORKER HOME}
        pip install -e .
        
3. You're done!

    * After, you will be able to run the project through the CLI Interface (See bellow for more details), be able to make any changes to the project you just brought down and have those changes be immediately reflected in the CLI

## CLI Interface

    usage: kafka_worker [-h]
                                         {init,version,start}
                                         ...
    
    positional arguments:
      {init,version,start}
                            sub-command help
        init                Creates a "[AirKafka Worker]" section in airflow.cfg used for the logger
        version             Prints out info and version of the AirKafka Worker
        start               Start the AirKafka Worker
    
    optional arguments:
      -h, --help            show this help message and exit


## Uninstall 

1. Run pip uninstall

        pip uninstall kafka_worker

2. If you ran the installation for development, follow these steps:

    a. Get the bin file location and use this value as the {BIN_CLI_FILE_PATH} placeholder

        which kafka_worker

    b. Remove the bin file

        rm {BIN_CLI_FILE_PATH}
