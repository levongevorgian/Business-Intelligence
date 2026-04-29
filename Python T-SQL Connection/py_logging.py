# pipeline_dimensional_data/py_logging.py

import logging
import os
from logging import Logger, LoggerAdapter

class ExecutionIDAdapter(LoggerAdapter):
    """
    LoggerAdapter to inject execution_id into log records.
    """
    def __init__(self, logger: Logger, execution_id: str):
        super().__init__(logger, {'execution_id': execution_id})

    def process(self, msg, kwargs):
        """
        Injects the execution_id into the log record's extra dictionary.
        """
        extra = kwargs.get('extra', {})
        # Update the extra dict with execution_id
        extra['execution_id'] = self.extra['execution_id']
        kwargs['extra'] = extra
        return msg, kwargs

def setup_logger(execution_id: str) -> LoggerAdapter:
    """
    Sets up and returns a LoggerAdapter configured to include the execution_id in each log message.

    :param execution_id: The unique identifier for the current execution instance.
    :return: Configured LoggerAdapter object.
    """
    logger = logging.getLogger('dimensional_data_flow_logger')
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent log messages from being propagated to the root logger

    # Ensure the logs directory exists
    log_directory = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_directory, exist_ok=True)

    log_file_path = os.path.join(log_directory, 'logs_dimensional_data_pipeline.txt')

    # Create a file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)

    # Define the log format including the execution_id
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(execution_id)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    # Add the following code to create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return ExecutionIDAdapter(logger, execution_id)
