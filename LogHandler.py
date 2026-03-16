import logging

from utils import Utils


class LogHandler(logging.Handler):
    _logBuffer = []

    def __init__(self, level=0):
        super().__init__(level)
        LogHandler._logBuffer = []

    def emit(self, record):
        msg = self.format(record)
        # replace multiple line and tab to single space
        msg = Utils.multiple_to_single_space(msg)
        print(msg)
        LogHandler._logBuffer.append(msg)

    @classmethod
    def get_log_buffer(cls):
        return cls._logBuffer
