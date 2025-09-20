from enum import Enum


class Executor(Enum):
    IN_PROCESS = "in_process"
    HTTP = "http"
    CELERY = "celery"
