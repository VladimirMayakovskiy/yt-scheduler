from enum import Enum

from common import classproperty

class JobState(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

class DagRunState(str, Enum):
    SCHEDULED = "scheduled"
    QUEUED = "queued" # шедулер начал обрабатывать dagrun: хотя бы одна задача dagrun SCHEDULED, но ни одной в READY
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

class TaskRunState(str, Enum):
    SCHEDULED = "scheduled"
    QUEUED = "queued" # задача запланирована для запуска. Шедулер решил что можно запускать
    RUNNING = "running" # задача активно выполняется
    SUCCESS = "success" # задача успешно завершилась
    FAILED = "failed"
    SKIPPED = "skipped"

    @classproperty
    def unfinished_states(self) -> list["TaskRunState"]:
        return [TaskRunState.SCHEDULED, TaskRunState.QUEUED, TaskRunState.RUNNING]
    @classproperty
    def finished_states(self) -> list["TaskRunState"]:
        return [TaskRunState.FAILED, TaskRunState.SUCCESS, TaskRunState.SKIPPED]

    def __str__(self) -> str:
        return self.value