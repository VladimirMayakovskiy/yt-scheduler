from enum import Enum

class JobState(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

class DagRunState(str, Enum):
    QUEUED = "queued" # шедулер начал обрабатывать dagrun: хотя бы одна задача dagrun SCHEDULED, но ни одной в READY ????????
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

class TaskRunState(str, Enum):
    SCHEDULED = "scheduled"
    READY = "ready" # задача  запланирована для запуска. Шедулер решил что можно запускать но пока еще не передал задачу в Экзекьютор
    QUEUED = "queued" # задача поставлена в очередь Экзекьютора. Ожидает фактичесвкого запуска на исполнение
    RUNNING = "running" # задача активно выполняется
    SUCCESS = "success" # задача успешно завершилась
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value