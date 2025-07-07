from enum import Enum

class JobState(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    # RESTARTING = "restarting"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

class DagRunState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

class TaskRunState(str, Enum):
    SCHEDULED = "scheduled" # задача  запланирована для запуска. Шедулер решил что можно запускать но пока еще не передал задачу в Экзекьютор
    READY = "ready"
    QUEUED = "queued" # задача поставлена в очередь Экзекьютора. Ожидает фактичесвкого запуска на исполнение
    RUNNING = "running" # задача активно выполняется
    SUCCESS = "success" # задача успешно завершилась
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value