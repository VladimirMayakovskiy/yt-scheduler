from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional
import yt.wrapper as yt

from executor import Executor
from state import JobState


@yt.yt_dataclass
class Job: #TODO возможно стоит переименовать в SchedulerJob, прокинуть логгирование, регистрацию через фабрику executor
    # Для отслеживания и логгирования состояния задач, не являющихся задачами графа, то есть Scheduler
    id: str

    start_date: datetime
    end_date: Optional[datetime]

    state: str#JobState

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.start_date = None
        self.end_date = None
        self.state = None

        self._executor = Executor(
            job_id=self.id
        )

    def prepare_for_execution(self, yt_client: yt.YtClient):
        # помечает задачу шедулера как RUNNING
        self.state = JobState.RUNNING
        self.start_date = datetime.utcnow()
        # with yt_client.Transaction():
        #     yt_client.write_table_structured(
        #         yt.TablePath("//home/job_state", append=True),
        #         Job,
        #         [
        #             self,
        #         ],
        #     )

    def complete_execution(self, yt_client: yt.YtClient):
        self.end_date = datetime.utcnow()
        # TODO check state
        # with yt_client.Transaction():
        #     yt_client.write_table_structured(
        #         yt.TablePath("//home/job_state", append=True),
        #         Job,
        #         [
        #             self,
        #         ],
        #     )

    @property
    def executor(self) -> Executor:
        return self._executor
