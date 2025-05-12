from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional
import yt.wrapper as yt

from executor import Executor
from state import JobState


@yt.yt_dataclass
class Job:
    id: str

    start_date: datetime
    end_date: Optional[datetime]

    state: str#JobState

    def __init__(self, executor: Executor | None = None):
        self._executor = executor
        self.id = str(uuid.uuid4())

    def prepare_for_execution(self, yt_client: yt.YtClient):
        self.state = JobState.RUNNING
        self.start_date = datetime.utcnow()
        self.end_date = None
        with yt_client.Transaction():
            yt_client.write_table_structured(
                yt.TablePath("//home/job_state", append=True),
                Job,
                [
                    self,
                ],
            )

    def complete_execution(self, yt_client: yt.YtClient):
        self.end_date = datetime.utcnow()
        with yt_client.Transaction():
            yt_client.write_table_structured(
                yt.TablePath("//home/job_state", append=True),
                Job,
                [
                    self,
                ],
            )

    @property
    def executor(self) -> Executor:
        if self._executor:
            return self._executor
        return Executor()
