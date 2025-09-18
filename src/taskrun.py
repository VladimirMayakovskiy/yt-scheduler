from __future__ import annotations

import typing
from dataclasses import field, asdict, KW_ONLY
from datetime import datetime, timezone as tz
from typing import Optional, ClassVar, Any

from rows_helpers import make_formatted_select, process_filter_value, format_select_columns, copy_fields
from base_row import YtRow, TablePath
if typing.TYPE_CHECKING:
    from scheduler import ShardingOptions
    from dagrun import DagRun
from state import TaskRunState
from logging_mixin import LoggingMixin
from task import Task
from yt_wrapper import with_yt_client

import yt.wrapper as yt

@yt.yt_dataclass
class TaskRunRow(YtRow):
    table_path:  ClassVar[str] = TablePath("task_run")
    key_columns: ClassVar[str] = ["run_id"]
    alias: ClassVar[str] = "taskrun"

    run_id: str = field(default_factory=lambda: yt.common.generate_uuid())

    _: KW_ONLY

    task_id: str
    dag_id: str
    dagrun_id: str
    state: str

    operation_id: Optional[str] = None

    scheduled_at: Optional[str] = None
    queued_at: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class TaskRun(TaskRunRow, LoggingMixin):
    row_type: ClassVar[type[TaskRunRow]] = TaskRunRow
    state_type: ClassVar[type[TaskRunState]] = TaskRunState

    def __init__(
        self,
        row: TaskRun.row_type | None = None,
        *,
        run_id: str | None = None,
        task: Task | None = None,
        dagrun_id: str | None = None,
        state: TaskRun.state_type | None = None,
        operation_id: str | None = None,
        scheduled_at: str | None = None,
        queued_at: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        if row is not None:
            super().__init__(**asdict(row))
        else:
            def _to_iso(value: datetime | str | None) -> str | None:
                if isinstance(value, datetime):
                    return value.isoformat()
                return value

            super().__init__(
                task_id=task.task_id,
                dag_id=task.dag_id,
                dagrun_id=dagrun_id,
                state=state,
                operation_id=operation_id,
                scheduled_at=_to_iso(scheduled_at),
                queued_at=_to_iso(queued_at),
                start_date=_to_iso(start_date),
                end_date=_to_iso(end_date),
            )

            if run_id is not None:
                self.run_id = run_id

    @classmethod
    def fetch_rows(
        cls,
        run_id: str | list[str] | tuple[str] = None,
        task_id: str | list[str] | tuple[str] = None,
        dag_id: str | list[str] | tuple[str] = None,
        dagrun_id: str | list[str] | tuple[str] = None,
        state: TaskRun.state_type | list[TaskRun.state_type] | tuple[TaskRun.state_type] = None,
        operation_id: str | list[str] | tuple[str] = None,
        limit: int = None,
        shard_key: str = None,
        shard: "ShardingOptions" | None = None,
    ) -> list[TaskRun]:
        rows = make_formatted_select(
            cls=cls,
            run_id=run_id,
            task_id=task_id,
            dag_id=dag_id,
            dagrun_id=dagrun_id,
            state=state,
            operation_id=operation_id,
            limit=limit,
            shard_key=shard_key,
            shard=shard,
        )
        return [cls(cls.row_type(**row)) for row in rows]

    @classmethod
    def get(
        cls,
        run_id: str = None,
        task_id: str = None,
        dag_id: str = None,
        dagrun_id: str = None,
        operation_id: str = None
    ) -> "TaskRun" | None:
        return super(TaskRunRow, cls).get(
            run_id=run_id,
            task_id=task_id,
            dag_id=dag_id,
            dagrun_id=dagrun_id,
            operation_id=operation_id
        )

    @classmethod
    @with_yt_client
    def get_executable_task_runs_to_queue(cls, shard: "ShardingOptions" | None = None) -> list["TaskRun"]:
        return cls.fetch_rows(state=cls.state_type.QUEUED,
                              shard_key=cls.key_columns[0],
                              shard=shard)

    @classmethod
    @with_yt_client
    def get_running_task_runs_to_poll(cls, shard: "ShardingOptions" | None = None) -> list["TaskRun"]:
        return cls.fetch_rows(state=cls.state_type.RUNNING,
                              shard_key=cls.key_columns[0],
                              shard=shard)

    @staticmethod
    def update_row(
        row: str | TaskRun.row_type | TaskRun,
        *,
        state: TaskRun.state_type | None = None,
        operation_id: str | None = None,
        required_task_id: str | list[str] | tuple[str] | None = None,
        required_dag_id: str | list[str] | tuple[str] | None = None,
        required_dagrun_id: str | list[str] | tuple[str] | None = None,
        required_state: TaskRun.state_type | list[TaskRun.state_type] | tuple[TaskRun.state_type] | None = None,
        required_operation_id: str | list[str] | tuple[str] | None = None,
    ) -> tuple[str | TaskRun.row_type | TaskRun, dict[str, Any], dict[str, Any]]:
        from rows_helpers import set_param
        params: dict[str, Any] = {}
        set_param(params, "state", state)
        set_param(params, "operation_id", operation_id)

        requires: dict[str, Any] = {}
        set_param(requires, "task_id", required_task_id)
        set_param(requires, "dag_id", required_dag_id)
        set_param(requires, "dagrun_id", required_dagrun_id)
        set_param(requires, "state", required_state)
        set_param(requires, "operation_id", required_operation_id)
        return row, params, requires

    @classmethod
    @with_yt_client
    def set_state(
        cls,
        rows: list[ tuple[str | TaskRun.row_type | TaskRun, dict[str, Any], dict[str, Any]] ]
              |     tuple[str | TaskRun.row_type | TaskRun, dict[str, Any], dict[str, Any]],
    ) -> list[TaskRun]:
        if not isinstance(rows, list):
            rows = [rows]

        fetchable_rids = set() # rows_with_required or isinstance(row, str)
        for r, _, reqs in rows:
            if isinstance(r, str):
                fetchable_rids.add(r)
            else:
                if reqs:
                    fetchable_rids.add(r.run_id)

        def _fetch_rows(rids) -> dict[str, cls]:
            return {tr.run_id: tr for tr in cls.fetch_rows(run_id=list(rids))}
        def _build_row(row: cls, state: cls.state_type | None = None, operation_id: str | None = None) -> cls.row_type:
            base = TaskRun.row_type(**asdict(row))
            now = datetime.now(tz.utc).isoformat()
            if state is not None and base.state != state:
                if state in TaskRun.state_type.unfinished_states:
                    base.scheduled_at = base.scheduled_at or now
                    if state == TaskRun.state_type.SCHEDULED:
                        base.queued_at = None
                        base.start_date = None
                    else:
                        base.queued_at = base.queued_at or now
                        if state == TaskRun.state_type.RUNNING:
                            base.start_date = now
                        else:
                            base.start_date = None
                    base.end_date = None
                elif base.state in TaskRun.state_type.unfinished_states:
                    base.end_date = now
                base.state = state

            if operation_id is not None:
                base.operation_id = operation_id
            return base
        def _is_matched(row, requires):
            for k, v in requires.items():
                if v is None:
                    continue
                if isinstance(v, (list, tuple, set)):
                    if getattr(row, k, None) not in v:
                        return False
                else :
                    if getattr(row, k, None) != v:
                        return False
            return True

        def _set_state(fetched=None):
            for row, params, requires in rows:
                try:
                    if isinstance(row, str):
                        tr = fetched[row]
                    else:
                        if requires:
                            tr = fetched[row.run_id]
                        elif isinstance(row, cls):
                            tr = row
                        elif isinstance(row, cls.row_type):
                            tr = TaskRun(row=row)
                        else:
                            raise TypeError
                except Exception as e:
                    cls.logger.warning("Failed to get row for task run id=%s: %s",
                                       row if isinstance(row, str) else row.run_id, e)
                    continue

                if not isinstance(row, str) and row != tr:
                    copy_fields(row, tr, cls=TaskRun)

                if requires and not _is_matched(tr, requires):
                    continue

                row_update = _build_row(tr, **params)
                rows_batch.append(row_update)
                if isinstance(row, str):
                    trs.append(tr)
                else:
                    trs.append(row)

            if rows_batch:
                try:
                    cls.upsert_rows(rows=rows_batch)
                except Exception as e:
                    cls.logger.exception("Failed to apply batch updates: %s", e)
                    raise

        trs = []
        rows_batch = []
        if fetchable_rids:
            try:
                with yt.Transaction(type="tablet"):
                    fetched_rows = _fetch_rows(fetchable_rids)
                    _set_state(fetched_rows)
            except Exception as exception:
                cls.logger.exception("Transaction failed: %s", exception)
                raise
        else:
            _set_state({})


        for r, r_update in zip(trs, rows_batch):
            if r != r_update:
                copy_fields(r, r_update, cls=TaskRun)
        return trs

    @classmethod
    @with_yt_client
    def get_orphaned_task_runs(cls, shard: "ShardingOptions", yt_client: yt.YtClient) -> list["TaskRun"]:
        from dagrun import DagRun
        _, dangling_states = process_filter_value([TaskRun.state_type.QUEUED, TaskRun.state_type.RUNNING])
        num_virtual_shards, num_shards, shard_index = shard["num_virtual_shards"], shard["num_shards"], shard["shard_index"]
        try:
            rows = list(yt_client.select_rows(
                f"""
                {format_select_columns(cls)}
                from [{cls.table_path}] as {cls.alias}
                left join [{DagRun.table_path}] as {DagRun.alias} 
                on {cls.alias}.dagrun_id = {DagRun.alias}.run_id
                where {cls.alias}.state in {dangling_states} 
                    and {DagRun.alias}.state = '{DagRun.state_type.FAILED}'
                    and (farm_hash(run_id) % {num_virtual_shards}) % {num_shards} = {shard_index}
                """,
                allow_join_without_index=True
            ))
            return [cls(cls.row_type(**row)) for row in rows]
        except Exception as e:
            cls.logger.exception("Failed to select orphan task run rows for %s: %s", cls.__name__, e)
            raise