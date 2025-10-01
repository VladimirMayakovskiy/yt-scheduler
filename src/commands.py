from handle_exceptions import handle_exceptions
from dagref import DagMeta, DagRef, TaskRef
from rows_clients import DagRefClient, DagMetaClient, make_transient_lock_errors_retry_options
from errors import DagInitializationError
from yt_wrapper import with_yt_client
import yt.wrapper as yt

@with_yt_client
def try_add_dag(dag, yt_client: yt.YtClient) -> (bool, str, str):
    try:
        serialized_repr, payload_hash = dag.to_serialized_repr()
    except DagInitializationError as e:
        raise DagInitializationError(f"Failed to serialize DAG: {e}") from e

    found = DagRefClient.get(payload_hash=payload_hash)
    dag_id = found.dag_id if found else yt.common.generate_uuid()

    meta = DagMeta(dag_id=dag_id)

    if not found:
        dag_ref = DagRef(
            dag_id=dag_id,
            serialized_repr=serialized_repr,
            payload_hash=payload_hash,
        )

        task_refs = [
            TaskRef(
                task_id=task.task_id,
                dag_id=dag_id,
                serialized_repr=ser,
                payload_hash=phash,
            )
            for task in dag.tasks
            for ser, phash in [task.to_serialized_repr()]
        ]

        def _create_refs_meta_atomic():
            with yt_client.Transaction(type="tablet"):
                DagRef.upsert_rows(rows=dag_ref)
                TaskRef.upsert_rows(rows=task_refs)
                DagMeta.upsert_rows(rows=meta)

        _create_refs_meta_atomic = handle_exceptions(
            _create_refs_meta_atomic,
            default_retry_options=make_transient_lock_errors_retry_options(retries=3, raise_on_exhaust=True)
        )()
        return True, dag_id, meta.id
    else:
        DagMetaClient.upsert_rows(rows=meta)
        return False, dag_id, meta.id