import yaml
import yt.wrapper as yt
import yt.type_info.typing as ti

from scheduler import Scheduler

CONFIG_FILE_PATH = "configs/config.cfg"
GRAPH_TABLE = "//home/flow_runner/graph_state"
TASK_TABLE = "//home/flow_runner/task_state"
YT_PROXY = 'localhost:8000'


def init(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy) if args.yt_proxy is not None else yt.YtClient(proxy=YT_PROXY)#configuration.conf.get('yt', 'yt_proxy'))

    if not yt_client.exists(GRAPH_TABLE):
        schema = yt.schema.TableSchema().add_column('pipeline_id', ti.String) \
            .add_column('spec_path', ti.String) \
            .add_column('work_dir', ti.String) \
            .add_column('status', ti.String) \
            .add_column('start_time', ti.Timestamp) \
            .add_column('end_time', ti.Timestamp)
        yt_client.create("table", GRAPH_TABLE, attributes={'schema': schema}, recursive=True)
        print(f"Table created: {GRAPH_TABLE}")

    if not yt_client.exists(TASK_TABLE):
        schema = yt.schema.TableSchema().add_column('task_id', ti.String) \
            .add_column('pipeline_id', ti.String) \
            .add_column('status', ti.String) \
            .add_column('name', ti.String) \
            .add_column('start_time', ti.Timestamp) \
            .add_column('end_time', ti.Timestamp)
        yt_client.create("table", TASK_TABLE, attributes={'schema': schema}, recursive=True)
        print(f"Table created: {TASK_TABLE}")


def run(args):
    with open(args.spec, 'r') as file:
        spec = yaml.safe_load(file)

    scheduler = Scheduler()
    pipeline_id = scheduler.add_pipeline(args.spec, args.work_dir)

    scheduler.run_pipeline(pipeline_id)
