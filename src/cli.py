from __future__ import annotations

import argparse
from typing import Optional, Callable
import commands

def add_ypath_argument(parser: argparse.ArgumentParser, name: str, help: str = "path in Cypress", **kwargs):
    description = "See also: https://ytsaurus.tech/docs/en/user-guide/storage/ypath" # todo
    help_text = help
    if "required" in kwargs and kwargs["required"]:
        help_text = (help_text or "") + " (required)"
    parser.add_argument(name, help=help_text, **kwargs)

def add_common_global_args(parser: argparse.ArgumentParser):
    """Добавляет общие для всех команд аргументы (например --yt-proxy)."""
    parser.add_argument("--yt-proxy", dest="proxy", required=False,
                        help="specify proxy for yt client, by default YT_PROXY from environment")
    # verbose

def add_subparser(subparsers: argparse._SubParsersAction):
    def add_parser(name: str, func: Optional[Callable] = None, help: Optional[str] = None, *args, **kwargs) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(name, *args, help=help, description=help, **kwargs)
        if func is not None:
            parser.set_defaults(func=func)
        return parser
    return add_parser

def add_group(subparsers: argparse._SubParsersAction, group_name: str, help: Optional[str] = None):
    group_parser = subparsers.add_parser(group_name, help=help, description=help)
    group_subparsers = group_parser.add_subparsers(required=True)
    return add_subparser(group_subparsers)

def add_scheduler_parser(subparsers: argparse._SubParsersAction):
    add_parser = add_group(subparsers, "scheduler", help="scheduler commands")
    add_parser("init", func=commands.prepare_tables, help='Initialize scheduler tables')
    add_parser("run", func=commands.run_scheduler, help='Run scheduler loop')

def add_dags_parser(subparsers: argparse._SubParsersAction):
    add_parser = add_group(subparsers, "dags", help="dag management commands")

    parser = add_parser("add", func=commands.add_dag, help='Add dag')
    add_ypath_argument(parser, "--spec", dest="spec", help="path to workflow specification", required=True)
    parser.add_argument("--work-dir", dest="work_dir", help="working directory for the pipeline")

def _prepare_parser() -> argparse.ArgumentParser:
    global_parser = argparse.ArgumentParser(add_help=False)

    add_common_global_args(global_parser)

    parser = argparse.ArgumentParser(prog="yt-scheduler", description="CLI for DAG scheduler", parents=[global_parser])

    subparsers = parser.add_subparsers(metavar="command")

    # scheduler <init|run>
    add_scheduler_parser(subparsers)
    # dags <add>
    add_dags_parser(subparsers)

    return parser