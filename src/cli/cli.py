from __future__ import annotations

from typing import Iterable, Any, NamedTuple, Callable, Sequence
import argparse

from .commands import *


class CLIArg:
    def __init__(self, flags: Iterable[str] = None, help: str = None, required: bool = None, type: Any = str):
        self.flags = flags
        self.kwargs = {}
        for k, v in locals().items():
            if k not in {"self", "flags", "kwargs"} and v is not None:
                self.kwargs[k] = v


class CLICommand(NamedTuple):
    name: str
    description: str
    args: Iterable[CLIArg]
    func: Callable | None = None
    subcommands: Iterable["CLICommand"] | None = None


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='flow_runner', description='Flow Runner CLI')
    _add_commands(parser, commands_)
    return parser


def _add_commands(parser: argparse.ArgumentParser, commands: Iterable[CLICommand]):
    subparsers = parser.add_subparsers(dest='subcommand', required=True)
    for cmd in commands:
        if isinstance(cmd, CLICommand):
            sub_proc = subparsers.add_parser(cmd.name, help=cmd.description)
            for arg in cmd.args:
                sub_proc.add_argument(*arg.flags, **arg.kwargs)
            if cmd.subcommands:
                _add_commands(sub_proc, cmd.subcommands)
            else:
                sub_proc.set_defaults(func=cmd.func)

ARG_YT_PROXY = CLIArg(flags=["--yt-proxy"], help='yt proxy for YTsaurus client', required=True)
ARG_RUN_ID = CLIArg(flags=["--run-id"], help="The id of the dag.", required=True)

commands_ = (
    CLICommand(
        name="scheduler",
        description='scheduler commands',
        args=(),
        subcommands=(
            CLICommand(
                name="run",
                description="run scheduler",
                func=run_scheduler,
                args=(
                    ARG_YT_PROXY,
                )
            ),
        )
    ),
    CLICommand(
        name="dags",
        description="dag commands",
        args=(),
        subcommands=(
            CLICommand(
                name="add",
                description="add dag to scheduler",
                func=add_dag,
                args=(
                    ARG_YT_PROXY,
                    CLIArg(flags=["--spec"], help="Path to the workflow specification (YAML file).", required=True),
                    CLIArg(flags=["--work-dir"], help="Working directory for the pipeline.", required=False),
                )
            ),
            CLICommand(
                name="state",
                description="Get the status of a dag run",
                func=dag_run_state,
                args=(
                    ARG_YT_PROXY,
                    ARG_RUN_ID,
                )
            ),
        )
    ),
    CLICommand(
        name="tasks",
        description="task commands",
        args=(),
        subcommands=(
            CLICommand(
                name="list-runs",
                description="List the taskruns within a dag run",
                func=taskrun_list,
                args=(
                    ARG_YT_PROXY,
                    ARG_RUN_ID,
                )
            ),
        )
    ),
    CLICommand(
        name="init",
        description="initialize database",
        func=init,
        args=(
            ARG_YT_PROXY,
        )
    )
)