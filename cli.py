from typing import NamedTuple, Iterable, Callable, Any
import argparse

from flow_runner import init, run


class CLIArg:
    def __init__(self, flags: Iterable[str] = None, help: str = None, required: bool = None, type: Any = str):
        self.flags = flags
        self.kwargs = {}
        for k, v in locals().items():
            if k not in {"self", "flags", "kwargs"} and v is not None:
                self.kwargs[k] = v


class CLICommand(NamedTuple):
    name: str
    func: Callable
    description: str
    args: Iterable[CLIArg]


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='flow_runner', description='Flow Runner CLI')
    subparsers = parser.add_subparsers(dest='subcommand', required=True)
    command_dict = {sp.name: sp for sp in commands}
    for _, sub in sorted(command_dict.items()):
        if isinstance(sub, CLICommand):
            sub_proc = subparsers.add_parser(sub.name, help=sub.description)
            for arg in sub.args:
                sub_proc.add_argument(*arg.flags, **arg.kwargs)
            sub_proc.set_defaults(func=sub.func)
    return parser


commands = (
    CLICommand(
        name="init",
        description="Initialize database.",
        func=init,
        args=(
            CLIArg(
                flags=["--yt-proxy"],
                help='yt proxy for YTsaurus client',
                required=False
            ),
        )
    ),
    CLICommand(
        name="run",
        description='Run dag',
        func=run,
        args=(
            CLIArg(
                flags=["--spec"],
                help="Path to the workflow specification (YAML file).",
                required=True
            ),
            CLIArg(
                flags=["--work-dir"],
                help="Working directory for the pipeline.",
                required=False
            )
        )
    )
)
