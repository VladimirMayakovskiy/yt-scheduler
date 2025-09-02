import sys
import traceback
from cli import _prepare_parser
from config import Config

def main_func():
    config = Config()
    parser = _prepare_parser()

    args, unrecognized = parser.parse_known_args()

    if unrecognized:
        print(unrecognized)
        args, _ = parser.parse_known_args(unrecognized, namespace=args)

    if args.proxy is not None:
        config.set_proxy(args.proxy)

    func_args = dict(vars(args))
    print(func_args)

    processed_keys = [
        "func",
        "proxy",
        "config", # todo
    ]

    for key in processed_keys:
        if key in func_args:
            func_args.pop(key)

    args.func(**func_args, config=config)

def main():
    try:
        main_func()
    except KeyboardInterrupt:
        print("Keyboard interrupt... exiting", file=sys.stderr)
        sys.exit(1)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()