from cli import get_parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    args.func(args)


if __name__ == "__main__":
    main()
