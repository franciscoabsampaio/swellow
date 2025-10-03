from . import *
import argparse
import sys


class HelpFormatterRedirect(argparse.HelpFormatter):
    """
    Overrides the default formatter by a call to the Rust binary.
    """

    def __init__(
        self,
        prog,
        indent_increment=2,
        max_help_position=24,
        width=None
    ):
        super().__init__(prog, indent_increment, max_help_position, width)

    def format_help(self):
        """
        Overrides the main help method to extract argument data only.
        """
        from .app import _run_swellow
        _run_swellow(*sys.argv[1:], capture_output=False)
        return ""


def main():
    parser = argparse.ArgumentParser(
        prog="swellow",
        formatter_class=HelpFormatterRedirect
    )

    parser.add_argument("--db", required=True)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--engine", required=True, default="postgres")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="store_true")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("peck")

    up_parser = subparsers.add_parser("up")
    up_parser.add_argument("--current-version-id", type=int)
    up_parser.add_argument("--target-version-id", type=int)
    up_parser.add_argument("--plan", action="store_true")
    up_parser.add_argument("--dry-run", action="store_true")

    down_parser = subparsers.add_parser("down")
    down_parser.add_argument("--current-version-id", type=int)
    down_parser.add_argument("--target-version-id", type=int)
    down_parser.add_argument("--plan", action="store_true")
    down_parser.add_argument("--dry-run", action="store_true")

    subparsers.add_parser("snapshot")

    args = parser.parse_args()

    try:
        if args.command == "peck":
            return_code = peck(args.db, args.dir, args.engine)
        elif args.command == "up":
            return_code = up(args.db, args.dir, args.engine, args.current_version_id, args.target_version_id, args.plan, args.dry_run)
        elif args.command == "down":
            return_code = down(args.db, args.dir, args.engine, args.current_version_id, args.target_version_id, args.plan, args.dry_run)
        elif args.command == "snapshot":
            return_code = snapshot(args.db, args.dir, args.engine)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return_code = 2
    except SwellowError as e:
        print(f"Error: {e}", file=sys.stderr)
        return_code = 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return_code = 1

    sys.exit(return_code)
