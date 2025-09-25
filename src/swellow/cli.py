from . import *
import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="swellow",
        description="Swellow is the simple, SQL-first tool for managing table migrations, written in Rust."
    )
    parser.add_argument("--db", required=True, help="Database connection string")
    parser.add_argument("--dir", required=True, help="Directory containing all migrations")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="store_true")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("peck", help="Test connection to the database")

    up_parser = subparsers.add_parser("up", help="Generate a migration plan and execute it")
    up_parser.add_argument("--current-version-id", type=int)
    up_parser.add_argument("--target-version-id", type=int)
    up_parser.add_argument("--plan", action="store_true")
    up_parser.add_argument("--dry-run", action="store_true")

    down_parser = subparsers.add_parser("down", help="Generate a rollback plan and execute it")
    down_parser.add_argument("--current-version-id", type=int)
    down_parser.add_argument("--target-version-id", type=int)
    down_parser.add_argument("--plan", action="store_true")
    down_parser.add_argument("--dry-run", action="store_true")

    subparsers.add_parser("snapshot", help="Take a snapshot of the database schema")

    args = parser.parse_args()

    try:
        if args.command == "peck":
            return_code = peck(args.db, args.dir)
        elif args.command == "up":
            return_code = up(args.db, args.dir, args.current_version_id, args.target_version_id, args.plan, args.dry_run)
        elif args.command == "down":
            return_code = down(args.db, args.dir, args.current_version_id, args.target_version_id, args.plan, args.dry_run)
        elif args.command == "snapshot":
            return_code = snapshot(args.db, args.dir)
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
