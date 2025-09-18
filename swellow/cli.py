from .app import *
import argparse


def main():
    parser = argparse.ArgumentParser(
        prog="swellow",
        description="The simple, intuitive tool for managing table migrations, written in Rust."
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

    if args.command == "peck":
        peck(args.db)
    elif args.command == "up":
        up(args.db, args.dir, args.current_version_id, args.target_version_id, args.plan, args.dry_run)
    elif args.command == "down":
        down(args.db, args.dir, args.current_version_id, args.target_version_id, args.plan, args.dry_run)
    elif args.command == "snapshot":
        snapshot(args.db, args.dir)
