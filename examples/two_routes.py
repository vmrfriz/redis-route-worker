"""Run from the project root: python examples/two_routes.py /tmp/storage."""

import argparse
import json
from pathlib import Path

from redis_route_worker import QueueRoute, RedisWorker, TaskContext


class ExampleHandler:
    def __init__(self, path: Path):
        self.path = path

    def create_directory(self, raw: str, context: TaskContext):
        task = json.loads(raw)
        (self.path / task["path"]).mkdir(parents=True, exist_ok=True)
        context.complete(json.dumps({"id": task["id"]}))

    def ping(self, raw: str, context: TaskContext):
        context.complete(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()
    handler = ExampleHandler(args.path)
    RedisWorker(
        routes=[
            QueueRoute("createDir", "newDir", "created", handler.create_directory, workers=1),
            QueueRoute("ping", "ping", "pong", handler.ping, workers=2),
        ],
        prefix="laravel_database",
        host=args.host,
        port=args.port,
    ).run()


if __name__ == "__main__":
    main()
