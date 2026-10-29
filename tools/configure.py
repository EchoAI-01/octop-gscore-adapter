"""Write the GsCore connector config file without exposing secrets in logs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS_DIR))

from _plugin_loader import load_plugin_package  # noqa: E402

load_plugin_package()
config_mod = __import__("gscore_connector_under_test.config", fromlist=["x"])
ConnectorConfig = config_mod.ConnectorConfig
default_config_path = config_mod.default_config_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Configure the Octop GsCore connector (writes JSON, hot reload)."
    )
    parser.add_argument("--config", type=Path, default=default_config_path())
    parser.add_argument("--show", action="store_true", help="print effective config")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--enable", action="store_true")
    group.add_argument("--disable", action="store_true")
    parser.add_argument("--host", help="GsCore host / Docker service name")
    parser.add_argument("--port", type=int)
    parser.add_argument("--url", help="full ws URL or URL template containing {bot_id}")
    parser.add_argument("--token", help="WS_TOKEN (avoid shell history; prefer --token-stdin)")
    parser.add_argument("--token-stdin", action="store_true", help="read WS_TOKEN from stdin")
    parser.add_argument("--bot-id", help="route bot_id (/ws/{bot_id})")
    parser.add_argument("--onebot-bot-id")
    parser.add_argument("--qq-group-bot-id", help="QQ official platform bot_id (reference: qq_official)")
    parser.add_argument("--feishu-bot-id", help="Feishu platform bot_id (reference: lark)")
    parser.add_argument("--channel-types", help="optional allowlist; empty observes every non-control channel")
    parser.add_argument("--gscore-only-prefixes", help="messages matching these prefixes skip the Agent path")
    parser.add_argument("--admin-user-ids", help="comma separated platform user ids")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    path: Path = args.config
    config = ConnectorConfig.load(path)

    if args.enable:
        config.enabled = True
    if args.disable:
        config.enabled = False
    if args.host is not None:
        config.gscore_host = args.host
    if args.port is not None:
        config.gscore_port = args.port
    if args.url is not None:
        config.gscore_ws_url = args.url
    if args.token_stdin:
        config.gscore_ws_token = sys.stdin.readline().strip()
    elif args.token is not None:
        config.gscore_ws_token = args.token
    if args.bot_id is not None:
        config.gscore_bot_id = args.bot_id
    if args.onebot_bot_id is not None:
        config.onebot_bot_id = args.onebot_bot_id
    if args.qq_group_bot_id is not None:
        config.qq_group_bot_id = args.qq_group_bot_id
    if args.feishu_bot_id is not None:
        config.feishu_bot_id = args.feishu_bot_id
    if args.channel_types is not None:
        config.channel_types = args.channel_types
    if args.gscore_only_prefixes is not None:
        config.gscore_only_prefixes = args.gscore_only_prefixes
    if args.admin_user_ids is not None:
        config.admin_user_ids = args.admin_user_ids

    if any(
        value is not None
        for value in (
            args.host,
            args.port,
            args.url,
            args.token,
            args.bot_id,
            args.onebot_bot_id,
            args.qq_group_bot_id,
            args.feishu_bot_id,
            args.channel_types,
            args.gscore_only_prefixes,
            args.admin_user_ids,
        )
    ) or args.enable or args.disable or args.token_stdin:
        config.save(path)

    if args.show or not any(
        value is not None
        for value in (
            args.host,
            args.port,
            args.url,
            args.token,
            args.bot_id,
            args.onebot_bot_id,
            args.qq_group_bot_id,
            args.feishu_bot_id,
            args.channel_types,
            args.gscore_only_prefixes,
            args.admin_user_ids,
        )
    ):
        safe = config.to_dict()
        if safe.get("gscore_ws_token"):
            safe["gscore_ws_token"] = "***"
        import json

        print(json.dumps(safe, ensure_ascii=False, indent=2))
    print(f"config: {path}")
    print(f"enabled: {config.enabled}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
