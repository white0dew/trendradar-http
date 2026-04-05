#!/usr/bin/env python3
import json
import os
import sys
import urllib.request
import urllib.error
from typing import Any

BASE_URL = os.environ.get('TRENDRADAR_BASE_URL', 'http://103.38.82.9:3333')
TARGET = os.environ.get('OC_TARGET', 'chat:oc_8ef926f368e194af3007377b2ec677e8')
CHANNEL = os.environ.get('OC_CHANNEL', 'feishu')
LIMIT = int(os.environ.get('TRENDRADAR_LIMIT', '8'))
TIME_LABEL = os.environ.get('TIME_LABEL', '推荐快报')


def fetch_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


def build_message(data: dict) -> str:
    items = data.get('items', [])
    if not items:
        return f"【TrendRadar｜{TIME_LABEL}】\n当前没有匹配到符合关键词的推荐新闻。\n\n查看：{BASE_URL}/recommended?limit={LIMIT}"

    lines = [f"【TrendRadar｜{TIME_LABEL}】", f"关键词推荐 {len(items)} 条："]
    for i, item in enumerate(items, 1):
        title = item.get('title', '').strip()
        platform = item.get('platform_name') or item.get('platform_id') or '未知来源'
        tags = '/'.join(item.get('matched_tags') or [])
        rank = item.get('rank')
        url = item.get('url') or item.get('mobile_url') or ''
        meta = f"{platform}"
        if rank is not None:
            meta += f" #{rank}"
        if tags:
            meta += f" | {tags}"
        lines.append(f"{i}. {title}")
        lines.append(f"   {meta}")
        if url:
            lines.append(f"   {url}")
    lines.append('')
    lines.append(f"完整推荐：{BASE_URL}/recommended?limit={LIMIT}")
    lines.append(f"简报接口：{BASE_URL}/brief?limit={LIMIT}")
    return '\n'.join(lines)


def main() -> int:
    try:
        data = fetch_json(f"{BASE_URL}/recommended?limit={LIMIT}")
        message = build_message(data)
        cmd = (
            f"openclaw message send --channel {sh_quote(CHANNEL)} "
            f"--target {sh_quote(TARGET)} --message {sh_quote(message)}"
        )
        rc = os.system(cmd)
        return 0 if rc == 0 else 1
    except urllib.error.URLError as e:
        sys.stderr.write(f"fetch failed: {e}\n")
        return 2
    except Exception as e:
        sys.stderr.write(f"unexpected error: {e}\n")
        return 3


if __name__ == '__main__':
    raise SystemExit(main())
