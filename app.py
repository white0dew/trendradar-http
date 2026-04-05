from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import os
import re
import sqlite3
import subprocess

import yaml
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

DB_PATH = Path(os.environ.get('TREND_DB_PATH', '/data/output/news'))
TREND_ROOT = Path(os.environ.get('TREND_ROOT', '/data/trendradar'))
CONFIG_PATH = TREND_ROOT / 'config' / 'config.yaml'
KEYWORDS_PATH = TREND_ROOT / 'config' / 'frequency_words.txt'
DOCKER_BIN = os.environ.get('DOCKER_BIN', '/usr/local/bin/docker')
CONTAINER_NAME = os.environ.get('TREND_CONTAINER_NAME', 'trendradar')
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / 'templates'
STATIC_DIR = BASE_DIR / 'static'

STATE_DIR = TREND_ROOT / "state"
LAST_SENT_PATH = STATE_DIR / "last_sent_recommended.json"
DEFAULT_PLATFORM_SOURCES = [
    {'id': 'toutiao', 'name': '今日头条'}, {'id': 'baidu', 'name': '百度热搜'},
    {'id': 'wallstreetcn-hot', 'name': '华尔街见闻'}, {'id': 'thepaper', 'name': '澎湃新闻'},
    {'id': 'bilibili-hot-search', 'name': 'bilibili 热搜'}, {'id': 'cls-hot', 'name': '财联社热门'},
    {'id': 'ifeng', 'name': '凤凰网'}, {'id': 'tieba', 'name': '贴吧'},
    {'id': 'weibo', 'name': '微博'}, {'id': 'douyin', 'name': '抖音'}, {'id': 'zhihu', 'name': '知乎'},
]
PLATFORM_MAP = {item['id']: item for item in DEFAULT_PLATFORM_SOURCES}
PLATFORM_NAME_MAP = {item['id']: item['name'] for item in DEFAULT_PLATFORM_SOURCES}

app = FastAPI(title='trendradar-http', version='0.6.0')
app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def latest_db_path() -> Path:
    files = sorted(DB_PATH.glob('*.db'))
    if not files:
        raise FileNotFoundError(f'No db files in {DB_PATH}')
    return files[-1]


def get_conn(db: Optional[str] = None):
    path = Path(db) if db else latest_db_path()
    return sqlite3.connect(path)


def load_yaml() -> Dict[str, Any]:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def save_yaml(data: Dict[str, Any]):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def run_cmd(cmd: List[str]) -> Dict[str, Any]:
    p = subprocess.run(cmd, capture_output=True, text=True)
    return {'ok': p.returncode == 0, 'code': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr, 'cmd': cmd}


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def load_last_sent_state() -> Dict[str, Any]:
    try:
        if LAST_SENT_PATH.exists():
            return yaml.safe_load(LAST_SENT_PATH.read_text(encoding='utf-8')) or {}
    except Exception:
        return {}
    return {}


def save_last_sent_state(data: Dict[str, Any]) -> None:
    ensure_state_dir()
    LAST_SENT_PATH.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding='utf-8')


def item_fingerprint(item: Dict[str, Any]) -> str:
    return ' | '.join([
        item.get('title', ''),
        item.get('platform_id', ''),
        item.get('url', ''),
        ','.join(item.get('matched_tags') or []),
    ])


def diff_against_last_sent(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    state = load_last_sent_state()
    prev_items = state.get('items') or []
    prev_fps = {item_fingerprint(x) for x in prev_items}
    new_items = [x for x in items if item_fingerprint(x) not in prev_fps]
    overlap = len(items) - len(new_items)
    return {
        'previous': state,
        'new_items': new_items,
        'overlap_count': overlap,
        'all_same': bool(items) and len(new_items) == 0,
    }


def parse_keywords_text(text: str) -> Dict[str, Any]:
    lines = [line.rstrip() for line in text.splitlines()]
    groups = []
    current = None
    for line in lines:
        s = line.strip()
        if not s or s.startswith('#'):
            continue
        if s.startswith('[') and s.endswith(']'):
            current = {'group': s[1:-1], 'lines': []}
            groups.append(current)
        else:
            if current is None:
                current = {'group': 'default', 'lines': []}
                groups.append(current)
            current['lines'].append(s)
    return {'raw': text, 'groups': groups}


def mutate_config(mutator):
    cfg = load_yaml()
    new_cfg = mutator(cfg) or cfg
    save_yaml(new_cfg)
    return new_cfg


def ensure_config_section(cfg: Dict[str, Any], section: str) -> Dict[str, Any]:
    value = cfg.get(section)
    if not isinstance(value, dict):
        value = {}
        cfg[section] = value
    return value


def load_match_rules() -> Tuple[List[str], List[Tuple[str, re.Pattern]], List[Tuple[str, str]]]:
    text = KEYWORDS_PATH.read_text(encoding='utf-8') if KEYWORDS_PATH.exists() else ''
    lines = text.splitlines()
    in_global = False
    in_groups = False
    global_filters = []
    regex_rules = []
    text_rules = []
    current_group_alias = None
    for raw in lines:
        s = raw.strip()
        if s == '[GLOBAL_FILTER]':
            in_global, in_groups = True, False
            continue
        if s == '[WORD_GROUPS]':
            in_groups, in_global = True, False
            continue
        if not s or s.startswith('#'):
            continue
        if in_global:
            if s.startswith('/') and s.endswith('/'):
                try:
                    global_filters.append(re.compile(s[1:-1], re.I))
                except re.error:
                    pass
            else:
                global_filters.append(s)
            continue
        if not in_groups:
            continue
        if s.startswith('[') and s.endswith(']'):
            current_group_alias = s[1:-1]
            continue
        if s.startswith('!') or s.startswith('+') or s.startswith('@'):
            continue
        alias = current_group_alias
        expr = s
        if '=>' in s:
            left, right = s.split('=>', 1)
            expr = left.strip()
            alias = right.strip()
        if expr.startswith('/'):
            m = re.match(r'^/(.*?)/([a-zA-Z]*)$', expr)
            if m:
                pattern, flags = m.groups()
                fl = re.I if 'i' in flags else 0
                try:
                    regex_rules.append((alias or expr, re.compile(pattern, fl)))
                except re.error:
                    pass
        else:
            text_rules.append((alias or expr, expr.lower()))
    return global_filters, regex_rules, text_rules


def is_filtered(title: str, filters) -> bool:
    for f in filters:
        if isinstance(f, str):
            if f and f in title:
                return True
        else:
            if f.search(title):
                return True
    return False


def match_title(title: str, filters, regex_rules, text_rules) -> List[str]:
    if is_filtered(title, filters):
        return []
    matched = []
    for alias, pat in regex_rules:
        if pat.search(title):
            matched.append(alias)
    low = title.lower()
    for alias, txt in text_rules:
        if txt and txt in low:
            matched.append(alias)
    out = []
    for x in matched:
        if x not in out:
            out.append(x)
    return out


def serialize_news_row(row, matched_tags: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        'id': row[0],
        'title': row[1],
        'platform_id': row[2],
        'platform_name': PLATFORM_NAME_MAP.get(row[2], row[2]),
        'rank': row[3],
        'first_crawl_time': row[4],
        'last_crawl_time': row[5],
        'url': row[6],
        'mobile_url': row[7],
        'crawl_count': row[8],
        'matched_tags': matched_tags or [],
    }


def fetch_news_rows(limit: int, platform: Optional[str] = None, sql_limit: Optional[int] = None) -> List[tuple]:
    con = get_conn()
    try:
        cur = con.cursor()
        sql = 'SELECT id, title, platform_id, rank, first_crawl_time, last_crawl_time, url, mobile_url, crawl_count FROM news_items'
        params: List[Any] = []
        if platform:
            sql += ' WHERE platform_id = ?'
            params.append(platform)
        sql += ' ORDER BY id DESC LIMIT ?'
        params.append(sql_limit or limit)
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        con.close()


def get_latest_items(limit: int = 20, platform: Optional[str] = None) -> Dict[str, Any]:
    rows = fetch_news_rows(limit=limit, platform=platform)
    return {'ok': True, 'db': latest_db_path().name, 'count': len(rows), 'items': [serialize_news_row(r) for r in rows]}


def get_matched_items(limit: int = 50, platform: Optional[str] = None) -> Dict[str, Any]:
    filters, regex_rules, text_rules = load_match_rules()
    rows = fetch_news_rows(limit=limit, platform=platform, sql_limit=1000)
    items = []
    for r in rows:
        tags = match_title(r[1], filters, regex_rules, text_rules)
        if tags:
            items.append(serialize_news_row(r, matched_tags=tags))
        if len(items) >= limit:
            break
    return {'ok': True, 'db': latest_db_path().name, 'count': len(items), 'items': items}


def get_recommended_items(limit: int = 20) -> Dict[str, Any]:
    filters, regex_rules, text_rules = load_match_rules()
    rows = fetch_news_rows(limit=1500, sql_limit=1500)
    scored = []
    for r in rows:
        tags = match_title(r[1], filters, regex_rules, text_rules)
        if not tags:
            continue
        score = len(tags) * 100 + max(0, 40 - min(r[3] or 40, 40)) + min(r[8] or 1, 20)
        scored.append((score, serialize_news_row(r, matched_tags=tags)))
    scored.sort(key=lambda x: (-x[0], -x[1]['crawl_count'], x[1]['rank']))
    items = [x[1] for x in scored[:limit]]
    return {'ok': True, 'db': latest_db_path().name, 'count': len(items), 'items': items}


def get_brief(limit: int = 10) -> Dict[str, Any]:
    data = get_recommended_items(limit=limit)
    grouped = defaultdict(list)
    for item in data['items']:
        for tag in item['matched_tags'][:2]:
            grouped[tag].append(item)
    lines = []
    for tag, items in list(grouped.items())[:8]:
        lines.append(f'【{tag}】')
        for it in items[:3]:
            lines.append(f"- {it['title']}（{it['platform_name']} #{it['rank']}）")
    return {'ok': True, 'db': latest_db_path().name, 'count': data['count'], 'brief': '\n'.join(lines), 'items': data['items'][:limit]}


def get_platform_stats() -> Dict[str, Any]:
    con = get_conn()
    try:
        cur = con.cursor()
        cur.execute('SELECT platform_id, COUNT(*) cnt FROM news_items GROUP BY platform_id ORDER BY cnt DESC')
        rows = cur.fetchall()
        return {'ok': True, 'db': latest_db_path().name, 'platforms': [{'platform_id': r[0], 'platform_name': PLATFORM_NAME_MAP.get(r[0], r[0]), 'count': r[1]} for r in rows]}
    finally:
        con.close()


def build_dashboard_context() -> Dict[str, Any]:
    latest = get_latest_items(limit=12)
    recommended_items = get_recommended_items(limit=30)
    latest_items = latest['items']

    return {
        'db_name': latest.get('db'),
        'latest_items': latest_items,
        'recommended_items': recommended_items['items'],
        'newest_time': latest_items[0]['last_crawl_time'] if latest_items else None,
    }


def build_overview_context() -> Dict[str, Any]:
    health_info = health()
    latest = get_latest_items(limit=12)
    brief_data = get_brief(limit=8)
    platforms_data = get_platform_stats()

    platform_counts = platforms_data['platforms']
    total_news = sum(item['count'] for item in platform_counts)
    top_platforms = platform_counts[:6]
    latest_items = latest['items']
    newest_time = latest_items[0]['last_crawl_time'] if latest_items else None

    tag_counter = Counter()
    for item in brief_data['items']:
        tag_counter.update(item['matched_tags'])

    return {
        'health': health_info,
        'db_name': latest.get('db'),
        'total_news': total_news,
        'platform_total': len(platform_counts),
        'top_platforms': top_platforms,
        'brief_text': brief_data['brief'],
        'brief_groups': [{'tag': tag, 'count': count} for tag, count in tag_counter.most_common(8)],
        'newest_time': newest_time,
        'recommended_count': brief_data['count'],
    }


class PlatformsUpdate(BaseModel):
    enabled_platform_ids: List[str] = Field(default_factory=list)


class KeywordsUpdate(BaseModel):
    content: str


class ToggleUpdate(BaseModel):
    enabled: bool


def build_settings_context() -> Dict[str, Any]:
    return {
        'config_summary': config_summary(),
        'platforms_config': config_platforms(),
        'keywords_config': get_keywords(),
    }


@app.get('/', response_class=HTMLResponse)
@app.get('/dashboard', response_class=HTMLResponse)
def dashboard(request: Request):
    try:
        context = build_dashboard_context()
        return templates.TemplateResponse(request, 'dashboard.html', {'request': request, **context})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/overview', response_class=HTMLResponse)
def overview_page(request: Request):
    try:
        context = build_overview_context()
        return templates.TemplateResponse(request, 'overview.html', {'request': request, **context})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/settings', response_class=HTMLResponse)
def settings_page(request: Request):
    try:
        context = build_settings_context()
        return templates.TemplateResponse(request, 'settings.html', {'request': request, **context})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/health')
def health():
    try:
        db = latest_db_path()
        return {'ok': True, 'latest_db': str(db.name), 'version': app.version}
    except Exception as e:
        return JSONResponse(status_code=503, content={'ok': False, 'error': str(e)})


@app.get('/latest')
def latest(limit: int = Query(20, ge=1, le=200), platform: Optional[str] = None):
    try:
        return get_latest_items(limit=limit, platform=platform)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/matched')
def matched(limit: int = Query(50, ge=1, le=300), platform: Optional[str] = None):
    try:
        return get_matched_items(limit=limit, platform=platform)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/recommended')
def recommended(limit: int = Query(20, ge=1, le=100)):
    try:
        return get_recommended_items(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/brief')
def brief(limit: int = Query(10, ge=1, le=30)):
    try:
        return get_brief(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/search')
def search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100), platform: Optional[str] = None):
    try:
        con = get_conn()
        try:
            cur = con.cursor()
            sql = 'SELECT id, title, platform_id, rank, first_crawl_time, last_crawl_time, url, mobile_url, crawl_count FROM news_items WHERE title LIKE ?'
            params: List[Any] = [f'%{q}%']
            if platform:
                sql += ' AND platform_id = ?'
                params.append(platform)
            sql += ' ORDER BY id DESC LIMIT ?'
            params.append(limit)
            cur.execute(sql, params)
            rows = cur.fetchall()
            return {'ok': True, 'db': latest_db_path().name, 'query': q, 'count': len(rows), 'items': [serialize_news_row(r) for r in rows]}
        finally:
            con.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/platforms')
def platforms():
    try:
        return get_platform_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/config/summary')
def config_summary():
    try:
        cfg = load_yaml()
        return {'ok': True, 'schedule': cfg.get('schedule', {}), 'platforms_enabled': cfg.get('platforms', {}).get('enabled'), 'platform_count': len(cfg.get('platforms', {}).get('sources', []) or []), 'rss_enabled': cfg.get('rss', {}).get('enabled'), 'rss_count': len(cfg.get('rss', {}).get('feeds', []) or []), 'notification_enabled': cfg.get('notification', {}).get('enabled'), 'report': cfg.get('report', {})}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/config/platforms')
def config_platforms():
    try:
        cfg = load_yaml()
        sources = cfg.get('platforms', {}).get('sources', []) or []
        return {'ok': True, 'enabled': cfg.get('platforms', {}).get('enabled', True), 'sources': sources, 'all_supported_sources': DEFAULT_PLATFORM_SOURCES}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/config/platforms')
def update_platforms(payload: PlatformsUpdate):
    missing = [pid for pid in payload.enabled_platform_ids if pid not in PLATFORM_MAP]
    if missing:
        raise HTTPException(status_code=400, detail=f'Unknown platform ids: {missing}')

    def _mutate(cfg: Dict[str, Any]):
        platforms_cfg = ensure_config_section(cfg, 'platforms')
        platforms_cfg.update({'sources': [PLATFORM_MAP[pid] for pid in payload.enabled_platform_ids], 'enabled': bool(payload.enabled_platform_ids)})
        return cfg

    mutate_config(_mutate)
    return {'ok': True, 'enabled_platform_ids': payload.enabled_platform_ids, 'hot_reload_supported': False, 'note': 'TrendRadar 每次运行时重新加载配置；修改后 run-once 或下一次 cron 会自动生效。'}


@app.get('/config/keywords')
def get_keywords():
    try:
        text = KEYWORDS_PATH.read_text(encoding='utf-8') if KEYWORDS_PATH.exists() else ''
        return {'ok': True, **parse_keywords_text(text)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/config/keywords')
def update_keywords(payload: KeywordsUpdate):
    try:
        KEYWORDS_PATH.write_text(payload.content, encoding='utf-8')
        return {'ok': True, 'hot_reload_supported': False, 'note': '关键词文件已写入；TrendRadar 在每次运行时重新读取配置，因此 run-once 或下一次 cron 会自动生效。'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/config/rss/enabled')
def set_rss_enabled(payload: ToggleUpdate):
    mutate_config(lambda cfg: ensure_config_section(cfg, 'rss').__setitem__('enabled', payload.enabled) or cfg)
    return {'ok': True, 'rss_enabled': payload.enabled, 'hot_reload_supported': False}


@app.post('/config/notification/enabled')
def set_notification_enabled(payload: ToggleUpdate):
    mutate_config(lambda cfg: ensure_config_section(cfg, 'notification').__setitem__('enabled', payload.enabled) or cfg)
    return {'ok': True, 'notification_enabled': payload.enabled, 'hot_reload_supported': False}


@app.post('/config/schedule/enabled')
def set_schedule_enabled(payload: ToggleUpdate):
    mutate_config(lambda cfg: ensure_config_section(cfg, 'schedule').__setitem__('enabled', payload.enabled) or cfg)
    return {'ok': True, 'schedule_enabled': payload.enabled, 'hot_reload_supported': False}


@app.get('/service/status')
def service_status():
    try:
        inspect = run_cmd([DOCKER_BIN, 'inspect', CONTAINER_NAME])
        return {'ok': True, 'inspect': inspect}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/service/restart')
def service_restart():
    try:
        result = run_cmd([DOCKER_BIN, 'restart', CONTAINER_NAME])
        return {'ok': result['ok'], 'result': result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/crawl/run-once')
def crawl_run_once():
    try:
        result = run_cmd([DOCKER_BIN, 'exec', CONTAINER_NAME, 'python', '-m', 'trendradar'])
        return {'ok': result['ok'], 'result': result, 'note': '已触发单次抓取；TrendRadar 本体按次读取配置，所以这里会吃到最新配置。'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/delivery/last-sent')
def delivery_last_sent():
    state = load_last_sent_state()
    return {'ok': True, 'state': state, 'path': str(LAST_SENT_PATH)}


@app.post('/delivery/last-sent/save')
def delivery_last_sent_save(limit: int = Query(20, ge=1, le=100)):
    data = get_recommended_items(limit=limit)
    diff = diff_against_last_sent(data['items'])
    payload = {
        'sent_at': None,
        'source_db': data.get('db'),
        'count': len(data['items']),
        'items': data['items'],
        'new_items_count': len(diff['new_items']),
        'overlap_count': diff['overlap_count'],
    }
    save_last_sent_state(payload)
    return {'ok': True, 'saved': True, 'path': str(LAST_SENT_PATH), 'state': payload}


@app.get('/delivery/diff')
def delivery_diff(limit: int = Query(20, ge=1, le=100)):
    data = get_recommended_items(limit=limit)
    diff = diff_against_last_sent(data['items'])
    return {
        'ok': True,
        'source_db': data.get('db'),
        'current_count': len(data['items']),
        'new_items_count': len(diff['new_items']),
        'overlap_count': diff['overlap_count'],
        'all_same': diff['all_same'],
        'new_items': diff['new_items'],
        'previous': diff['previous'],
    }


@app.post('/delivery/complete-cycle')
def delivery_complete_cycle(limit: int = Query(20, ge=1, le=100), doc_url: str = Query('', description='发送成功后的飞书文档链接')):
    data = get_recommended_items(limit=limit)
    diff = diff_against_last_sent(data['items'])
    if diff['all_same']:
        return {
            'ok': True,
            'should_send': False,
            'reason': 'same_as_last_sent',
            'source_db': data.get('db'),
            'current_count': len(data['items']),
            'overlap_count': diff['overlap_count'],
            'new_items_count': 0,
            'new_items': [],
        }

    payload = {
        'sent_at': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
        'doc_url': doc_url,
        'source_db': data.get('db'),
        'count': len(data['items']),
        'items': data['items'],
        'new_items_count': len(diff['new_items']),
        'overlap_count': diff['overlap_count'],
    }
    save_last_sent_state(payload)
    return {
        'ok': True,
        'should_send': True,
        'source_db': data.get('db'),
        'current_count': len(data['items']),
        'overlap_count': diff['overlap_count'],
        'new_items_count': len(diff['new_items']),
        'new_items': diff['new_items'],
        'saved_path': str(LAST_SENT_PATH),
    }
