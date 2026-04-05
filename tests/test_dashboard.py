import importlib
import sqlite3
import sys
from pathlib import Path

import yaml
from fastapi.testclient import TestClient


def build_test_db(path: Path):
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute(
        '''
        CREATE TABLE news_items (
            id INTEGER PRIMARY KEY,
            title TEXT,
            platform_id TEXT,
            rank INTEGER,
            first_crawl_time TEXT,
            last_crawl_time TEXT,
            url TEXT,
            mobile_url TEXT,
            crawl_count INTEGER
        )
        '''
    )
    rows = [
        (1, 'AI 芯片公司发布新产品', 'weibo', 2, '2026-03-24 09:00:00', '2026-03-24 09:05:00', 'https://example.com/1', '', 5),
        (2, '新能源车销量创新高', 'baidu', 1, '2026-03-24 09:10:00', '2026-03-24 09:15:00', 'https://example.com/2', '', 3),
        (3, '体育赛事回顾', 'zhihu', 8, '2026-03-24 09:20:00', '2026-03-24 09:25:00', 'https://example.com/3', '', 1),
    ]
    cur.executemany('INSERT INTO news_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
    con.commit()
    con.close()


def test_dashboard_and_api_smoke(tmp_path, monkeypatch):
    db_dir = tmp_path / 'news'
    db_dir.mkdir()
    build_test_db(db_dir / '20260324.db')

    trend_root = tmp_path / 'trend'
    config_dir = trend_root / 'config'
    config_dir.mkdir(parents=True)
    config_path = config_dir / 'config.yaml'
    config_path.write_text(
        'schedule:\n  enabled: true\nplatforms:\n  enabled: true\n  sources: []\nrss:\n  enabled: false\n  feeds: []\nnotification:\n  enabled: true\nreport:\n  mode: simple\n',
        encoding='utf-8',
    )
    (config_dir / 'frequency_words.txt').write_text(
        '[WORD_GROUPS]\n[科技]\nAI\n芯片\n[汽车]\n新能源\n',
        encoding='utf-8',
    )

    monkeypatch.setenv('TREND_DB_PATH', str(db_dir))
    monkeypatch.setenv('TREND_ROOT', str(trend_root))

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import app as app_module
    importlib.reload(app_module)

    client = TestClient(app_module.app)

    dashboard = client.get('/')
    assert dashboard.status_code == 200
    assert 'TrendRadar Dashboard' in dashboard.text
    assert '新闻列表' in dashboard.text
    assert 'AI 芯片公司发布新产品' in dashboard.text
    assert '推荐新闻 <small>2</small>' in dashboard.text
    assert '最新新闻 <small>3</small>' in dashboard.text
    assert 'recommended-news-panel' in dashboard.text
    assert 'latest-news-panel' in dashboard.text
    assert '抓取总览' not in dashboard.text

    overview = client.get('/overview')
    assert overview.status_code == 200
    assert '抓取总览' in overview.text
    assert '关键词匹配简报' in overview.text
    assert '平台统计' in overview.text

    latest = client.get('/latest')
    assert latest.status_code == 200
    payload = latest.json()
    assert payload['count'] == 3

    recommended = client.get('/recommended')
    assert recommended.status_code == 200
    rec_payload = recommended.json()
    assert rec_payload['count'] >= 2
    assert rec_payload['items'][0]['matched_tags']

    toggle_resp = client.post('/config/rss/enabled', json={'enabled': True})
    assert toggle_resp.status_code == 200
    cfg = yaml.safe_load(config_path.read_text(encoding='utf-8'))
    assert cfg['rss']['enabled'] is True


def test_config_toggle_endpoints_tolerate_missing_sections(tmp_path, monkeypatch):
    db_dir = tmp_path / 'news'
    db_dir.mkdir()
    build_test_db(db_dir / '20260324.db')

    trend_root = tmp_path / 'trend'
    config_dir = trend_root / 'config'
    config_dir.mkdir(parents=True)
    config_path = config_dir / 'config.yaml'
    config_path.write_text('report:\n  mode: simple\n', encoding='utf-8')
    (config_dir / 'frequency_words.txt').write_text('', encoding='utf-8')

    monkeypatch.setenv('TREND_DB_PATH', str(db_dir))
    monkeypatch.setenv('TREND_ROOT', str(trend_root))

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import app as app_module
    importlib.reload(app_module)

    client = TestClient(app_module.app)

    assert client.post('/config/rss/enabled', json={'enabled': True}).status_code == 200
    assert client.post('/config/notification/enabled', json={'enabled': False}).status_code == 200
    assert client.post('/config/schedule/enabled', json={'enabled': True}).status_code == 200
    assert client.post('/config/platforms', json={'enabled_platform_ids': ['weibo', 'zhihu']}).status_code == 200

    cfg = yaml.safe_load(config_path.read_text(encoding='utf-8'))
    assert cfg['rss']['enabled'] is True
    assert cfg['notification']['enabled'] is False
    assert cfg['schedule']['enabled'] is True
    assert cfg['platforms']['enabled'] is True
    assert [item['id'] for item in cfg['platforms']['sources']] == ['weibo', 'zhihu']
