"""在独立解释器中验证真实管理接口，避免加载本机配置及污染其他测试的模块桩。"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = r'''
import asyncio, json, os, sys
from unittest.mock import AsyncMock, patch
sys.path.insert(0, sys.argv[1])
import main, model_loader
from fastapi.testclient import TestClient
from resumable import pump_stream, resumable_store

async def seed():
    records = []
    for name in ('fixture-one', 'fixture-two'):
        buf, _ = await resumable_store.create_if_absent(name, model='本地测试模型')
        async def body():
            await asyncio.Event().wait()
            yield 'data: [DONE]\n\n'
        task = asyncio.create_task(pump_stream(buf, body()))
        resumable_store.register_pump(buf.id, task, expected=buf)
        records.append(buf.meta())
    return records

with patch.object(model_loader, 'refresh_models_config_cache', AsyncMock(return_value=True)):
    with TestClient(main.app) as client:
        assert client.get('/api/tasks').status_code == 401
        assert client.post('/api/tasks/fixture-one/cancel', json={'attempt_id': 'unknown'}).status_code == 401
        assert client.post('/api/login', json={'password': os.environ['API_KEY']}).status_code == 200
        assert '后台任务' in client.get('/').text
        first, second = client.portal.call(seed)
        async def add_private_error():
            buf = await resumable_store.get(first['id'])
            buf.error = '不应出现在任务列表的异常正文或凭据'
        client.portal.call(add_private_error)
        response = client.get('/api/tasks')
        assert response.status_code == 200
        data = response.json()
        assert data['enabled'] is True and len(data['tasks']) == 2
        assert '不应出现' not in json.dumps(data, ensure_ascii=False)
        assert all('attempt_id' in task and 'phase' in task for task in data['tasks'])
        assert client.post('/api/tasks/fixture-one/cancel', json={'attempt_id': 'stale'}).status_code == 409
        assert client.post('/api/tasks/missing/cancel', json={'attempt_id': 'unknown'}).status_code == 404
        cancelled = client.post('/api/tasks/fixture-one/cancel', json={'attempt_id': first['attempt_id']})
        assert cancelled.status_code == 200 and cancelled.json()['status'] == 'cancelled'
        again = client.post('/api/tasks/fixture-one/cancel', json={'attempt_id': first['attempt_id']})
        assert again.json()['status'] == 'finished'
        tasks = {task['id']: task for task in client.get('/api/tasks').json()['tasks']}
        assert tasks[first['id']]['status_code'] == 499
        assert tasks[second['id']]['status'] == 'running'
        replacement, created = client.portal.call(resumable_store.create_if_absent, first['id'])
        assert created and replacement.attempt_id != first['attempt_id']
        stale = client.post('/api/tasks/fixture-one/cancel', json={'attempt_id': first['attempt_id']})
        assert stale.status_code == 409 and replacement.status == 'running'
print('管理接口鉴权、单任务停止、重复操作及旧轮次保护检查通过。')
'''


class BackgroundTasksApiTests(unittest.TestCase):
    def test_管理接口鉴权和逐轮次停止真实集成(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="vertex2openai-task-api-") as temp:
            env = dict(os.environ, PYTHONUTF8="1", STATE_DIR=temp, API_KEY="local-task-test-key",
                       VERTEX_EXPRESS_API_KEY="", GOOGLE_COOKIE="", GOOGLE_PROJECT_ID="", PROXY_URL="")
            result = subprocess.run([sys.executable, "-c", SCRIPT, str(root / "app")],
                                    cwd=temp, env=env, capture_output=True, encoding="utf-8", timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
