"""在隔离配置中验证思考档位与后缀继承，不读取部署密钥或调用真实模型。"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = r'''
import sys
sys.path.insert(0, sys.argv[1])
import model_capabilities as mc
from models import OpenAIRequest
from runtime_state import app_state
from upstreams.express_sdk import _normalize_model_name, _build_thinking_config as express_thinking
from upstreams.cookie_proxy import _build_thinking_config as cookie_thinking

def request(model, **extra):
    return OpenAIRequest(model=model, messages=[], **extra)

app_state.update_settings({'native_thinking_mode': 'request', 'thinking_g3_level': ''})
for model in ('gemini-4-argon', 'gemini-4-hydrogen', 'gemini-3.1-flash-image'):
    req = request(model)
    assert express_thinking(model, req, False) == {'include_thoughts': True}
    assert cookie_thinking(model, req) == {'includeThoughts': True}

app_state.update_settings({'native_thinking_mode': 'off'})
for model, lowest in (('gemini-3.6-flash', 'minimal'), ('gemini-3.7-flash', 'low'),
                      ('gemini-4-hydrogen', 'low')):
    req = request(model, reasoning_effort='xhigh')
    assert express_thinking(model, req, False) == {'thinking_level': lowest, 'include_thoughts': False}
    assert cookie_thinking(model, req) == {'thinkingLevel': lowest.upper(), 'includeThoughts': False}

app_state.update_settings({'native_thinking_mode': 'console', 'thinking_g3_level': 'high',
                           'prefill_suppress_thinking': True})
req = request('gemini-4-hydrogen', reasoning_effort='low')
assert express_thinking(req.model, req, False, prefill_active=True)['thinking_level'] == 'high'
assert cookie_thinking(req.model, req, prefill_active=True)['includeThoughts'] is True

app_state.update_settings({'native_thinking_mode': 'request', 'thinking_g3_level': ''})
for name, level in (('gemini-4-hydrogen-think-high-search', 'high'),
                    ('gemini-4-hydrogen-search-think-high', 'high'),
                    ('gemini-4-hydrogen-think-off-search', 'low'),
                    ('gemini-4-hydrogen-search-think-off', 'low')):
    base, search = _normalize_model_name(name)
    assert base == 'gemini-4-hydrogen' and search
    req = request(name)
    express = express_thinking(base, req, False)
    cookie = cookie_thinking(base, req)
    assert express['thinking_level'] == level, (name, express)
    assert cookie['thinkingLevel'] == level.upper(), (name, cookie)
    assert express['include_thoughts'] == ('off' not in name)

req = request('gemini-4-hydrogen-think-low', reasoning_effort='high')
assert mc.resolve_thinking(req.model, req, app_state.get_settings())['level'] == 'high'
assert mc.resolve_thinking(req.model, req, app_state.get_settings(), prefill_active=True)['level'] == 'low'
assert mc.resolve_thinking(req.model, req, app_state.get_settings(), prefill_active=True)['include_thoughts'] is False

app_state.set_model_override('gemini-4-hydrogen', {'thinking_g3_level': 'medium'})
for name in ('gemini-4-hydrogen-search-think-high', 'gemini-4-hydrogen-think-high-search',
             'projects/local/locations/global/publishers/google/models/gemini-4-hydrogen-search-think-high'):
    assert app_state.get_effective_settings(name)['thinking_g3_level'] == 'medium', name
app_state.set_model_override('gemini-4-hydrogen-search-think-high', {'thinking_g3_level': 'low'})
assert app_state.get_effective_settings('gemini-4-hydrogen-search-think-high')['thinking_g3_level'] == 'low'
print('双通道自动/压制/控制台思考档位、后缀组合及覆盖设置继承检查通过。')
'''


class ThinkingConfigTests(unittest.TestCase):
    def test_双通道思考优先级和模型后缀组合(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="vertex2openai-thinking-") as temp:
            env = dict(os.environ, PYTHONUTF8="1", STATE_DIR=temp, API_KEY="local-thinking-test-key",
                       VERTEX_EXPRESS_API_KEY="", GOOGLE_COOKIE="", GOOGLE_PROJECT_ID="", PROXY_URL="")
            result = subprocess.run([sys.executable, "-c", SCRIPT, str(root / "app")],
                                    cwd=temp, env=env, capture_output=True, encoding="utf-8", timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
