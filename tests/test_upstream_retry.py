"""使用假凭据和假 SDK 调用验证统一调度器，不加载部署配置或发送网络请求。"""

import asyncio
import importlib
import json
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi.responses import JSONResponse, StreamingResponse
from test_chat_api_resumable import models
from test_resumable import ROOT, r


settings = {"retry_max": 2, "retry_backoff_seconds": 0, "cross_channel_fallback": False}
config = types.ModuleType("config")
config.DEFAULT_SETTINGS = settings
config.FAKE_STREAMING_ENABLED = False
config.FAKE_STREAMING_INTERVAL_SECONDS = 1
config.ROUNDROBIN = True
config.SAFETY_SCORE = False
config.PROXY_URL = None
config.SSL_CERT_FILE = None
config.VERTEX_BASE_URL = None
state = types.SimpleNamespace(get_setting=lambda key, default=None: settings.get(key, default))
runtime = types.ModuleType("runtime_state")
runtime.app_state = state
with patch.dict(sys.modules, {"config": config, "runtime_state": runtime, "resumable": r}), \
     patch.object(sys, "path", [str(ROOT / "app"), *sys.path]):
    api = importlib.import_module("api_helpers")
    key_manager_module = importlib.import_module("express_key_manager")
    http_options = importlib.import_module("http_options")


class UpstreamFailure(Exception):
    def __init__(self, code):
        super().__init__(f"本地模拟上游错误 {code}")
        self.code = code


class RetryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.records = [
            {"key": "local-key-a", "project_id": "local-project-a", "enabled": True},
            {"key": "local-key-b", "project_id": "local-project-b", "enabled": True},
        ]
        self.settings_patch = patch.dict(settings, {"roundrobin": True, "cross_channel_fallback": False})
        self.settings_patch.start()
        self.addCleanup(self.settings_patch.stop)
        self.keys_patch = patch.object(state, "get_express_keys", lambda: self.records, create=True)
        self.keys_patch.start()
        self.addCleanup(self.keys_patch.stop)
        self.request_obj = models.OpenAIRequest(model="gemini-4-argon", messages=[], stream=False)
        self.request = types.SimpleNamespace(
            app=types.SimpleNamespace(state=types.SimpleNamespace(
                express_key_manager=key_manager_module.ExpressKeyManager())),
            is_disconnected=AsyncMock(return_value=False),
        )
        self.used = []
        self.outcomes = []

        def build_spec(ctx, record):
            self.used.append((record["key"], record["project_id"]))
            client = types.SimpleNamespace(aio=types.SimpleNamespace(models=types.SimpleNamespace(
                generate_content=AsyncMock(side_effect=self.outcomes.pop(0)),
            )))
            return {"client": client, "model_to_call": f"projects/{record['project_id']}/model",
                    "fallback_model": None, "fallback_client_factory": None}

        self.express = types.SimpleNamespace(
            prepare_express_context=lambda request: ({"prompt_func": lambda _: [], "request_obj": request,
                                                       "gen_config_dict": {}, "prefill_text": ""}, None),
            build_key_call_spec=build_spec,
        )
        self.cookie = types.SimpleNamespace(
            check_cookie_ready=lambda: (True, "", "local-cookie-project", "", 401, ""),
            execute_once=AsyncMock(return_value=JSONResponse({"source": "cookie"})),
        )
        self.patches = [
            patch.object(api, "is_gemini_response_valid", return_value=True),
            patch.object(api, "convert_to_openai_format", return_value={"choices": [{"message": {"content": "本地回复"}}]}),
            patch.object(api, "_record_usage"), patch.object(api, "stats", Mock()),
            patch("builtins.print"),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    async def complete(self):
        return await api.execute_unified_completion(
            self.request_obj, self.request, express_upstream=self.express, cookie_upstream=self.cookie)

    async def test_限流后轮换密钥并绑定对应项目(self):
        self.outcomes = [UpstreamFailure(429), lambda **_: object()]
        response = await self.complete()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.used, [("local-key-a", "local-project-a"), ("local-key-b", "local-project-b")])

    async def test_失效密钥在当前请求中被隔离(self):
        self.outcomes = [UpstreamFailure(401), UpstreamFailure(503), lambda **_: object()]
        self.assertEqual((await self.complete()).status_code, 200)
        self.assertEqual([key for key, _ in self.used], ["local-key-a", "local-key-b", "local-key-b"])

    async def test_默认不跨通道而显式开启后允许兜底(self):
        self.outcomes = [UpstreamFailure(503)] * 3
        self.assertEqual((await self.complete()).status_code, 503)
        self.cookie.execute_once.assert_not_awaited()
        self.used.clear()
        self.outcomes = [UpstreamFailure(503)] * 2
        with patch.dict(settings, {"cross_channel_fallback": True}):
            response = await self.complete()
        self.assertEqual(json.loads(response.body)["source"], "cookie")
        self.cookie.execute_once.assert_awaited_once()

    def test_内容拦截不重试而网络与服务端故障可重试(self):
        self.assertFalse(api.is_retryable_exception(ValueError("本地模拟内容拦截")))
        self.assertTrue(api.is_retryable_exception(UpstreamFailure(500)))
        self.assertTrue(api.is_retryable_exception(ConnectionError("本地模拟网络故障")))

    def test_aiohttp选项不重复传递超时(self):
        with patch.object(http_options, "_HAS_AIOHTTP", True):
            options = http_options.get_http_options()
        self.assertEqual(options.timeout, 600_000)
        self.assertNotIn("timeout", options.async_client_args)

    async def test_流式部分产出后失败会发送错误且不重复生成(self):
        self.request_obj.stream = True
        manager = self.request.app.state.express_key_manager
        self.request.app.state.express_key_manager = types.SimpleNamespace(get_ordered_express_records=lambda: [])

        async def body():
            yield 'data: {"choices":[{"delta":{"content":"部分回复"}}]}\n\n'
            raise UpstreamFailure(503)

        self.cookie.execute_once = AsyncMock(side_effect=lambda *_: StreamingResponse(body()))
        with patch.dict(settings, {"cross_channel_fallback": True}):
            response = await self.complete()
            output = "".join([chunk async for chunk in response.body_iterator])
        self.assertIn("部分回复", output)
        self.assertIn('"error":', output)
        self.assertIn("[DONE]", output)
        self.cookie.execute_once.assert_awaited_once()
        self.request.app.state.express_key_manager = manager

    async def test_SDK观测区分首块与有效内容且中断仍有汇总(self):
        self.request_obj.stream = True
        self.request._buf = types.SimpleNamespace(id="req-local-observation", attempt_id="local-round")
        for interrupted in (False, True):
            with self.subTest(中途失败=interrupted):
                clock = [10.0]

                async def body():
                    clock[0] = 20.0
                    yield object()
                    clock[0] = 30.0
                    yield object()
                    clock[0] = 40.0
                    if interrupted:
                        raise UpstreamFailure(503)

                async def open_stream(**kwargs):
                    clock[0] = 12.0
                    return body()

                generate = AsyncMock(side_effect=open_stream)
                client = types.SimpleNamespace(aio=types.SimpleNamespace(models=types.SimpleNamespace(
                    generate_content_stream=generate)))
                spec = {"client": client, "model_to_call": "local-model", "fallback_model": None,
                        "fallback_client_factory": None}
                chunks = [
                    'data: {"choices":[{"delta":{"content":""}}]}\n\n',
                    'data: {"choices":[{"delta":{"content":"本地模拟正文"}}]}\n\n',
                ]
                with patch.object(self.express, "build_key_call_spec", return_value=spec), \
                     patch.object(api, "time", types.SimpleNamespace(time=lambda: 1000.0, monotonic=lambda: clock[0])), \
                     patch.object(api, "convert_chunk_to_openai", side_effect=chunks), \
                     patch("builtins.print") as logged:
                    response = await self.complete()
                    output = "".join([chunk async for chunk in response.body_iterator])

                logs = "\n".join(str(call.args[0]) for call in logged.call_args_list)
                generate.assert_awaited_once()
                self.assertIn("本地模拟正文", output)
                self.assertIn("[DONE]", output)
                self.assertEqual('"error":' in output, interrupted)
                self.assertIn("[SDK 调用开始]", logs)
                self.assertIn("[SDK 迭代器就绪]", logs)
                self.assertIn("首块等待=10.000秒", logs)
                self.assertIn("首块后等待=10.000秒", logs)
                self.assertIn("首次有效内容=20.000秒", logs)
                self.assertIn("首块后读取=20.000秒", logs)
                self.assertIn("SDK总耗时=30.000秒", logs)
                self.assertIn("SDK块数=2", logs)
                self.assertIn("轮次=local-round", logs)
                self.assertIn("迭代=中断或失败" if interrupted else "迭代=读完", logs)
                self.assertNotIn("本地模拟正文", logs)
                self.assertNotIn("连接建立，正在实时流式输出", logs)


if __name__ == "__main__":
    unittest.main()
