"""调用真实续传路由，以假上游验证 HTTP/SSE 行为，不依赖 Google SDK。"""

import asyncio
import importlib.util
import json
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.responses import JSONResponse, StreamingResponse
from test_resumable import ROOT, r, runtime


model_spec = importlib.util.spec_from_file_location("models", ROOT / "app" / "models.py")
models = importlib.util.module_from_spec(model_spec)
model_spec.loader.exec_module(models)
auth = types.ModuleType("auth")
auth.get_api_key = lambda: "本地测试"
helpers = types.ModuleType("api_helpers")
helpers.create_openai_error_response = lambda code, message, kind: {
    "error": {"code": code, "message": message, "type": kind}
}
helpers.extract_upstream_error = lambda error: (500, str(error))
express = types.ModuleType("upstreams.express_sdk")
express.ExpressSDKUpstream = type("FakeExpress", (), {})
cookie = types.ModuleType("upstreams.cookie_proxy")
cookie.CookieProxyUpstream = type("FakeCookie", (), {})
route_spec = importlib.util.spec_from_file_location("chat_api_under_test", ROOT / "app" / "routes" / "chat_api.py")
routes = importlib.util.module_from_spec(route_spec)
with patch.dict(sys.modules, {
    "models": models, "auth": auth, "api_helpers": helpers,
    "runtime_state": runtime, "resumable": r,
    "upstreams.express_sdk": express, "upstreams.cookie_proxy": cookie,
}):
    route_spec.loader.exec_module(routes)


async def read_response(response):
    return "".join([frame async for frame in response.body_iterator])


class RouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = r.ResumableStore()
        self.store_patch = patch.object(routes, "resumable_store", self.store)
        self.store_patch.start()
        self.request = types.SimpleNamespace(headers={}, query_params={})
        self.model = models.OpenAIRequest(model="本地测试模型", messages=[], stream=True)

    async def asyncTearDown(self):
        tasks = list(self.store._pumps.values())
        self.store.cancel_all_pumps()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)
        self.store_patch.stop()

    async def test_路由并发续传只调用一次上游(self):
        async def body():
            yield 'data: {"choices":[{"delta":{"content":"本地回复"}}]}\n\n'
            yield r.DONE_FRAME

        upstream = AsyncMock(side_effect=lambda *_: StreamingResponse(body()))
        with patch.object(routes, "_call_upstream", upstream):
            first = await routes._resumable_stream(self.request, self.model, "本地测试")
            second = await routes._resumable_stream(self.request, self.model, "本地测试")
            self.assertEqual(second.headers["X-Resumable-Replay"], "1")
            first_text, second_text = await asyncio.gather(read_response(first), read_response(second))
        self.assertEqual(upstream.await_count, 1)
        self.assertEqual(first_text, second_text)
        self.assertIn("本地回复", first_text)
        self.assertIn("text/event-stream", first.headers["content-type"])

    async def test_流式超时后同样请求可重新生成(self):
        async def hang():
            await asyncio.Event().wait()
            yield r.DONE_FRAME

        async def success():
            yield 'data: {"ok":true}\n\n'
            yield r.DONE_FRAME

        upstream = AsyncMock(side_effect=[StreamingResponse(hang()), StreamingResponse(success())])
        with patch.object(r, "upstream_timeout_seconds", return_value=0.03), patch.object(routes, "_call_upstream", upstream):
            first = await routes._resumable_stream(self.request, self.model, "本地测试")
            text = await asyncio.wait_for(read_response(first), 1)
            self.assertIn('"code": 504', text)
            self.assertIn("[DONE]", text)
            second = await routes._resumable_stream(self.request, self.model, "本地测试")
            self.assertIn('"ok":true', await read_response(second))
        self.assertEqual(upstream.await_count, 2)

    async def test_上游建流阶段超时返回明确的HTTP错误(self):
        async def hang(*_):
            await asyncio.Event().wait()

        with patch.object(r, "upstream_timeout_seconds", return_value=0.03), patch.object(routes, "_call_upstream", side_effect=hang):
            response = await asyncio.wait_for(routes._resumable_stream(self.request, self.model, "本地测试"), 1)
        self.assertEqual(response.status_code, 504)
        self.assertEqual(json.loads(response.body)["error"]["type"], "timeout")

    async def test_非流式超时后重试可获得正常响应(self):
        self.model.stream = False

        async def hang(*_):
            await asyncio.Event().wait()

        with patch.object(r, "upstream_timeout_seconds", return_value=0.03), patch.object(routes, "_call_upstream", side_effect=hang):
            failed = await asyncio.wait_for(routes._resumable_json(self.request, self.model, "本地测试"), 1)
        self.assertEqual(failed.status_code, 504)
        self.assertEqual(json.loads(failed.body)["error"]["code"], 504)
        with patch.object(routes, "_call_upstream", AsyncMock(return_value=JSONResponse({"ok": True}))):
            response = await routes._resumable_json(self.request, self.model, "本地测试")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body), {"ok": True})

    def test_续传请求头从下一事件开始而查询参数是包含起点(self):
        self.request.headers = {"last-event-id": "3"}
        self.request.query_params = {"resume_from": "1"}
        self.assertEqual(routes._resume_start_seq(self.request), 4)
        self.request.headers = {"last-event-id": "无效"}
        self.assertEqual(routes._resume_start_seq(self.request), 1)

    async def test_非流式客户端断开后在宽限期结束时取消任务(self):
        self.model.stream = False
        self.request.is_disconnected = AsyncMock(return_value=True)

        async def hang(*_):
            await asyncio.Event().wait()

        with patch.object(r, "CANCEL_GRACE_SECONDS", 0.01), patch.object(routes, "_call_upstream", side_effect=hang):
            response = await asyncio.wait_for(routes._resumable_json(self.request, self.model, "本地测试"), 1)
            buf = next(iter(self.store._buffers.values()))
            await asyncio.wait_for(buf._abandon_task, 1)
        self.assertEqual(response.status_code, 499)
        self.assertEqual(buf.status_code, 499)

    async def test_非流式进入等待前已到截止时间也能及时结束(self):
        self.model.stream = False
        original_init = r.ResumableBuffer.__init__

        def expired_init(buf, *args, **kwargs):
            original_init(buf, *args, **kwargs)
            buf._deadline = r.time.monotonic() - 1

        with patch.object(r.ResumableBuffer, "__init__", expired_init), \
             patch.object(routes, "_call_upstream", AsyncMock(return_value=JSONResponse({"ok": True}))):
            response = await asyncio.wait_for(routes._resumable_json(self.request, self.model, "本地测试"), 1)
        self.assertEqual(response.status_code, 504)

    async def test_失败轮询不会标记为成功交付(self):
        buf, _ = await self.store.create_if_absent("poll-error")
        await buf.fail("本地模拟故障", 503, "upstream_error")
        response = await routes.resumable_poll(buf.id, self.request, "本地测试", 0, 200, 0)
        self.assertTrue(json.loads(response.body)["done"])
        self.assertFalse(buf.fully_delivered)

    async def test_非流式日志区分等待重放及断线后后台落缓冲(self):
        self.model.stream = False
        entered = asyncio.Event()
        release = asyncio.Event()

        async def upstream(*_):
            entered.set()
            await release.wait()
            return JSONResponse({"content": "不应出现在日志的回复"})

        with patch("builtins.print") as output, patch.object(routes, "_call_upstream", side_effect=upstream) as call:
            first = asyncio.create_task(routes._resumable_json(self.request, self.model, "不应输出的密钥"))
            await entered.wait()
            second = asyncio.create_task(routes._resumable_json(self.request, self.model, "不应输出的密钥"))
            await asyncio.sleep(0)
            logs = "\n".join(item.args[0] for item in output.call_args_list)
            self.assertIn("等待同一轮后台任务", logs)
            self.assertNotIn("[响应落缓冲]", logs)
            first.cancel()
            second.cancel()
            await asyncio.gather(first, second, return_exceptions=True)
            release.set()
            await asyncio.gather(*list(self.store._pumps.values()))
            logs = "\n".join(item.args[0] for item in output.call_args_list)
            self.assertIn("[响应落缓冲]", logs)
            self.assertNotIn("[下游响应返回]", logs)
            replay = await routes._resumable_json(self.request, self.model, "不应输出的密钥")
        logs = "\n".join(item.args[0] for item in output.call_args_list)
        self.assertEqual(call.await_count, 1)
        self.assertEqual(logs.count("[请求开始]"), 1)
        self.assertEqual(logs.count("[响应落缓冲]"), 1)
        self.assertIn("重放完整回复", logs)
        self.assertIn("[下游响应返回]", logs)
        self.assertNotIn("不应输出的密钥", logs)
        self.assertNotIn("不应出现在日志的回复", logs)
        self.assertEqual(json.loads(replay.body)["content"], "不应出现在日志的回复")


if __name__ == "__main__":
    unittest.main()
