"""使用假凭据和假 SDK 调用验证统一调度器，不加载部署配置或发送网络请求。"""

import asyncio
import base64
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
        modules_patch = patch.dict(sys.modules, {"config": config, "runtime_state": runtime, "resumable": r})
        modules_patch.start()
        self.addCleanup(modules_patch.stop)
        path_patch = patch.object(sys, "path", [str(ROOT / "app"), *sys.path])
        path_patch.start()
        self.addCleanup(path_patch.stop)
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

    def test_图片历史恢复真实签名及缓存缺失时的哨兵(self):
        import message_processing as messages
        from signature_store import ImageSignatureStore, SKIP_VALIDATOR_SENTINEL
        image = b"local-image-bytes"
        signature = b"local-image-signature"
        markdown = "![Image](data:image/png;base64," + base64.b64encode(image).decode() + ")"
        store = ImageSignatureStore()
        history = [models.OpenAIMessage(role="assistant", content=markdown),
                   models.OpenAIMessage(role="user", content="继续修改图片")]
        with patch.object(messages, "image_signature_store", store), \
             patch.object(messages, "optimize_image_bytes", side_effect=lambda data, mime: (data, mime)):
            for cached in (True, False):
                with self.subTest(缓存命中=cached):
                    store.clear()
                    if cached:
                        store.put(image, signature)
                    prompt = messages.create_gemini_prompt(history, model_name="gemini-3.1-flash-image")
                    restored = next(part for content in prompt if content.role == "model"
                                    for part in content.parts if part.inline_data is not None)
                    self.assertEqual(restored.thought_signature, signature if cached else SKIP_VALIDATOR_SENTINEL)

    def test_模型图片保留签名字节而用户图片不附签名(self):
        import message_processing as messages
        from signature_store import ImageSignatureStore
        original, compressed, signature = b"local-original-image", b"local-compressed-image", b"local-signature"
        encoded = base64.b64encode(original).decode()
        store = ImageSignatureStore()
        store.put(original, signature)
        variants = ["![Image](data:image/png;base64," + encoded + ")",
                    [{"type": "image_url", "image_url": {"url": "data:image/png;base64," + encoded}}]]
        for content in variants:
            for role in ("assistant", "user"):
                with self.subTest(格式=type(content).__name__, 角色=role), \
                     patch.object(messages, "image_signature_store", store), \
                     patch.object(messages, "optimize_image_bytes", return_value=(compressed, "image/jpeg")) as optimize:
                    extra = {"google": {"thought_signature": base64.b64encode(signature).decode()}}
                    prompt = messages.create_gemini_prompt(
                        [models.OpenAIMessage(role=role, content=content, extra_content=extra)],
                        model_name="gemini-3.1-flash-image")
                    part = next(p for item in prompt for p in item.parts if p.inline_data is not None)
                    self.assertEqual(part.inline_data.data, original if role == "assistant" else compressed)
                    self.assertEqual(part.thought_signature, signature if role == "assistant" else None)
                    if role == "assistant":
                        optimize.assert_not_called()

    def test_抗截断流式与非流式保留代码表格及合法标签(self):
        from synthetic_wrapper import extract_synthetic_content
        from message_processing import FunctionCallStreamAssembler
        samples = [
            '```python\nfoo({"a": [1]})\n)\n)\n```\n',
            '| 标题 | 数值 |\n| --- | --- |\n| 示例 | 1 |\n',
            '<div><div><span>正文</span></div></div>',
            '```json\n{"content": "业务字段", "items": [1]}\n```',
        ]
        prefix = "".join(f"第{i}项说明。" for i in range(25)) + "\n"
        for sample in samples:
            text = prefix + sample
            with self.subTest(正文=sample):
                self.assertEqual(extract_synthetic_content({"content": text}), text)
                assembler = FunctionCallStreamAssembler("compose_reply")
                output = []
                for pos in range(0, len(text), 7):
                    _, delta, _ = assembler.feed_with_synthetic({
                        "name": "compose_reply", "args": {"content": text[pos:pos + 7]},
                        "willContinue": pos + 7 < len(text),
                    })
                    output.append(delta)
                output.append(assembler.flush_synthetic_tail())
                self.assertEqual("".join(output), text)
                self.assertFalse(assembler.synthetic_halted)

    def test_预填充词法继承及思维链守卫不误认正文标签(self):
        from synthetic_wrapper import extract_synthetic_content
        from message_processing import detect_unclosed_cot_tag
        self.assertEqual(extract_synthetic_content({"content": "正文</span></div>"},
                                                 prefill_text="<div><span>"), "正文</span></div>")
        self.assertIsNone(detect_unclosed_cot_tag("<div><style>"))
        self.assertIsNone(detect_unclosed_cot_tag("```python\nif a < b:\n    pass\n```"))
        self.assertEqual(detect_unclosed_cot_tag("<thinking>推理"), "thinking")
        self.assertEqual(extract_synthetic_content({"content": '正文"}'}), "正文")

    def test_工具历史不猜配陌生ID且保留重复结果(self):
        from message_processing import sanitize_orphan_tool_turns
        call = models.OpenAIMessage(role="assistant", tool_calls=[{
            "id": "call-a", "type": "function", "function": {"name": "search", "arguments": "{}"}}])
        for result_id in ("call-b", "call-a"):
            with self.subTest(结果ID=result_id):
                history = [call]
                if result_id == "call-a":
                    history.append(models.OpenAIMessage(role="tool", tool_call_id="call-a", content="正确结果"))
                history.append(models.OpenAIMessage(role="tool", tool_call_id=result_id, content="多余结果"))
                snapshot = [m.model_dump() for m in history]
                cleaned = sanitize_orphan_tool_turns(history)
                tools = [m for m in cleaned if m.role == "tool"]
                self.assertEqual(len(tools), 1)
                self.assertEqual(tools[0].tool_call_id, "call-a")
                self.assertNotIn("多余结果", tools[0].content)
                self.assertTrue(any(m.role == "user" and "多余结果" in m.content for m in cleaned))
                self.assertEqual([m.model_dump() for m in history], snapshot)

    def test_合成传输默认关闭且与真实工具调用区分(self):
        from synthetic_wrapper import create_sandbox_spec, inject_express_gen_config
        request = models.OpenAIRequest(model="gemini-4-carbon", messages=[], stream=True)
        spec = create_sandbox_spec(request.model, request, {})
        original = {"temperature": 0.5}
        inject_express_gen_config(original, spec)
        self.assertFalse(spec.enabled)
        self.assertEqual(original, {"temperature": 0.5})
        enabled = create_sandbox_spec(request.model, request, {"synthetic_sandbox_enabled": True})
        self.assertTrue(enabled.enabled)
        inject_express_gen_config(original, enabled)
        self.assertEqual(original["tool_config"]["function_calling_config"]["mode"], "ANY")
        for name, arguments in ((enabled.emit_tool_name, {"content": "本地模拟正文"}),
                                ("local_real_tool", {"city": "本地测试"})):
            with self.subTest(工具=name):
                chunk = api.types.GenerateContentResponse(candidates=[api.types.Candidate(
                    content=api.types.Content(role="model", parts=[api.types.Part(
                        function_call=api.types.FunctionCall(name=name, args=arguments))]),
                    finish_reason="STOP")])
                output = api.convert_chunk_to_openai(chunk, request.model, "local-response",
                                                     indexer=api.ToolCallIndexer(enabled.emit_tool_name))
                delta = json.loads(output[5:])["choices"][0]["delta"]
                if name == enabled.emit_tool_name:
                    self.assertEqual(delta["content"], "本地模拟正文")
                    self.assertNotIn("tool_calls", delta)
                else:
                    self.assertEqual(delta["tool_calls"][0]["function"]["name"], name)

    def test_双通道原始采样必须显式开启调试(self):
        with patch.dict(sys.modules, {"config": config, "runtime_state": runtime}), \
             patch.object(sys, "path", [str(ROOT / "app"), *sys.path]):
            cookie = importlib.import_module("upstreams.cookie_proxy")
        for sampler_class in (api._RawSampler, cookie._RawSampler):
            for debug in (False, True):
                with self.subTest(通道=sampler_class.__module__, 调试=debug), \
                     patch.dict(settings, {"debug_outbound": debug}):
                    sampler = sampler_class()
                    sampler.add({"text": "不应默认记录的私有正文"})
                    self.assertEqual(sampler.count, 1 if debug else 0)
                    self.assertEqual("私有正文" in sampler.dump(), debug)

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

    async def test_短合成尾段及普通响应的结束帧顺序(self):
        self.request_obj.stream = True
        for synthetic, reason in ((True, None), (False, "MAX_TOKENS"), (False, None)):
            with self.subTest(合成=synthetic, 结束原因=reason):
                part = (api.types.Part(function_call=api.types.FunctionCall(
                    name="compose_reply", args={"content": "短正文"}, will_continue=True)) if synthetic
                    else api.types.Part(text="短正文"))
                chunk = api.types.GenerateContentResponse(candidates=[api.types.Candidate(
                    content=api.types.Content(role="model", parts=[part]), finish_reason=reason)])

                async def body():
                    yield chunk

                generate = AsyncMock(return_value=body())
                client = types.SimpleNamespace(aio=types.SimpleNamespace(models=types.SimpleNamespace(
                    generate_content_stream=generate)))
                context = {"prompt_func": lambda _: [], "request_obj": self.request_obj,
                           "gen_config_dict": {}, "prefill_text": "",
                           "synthetic_tool_name": "compose_reply" if synthetic else ""}
                spec = {"client": client, "model_to_call": "local-model", "fallback_model": None,
                        "fallback_client_factory": None}
                with patch.object(self.express, "build_key_call_spec", return_value=spec), \
                     patch.object(self.express, "prepare_express_context", return_value=(context, None)):
                    response = await self.complete()
                    frames = [frame async for frame in response.body_iterator]
                deltas = [json.loads(frame[5:])["choices"][0] for frame in frames
                          if frame.startswith("data:") and "[DONE]" not in frame]
                reasons = [item["finish_reason"] for item in deltas if item["finish_reason"]]
                self.assertEqual(reasons, ["length" if reason == "MAX_TOKENS" else "stop"])
                self.assertEqual("".join(item["delta"].get("content", "") for item in deltas), "短正文")
                self.assertIn("[DONE]", frames[-1])
                generate.assert_awaited_once()

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
