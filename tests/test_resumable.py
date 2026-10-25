"""续传生命周期回归测试：只使用假上游，不读取密钥或发送模型请求。"""

import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import time
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
DEFAULTS = {
    "resumable_enabled": True,
    "resumable_ttl_seconds": 1800,
    "resumable_max_streams": 200,
    "resumable_upstream_timeout_seconds": 900,
}
config = types.ModuleType("config")
config.DEFAULT_SETTINGS = DEFAULTS
runtime = types.ModuleType("runtime_state")
runtime.app_state = types.SimpleNamespace(get_setting=lambda key, default=None: DEFAULTS.get(key, default))
spec = importlib.util.spec_from_file_location("resumable_under_test", ROOT / "app" / "resumable.py")
r = importlib.util.module_from_spec(spec)
with patch.dict(sys.modules, {"config": config, "runtime_state": runtime}):
    spec.loader.exec_module(r)


async def collect(buf, start=0):
    return "".join([frame async for frame in r.sse_from_buffer(buf, start)])


class ResumableTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = r.ResumableStore()
        self.tasks = []

    async def asyncTearDown(self):
        self.store.cancel_all_pumps()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await asyncio.sleep(0)

    def start(self, buf, operation):
        task = asyncio.create_task(operation)
        self.tasks.append(task)
        self.store.register_pump(buf.id, task, expected=buf)
        return task

    async def test_正常并发请求只创建一份缓冲并保持事件序号(self):
        results = await asyncio.gather(*[self.store.create_if_absent("same") for _ in range(12)])
        self.assertEqual(sum(is_new for _, is_new in results), 1)
        buf = results[0][0]
        self.assertTrue(all(other is buf for other, _ in results))

        async def upstream():
            yield 'data: {"text":"本地测试"}\n\n'
            yield r.DONE_FRAME

        self.start(buf, r.pump_stream(buf, upstream()))
        first, second = await asyncio.gather(collect(buf), collect(buf))
        self.assertEqual(first, second)
        self.assertIn("id: 0\ndata:", first)
        self.assertIn("id: 1\ndata: [DONE]", first)
        self.assertEqual(buf.status, "done")

    async def test_断线不取消后台读取且可从正确序号续传(self):
        buf, _ = await self.store.create_if_absent("disconnect")
        gate = asyncio.Event()

        async def upstream():
            yield 'data: {"choices":[{"delta":{"content":"第一部分"}}]}\n\n'
            await gate.wait()
            yield 'data: {"choices":[{"delta":{"content":"第二部分"}}]}\n\n'

        pump = self.start(buf, r.pump_stream(buf, upstream()))
        reader = r.sse_from_buffer(buf)
        await anext(reader)  # 首个注释保活。
        self.assertIn("id: 0", await anext(reader))
        await reader.aclose()
        self.assertFalse(pump.done())
        self.assertFalse(buf.fully_delivered)
        gate.set()
        await pump
        resumed = await collect(buf, 1)
        self.assertTrue(buf.has_model_output)
        self.assertIsNone(buf._abandon_task)
        self.assertNotIn("第一部分", resumed)
        self.assertIn("第二部分", resumed)
        self.assertIn("id: 2\ndata: [DONE]", resumed)

    async def test_上游一直不返回时读者收到超时错误和结束帧(self):
        buf, _ = await self.store.create_if_absent("timeout")
        buf._deadline = time.monotonic() + 0.03
        closed = asyncio.Event()

        async def upstream():
            try:
                yield ": keep-alive\n\n"
                await asyncio.Event().wait()
            finally:
                closed.set()

        pump = self.start(buf, r.pump_stream(buf, upstream()))
        output = await asyncio.wait_for(collect(buf), 1)
        await asyncio.gather(pump, return_exceptions=True)
        self.assertEqual(buf.status, "error")
        self.assertEqual(buf.status_code, 504)
        self.assertIn('"type": "timeout"', output)
        self.assertIn("data: [DONE]", output)
        self.assertTrue(closed.is_set())

    async def test_续传和保活都不延长原截止时间(self):
        buf, _ = await self.store.create_if_absent("deadline")
        deadline = buf._deadline
        reader = r.sse_from_buffer(buf)
        with patch.object(r, "HEARTBEAT_INTERVAL_SECONDS", 0.001):
            self.assertTrue((await anext(reader)).startswith(":"))
            self.assertTrue((await anext(reader)).startswith(":"))
        await reader.aclose()
        same, created = await self.store.create_if_absent(buf.id)
        self.assertIs(same, buf)
        self.assertFalse(created)
        self.assertEqual(buf._deadline, deadline)
        self.assertEqual(buf.events, [])
        self.assertEqual(buf.readers, 0)

    async def test_清理器即使有活动读者也结束超时任务(self):
        buf, _ = await self.store.create_if_absent("sweep")
        iterator = buf.iter_from()
        reader = asyncio.create_task(anext(iterator))
        self.tasks.append(reader)
        await asyncio.sleep(0)
        self.assertEqual(buf.readers, 1)
        buf._deadline = time.monotonic() - 1
        await self.store.sweep()
        _, frame = await asyncio.wait_for(reader, 1)
        self.assertIn('"code": 504', frame)
        self.assertEqual(buf.status, "error")
        await iterator.aclose()

    async def test_超时后并发重试只产生一个新任务(self):
        old, _ = await self.store.create_if_absent("retry")
        old._deadline = time.monotonic() - 1
        results = await asyncio.gather(*[self.store.create_if_absent("retry") for _ in range(12)])
        self.assertEqual(sum(created for _, created in results), 1)
        new = results[0][0]
        self.assertIsNot(new, old)
        self.assertNotEqual(new.attempt_id, old.attempt_id)
        self.assertTrue(all(buf is new for buf, _ in results))
        self.assertEqual(old.status, "error")
        self.assertIn("[DONE]", old.events[-1])

    async def test_旧任务回调和撤销不能删除新任务(self):
        old, _ = await self.store.create_if_absent("replace")
        old_task = self.start(old, asyncio.Event().wait())
        await asyncio.sleep(0)
        old._deadline = time.monotonic() - 1
        new, created = await self.store.create_if_absent("replace")
        self.assertTrue(created)
        new_task = self.start(new, asyncio.Event().wait())
        await asyncio.gather(old_task, return_exceptions=True)
        await asyncio.sleep(0)
        self.assertIs(self.store._pumps["replace"], new_task)
        self.assertFalse(await self.store.drop("replace", expected=old))
        self.assertIs(await self.store.get("replace"), new)

    async def test_非流式超时同样唤醒等待者并保留错误(self):
        buf, _ = await self.store.create_if_absent("json-timeout", kind="json")
        buf._deadline = time.monotonic() + 0.03

        async def operation():
            try:
                await buf.await_upstream(asyncio.Event().wait())
            except r.ResumableTimeoutError:
                return

        self.start(buf, operation())
        await asyncio.wait_for(buf.wait_for(2 ** 62, 1), 1)
        self.assertEqual(buf.status_code, 504)
        self.assertEqual(buf.payload["error"]["type"], "timeout")
        await buf.finish("done", payload={"late": True})
        self.assertEqual(buf.status, "error")
        self.assertNotIn("late", buf.payload)

    async def test_正常非流式调用不被改写(self):
        buf, _ = await self.store.create_if_absent("json-success", kind="json")
        result = await buf.await_upstream(asyncio.sleep(0, result={"ok": True}))
        await buf.finish("done", payload=result, status_code=200)
        await buf.wait_for(2 ** 62, 1)
        self.assertEqual(buf.payload, {"ok": True})
        self.assertEqual(buf.status_code, 200)

    async def test_底层先超时也必须结束缓冲(self):
        buf, _ = await self.store.create_if_absent("early-timeout", kind="json")

        async def upstream():
            raise TimeoutError("本地模拟读取超时")

        with self.assertRaises(r.ResumableTimeoutError):
            await buf.await_upstream(upstream())
        self.assertGreater(buf.remaining_timeout(), 0)
        self.assertEqual(buf.status, "error")
        self.assertEqual(buf.status_code, 504)
        self.assertEqual(buf.payload["error"]["type"], "timeout")

    async def test_取消任务会发出错误而非静默断流(self):
        buf, _ = await self.store.create_if_absent("cancel")
        entered = asyncio.Event()

        async def upstream():
            entered.set()
            await asyncio.Event().wait()
            yield r.DONE_FRAME

        pump = self.start(buf, r.pump_stream(buf, upstream()))
        await entered.wait()
        pump.cancel()
        await asyncio.gather(pump, return_exceptions=True)
        output = await collect(buf)
        self.assertIn('"code": 499', output)
        self.assertIn("[DONE]", output)

    async def test_生成失败可重试而完成未交付的回复仍可重放(self):
        failed, _ = await self.store.create_if_absent("error")

        async def upstream():
            raise ValueError("本地模拟错误")
            yield r.DONE_FRAME

        await r.pump_stream(failed, upstream())
        new, created = await self.store.create_if_absent("error")
        self.assertTrue(created)
        self.assertIsNot(new, failed)
        good, _ = await self.store.create_if_absent("good")
        await good.append('data: {"choices":[{"delta":{"content":"正常回复"}}]}\n\n', is_model_output=True)
        await good.append(r.DONE_FRAME)
        await good.finish()
        same, created = await self.store.create_if_absent("good")
        self.assertFalse(created)
        self.assertIs(same, good)
        await collect(good)
        replacement, created = await self.store.create_if_absent("good")
        self.assertTrue(created)
        self.assertIsNot(replacement, good)

    async def test_保活与角色空帧不计为有效产出且不污染序号(self):
        buf, _ = await self.store.create_if_absent("heartbeat")

        async def upstream():
            yield ": keep-alive\n\n"
            yield r.make_keepalive_sse("本地模型")
            yield 'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
            yield r.DONE_FRAME

        await r.pump_stream(buf, upstream())
        self.assertFalse(buf.has_model_output)
        self.assertEqual(buf.next_seq, 2)
        self.assertTrue(buf.events[0].startswith("id: 0\n"))
        self.assertNotIn("chatcmpl-keepalive", "".join(buf.events))
        new, created = await self.store.create_if_absent(buf.id)
        self.assertTrue(created)
        self.assertIsNot(new, buf)

    async def test_错误流不会标记成功交付且允许重新生成(self):
        buf, _ = await self.store.create_if_absent("error-frame")

        async def upstream():
            yield 'data: {"choices":[{"delta":{"content":"部分回复"}}]}\n\n'
            yield 'data: {"error":{"code":503,"message":"本地模拟故障"}}\n\n'
            yield r.DONE_FRAME

        await r.pump_stream(buf, upstream())
        with patch("builtins.print") as output:
            self.assertIn("部分回复", await collect(buf))
        self.assertIn("终止帧=已输出", "\n".join(item.args[0] for item in output.call_args_list))
        self.assertEqual(buf.status, "error")
        self.assertFalse(buf.fully_delivered)
        _, created = await self.store.create_if_absent(buf.id)
        self.assertTrue(created)

    async def test_首包前断连在宽限期后取消上游(self):
        buf, _ = await self.store.create_if_absent("abandon")
        pump = self.start(buf, r.pump_stream(buf, asyncio_waiting_stream()))
        reader = r.sse_from_buffer(buf)
        await anext(reader)
        with patch.object(r, "CANCEL_GRACE_SECONDS", 0.01):
            await reader.aclose()
            await asyncio.wait_for(buf._abandon_task, 1)
        await asyncio.gather(pump, return_exceptions=True)
        self.assertEqual(buf.status_code, 499)
        self.assertTrue(pump.cancelled())

    async def test_首包前及时重连复用原任务并取消撤销计时(self):
        buf, _ = await self.store.create_if_absent("rejoin")
        pump = self.start(buf, r.pump_stream(buf, asyncio_waiting_stream()))
        first = r.sse_from_buffer(buf)
        await anext(first)
        await first.aclose()
        abandoned = buf._abandon_task
        detached = r.DetachedRequest(types.SimpleNamespace(is_disconnected=lambda: None), buf)
        self.assertFalse(await detached.is_disconnected())
        same, created = await self.store.create_if_absent(buf.id)
        second = r.sse_from_buffer(same)
        await anext(second)
        await asyncio.gather(abandoned, return_exceptions=True)
        self.assertFalse(created)
        self.assertIs(same, buf)
        self.assertIsNone(buf._abandon_task)
        self.assertFalse(pump.done())
        await buf.fail("本地测试结束", 499, "client_closed_request")
        await second.aclose()

    async def test_假流式下游保活不写入缓冲(self):
        buf, _ = await self.store.create_if_absent("fake-heartbeat")
        reader = r.sse_from_buffer(buf)
        with patch.dict(DEFAULTS, {"fake_streaming": True, "fake_streaming_interval": 0.5}):
            self.assertTrue((await anext(reader)).startswith(":"))
            self.assertIn("chatcmpl-keepalive", await asyncio.wait_for(anext(reader), 1))
        self.assertEqual(buf.events, [])
        await buf.fail("本地测试结束", 499, "client_closed_request")
        await reader.aclose()

    def test_无效超时配置回退为十五分钟(self):
        for value in [None, "无效", 0, -1, float("nan"), float("inf")]:
            with patch.object(r.app_state, "get_setting", return_value=value):
                self.assertEqual(r.upstream_timeout_seconds(), 900)
        with patch.object(r.app_state, "get_setting", return_value=1800):
            self.assertEqual(r.upstream_timeout_seconds(), 1800)

    async def test_日志区分接收完成和下游发送且不包含回复正文(self):
        buf, _ = await self.store.create_if_absent("log-stream")

        async def upstream():
            yield 'data: {"content":"不应输出的聊天正文"}\n\n'
            yield r.DONE_FRAME

        with patch("builtins.print") as output:
            await r.pump_stream(buf, upstream())
            await collect(buf)
        logs = "\n".join(call.args[0] for call in output.call_args_list)
        self.assertEqual(logs.count("[首个上游事件]"), 1)
        self.assertEqual(logs.count("[响应落缓冲]"), 1)
        self.assertEqual(logs.count("[下游发送结束]"), 1)
        self.assertLess(logs.index("[响应落缓冲]"), logs.index("[下游发送结束]"))
        self.assertIn(f"轮次={buf.attempt_id}", logs)
        self.assertIn("事件=2", logs)
        self.assertIn("不代表客户端已显示", logs)
        self.assertNotIn("不应输出的聊天正文", logs)

    def test_无事件等待日志限频且有数据时不误报(self):
        with patch.object(r.time, "monotonic", return_value=100):
            buf = r.ResumableBuffer("log-wait")
        with patch("builtins.print") as output:
            for now in (159, 160, 170):
                with patch.object(r.time, "monotonic", return_value=now):
                    buf.log_wait_if_due()
            buf._last_event_at = 200
            for now in (220, 261, 270):
                with patch.object(r.time, "monotonic", return_value=now):
                    buf.log_wait_if_due()
            buf.status = "done"
            with patch.object(r.time, "monotonic", return_value=400):
                buf.log_wait_if_due()
        logs = "\n".join(call.args[0] for call in output.call_args_list)
        self.assertEqual(output.call_count, 2)
        self.assertIn("无事件=60.0秒", logs)
        self.assertIn("无事件=61.0秒", logs)
        self.assertIn("阶段=等待上游响应", logs)

    async def test_异常日志不输出可能含凭据的异常正文(self):
        buf, _ = await self.store.create_if_absent("log-error")

        async def upstream():
            raise ValueError("Authorization: Bearer 不应输出的密钥")
            yield r.DONE_FRAME

        with patch("builtins.print") as output:
            await r.pump_stream(buf, upstream())
        logs = "\n".join(call.args[0] for call in output.call_args_list)
        self.assertIn("异常类型=ValueError", logs)
        self.assertIn("HTTP 500", logs)
        self.assertNotIn("不应输出的密钥", logs)

    async def test_日志输出失败不影响错误终止和取消后台任务(self):
        buf, _ = await self.store.create_if_absent("log-unavailable")
        task = self.start(buf, asyncio.Event().wait())
        await asyncio.sleep(0)
        with patch("builtins.print", side_effect=OSError("本地模拟终端关闭")):
            self.assertTrue(await buf.fail("本地测试超时"))
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assertEqual(buf.status_code, 504)
        self.assertTrue(buf.events[-1].strip().endswith("[DONE]"))


async def asyncio_waiting_stream():
    await asyncio.Event().wait()
    yield r.DONE_FRAME


if __name__ == "__main__":
    unittest.main()
