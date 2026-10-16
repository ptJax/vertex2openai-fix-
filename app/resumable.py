"""弱网可靠传输：服务端响应缓冲 + 序列号事件 + 断线重放 / 分片轮询拉取。

问题背景
--------
反代 → Google 的链路是好的，Google 的回复能完整到达反代；坏的是**前端 → 反代**这一段
（延迟抖动、丢包、连接中断）。原实现把上游的读取与前端连接绑死：
  - 前端连接一断，`fastapi_request.is_disconnected()` 变真，上游调用/重试被主动放弃；
  - 已经产出的增量只存在于那条 TCP 连接里，断了就永远找不回来，前端只能整轮重发
    （重新烧一次上游配额，且长回复在弱网下几乎不可能一次跑完）。

方案（响应缓冲、断线续传与有界等待）
--------------------------------------
1. **响应落缓冲**：每个请求在服务端建一个缓冲区，上游产出的每个 SSE 帧按到达顺序
   追加进去并分配**单调递增的序列号 seq**。上游的读取由一个**后台任务**驱动，
   与前端连接解耦——前端断开不会直接取消它，后台持续接收直到完成或超时。
2. **稳定请求 ID**：缓冲区的 id 优先取客户端请求头（X-Request-Id / Idempotency-Key），
   否则由「API Key + 请求体」哈希而来，因此**同一请求重发即命中同一缓冲区**。
3. **断线续传**：
   - SSE 路径：每帧前带 `id: <seq>`，重连时用 `Last-Event-ID` 或 `?resume_from=` 续取；
   - 轮询路径：`GET /v1/resumable/{stream_id}?from=<seq>&limit=&wait=` 拿一段分片（短连接，天生抗抖动），
     响应里带 `next` 游标与 `done` 标志，客户端按 seq 去重、按 done 判完成。
4. **幂等重放**：普通前端（酒馆等）在网络失败后会原样重发同一请求。此时若缓冲区里
   那次回复**还没被完整交付过**，就直接从 seq=0 重放已缓冲内容并继续跟随上游，
   **不再打一次 Google**；已经完整交付过的，才当成一次新生成。
5. **生命周期与容量**：缓冲区在完成后保留 `resumable_ttl_seconds`，总数超过
   `resumable_max_streams` 时淘汰最旧的已完成缓冲；所有状态变更在锁/条件变量下进行。
6. **有界等待**：后台生成受整轮总时限约束，重连不重置计时；超时写入错误与终止帧。
   下游注释保活不写入缓冲、不占事件序号，也不延长生成时限。

为什么这能满足「Google 的回复到了反代，前端就一定能拿到完整一致的回复」
-----------------------------------------------------------------------
回复的权威副本在服务端缓冲区里，且是**带序号的有序事件流**；前端连接只是这份副本的
一个游标读者。抖动 → 读者慢一点，数据仍在缓冲里等它；断线 → 换一条连接从 `next`
继续读，语义上等价于文件断点续传；前端整轮重发 → 命中同一缓冲区，得到同一份内容而
不是新的一次生成。因此最终交付的内容与服务端收到的内容逐字节一致。
"""

import asyncio
import hashlib
import json
import math
import time
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import config as app_config
from runtime_state import app_state

# 终止帧：与 OpenAI 流一致，客户端见到它即可判定「本轮完整结束」
DONE_FRAME = "data: [DONE]\n\n"
HEARTBEAT_INTERVAL_SECONDS = 15.0
WAIT_LOG_INTERVAL_SECONDS = 60.0


class ResumableTimeoutError(TimeoutError):
    """后台生成超过整轮时限。"""


def upstream_timeout_seconds() -> float:
    """无效配置使用安全默认值，避免无限等待。"""
    try:
        value = float(_setting("resumable_upstream_timeout_seconds"))
        if math.isfinite(value) and value > 0:
            return value
    except (TypeError, ValueError):
        pass
    return 900.0


def _setting(key: str) -> Any:
    try:
        return app_state.get_setting(key, app_config.DEFAULT_SETTINGS.get(key))
    except Exception:
        return app_config.DEFAULT_SETTINGS.get(key)


def is_enabled() -> bool:
    return bool(_setting("resumable_enabled"))


def ttl_seconds() -> float:
    try:
        return max(30.0, float(_setting("resumable_ttl_seconds")))
    except (TypeError, ValueError):
        return float(app_config.DEFAULT_SETTINGS["resumable_ttl_seconds"])


def max_streams() -> int:
    try:
        return max(1, int(_setting("resumable_max_streams")))
    except (TypeError, ValueError):
        return int(app_config.DEFAULT_SETTINGS["resumable_max_streams"])


def compute_stream_id(headers: Any, api_key: str, fingerprint: str) -> Tuple[str, str]:
    """决定这次请求的缓冲区 ID。返回 (id, 来源说明)。

    优先用客户端显式给的幂等键——聪明的客户端可以自己控制"哪两次请求算同一次"；
    没有就用「API Key + 请求体」哈希，让**原样重发**天然命中同一缓冲区。
    """
    for h in ("x-resume-id", "x-request-id", "x-idempotency-key", "idempotency-key"):
        try:
            v = (headers.get(h) or "").strip()
        except Exception:
            v = ""
        if v:
            # 幂等键只做“同一用户 + 同一请求体”之内的显式身份：混入 API Key 与请求指纹，
            # 避免不同用户复用同一键值时串缓冲（A 的回复被 B 读到），也避免同一键配不同
            # 请求体时误重放。客户端原样重发（同键 + 同体）仍稳定命中同一缓冲。
            digest = hashlib.sha256(f"{api_key}\x00{v}\x00{fingerprint}".encode("utf-8")).hexdigest()[:32]
            return f"cid-{digest}", f"客户端请求头 {h}"
    digest = hashlib.sha256(f"{api_key}\x00{fingerprint}".encode("utf-8")).hexdigest()[:32]
    return f"req-{digest}", "请求体指纹"


class ResumableBuffer:
    """一次回复的服务端权威副本（有序、带序号、可多次读取）。"""

    def __init__(self, stream_id: str, model: str = "", kind: str = "stream"):
        self.id = stream_id
        self.model = model
        self.kind = kind                      # stream | json
        self.created_at = time.time()
        self.finished_at: Optional[float] = None
        self.last_access = self.created_at
        self.status = "running"               # running | done | error
        self.error: Optional[str] = None
        self.events: List[str] = []           # SSE 帧，下标即 seq
        self.payload: Optional[dict] = None   # 非流式：完整 JSON 响应体
        self.status_code: int = 200
        self.fully_delivered = False          # 是否已有读者完整读到终止帧
        self.readers = 0
        self._cond = asyncio.Condition()
        self._timeout_seconds = upstream_timeout_seconds()
        self._started_at = time.monotonic()
        self._deadline = self._started_at + self._timeout_seconds
        self._finished_monotonic: Optional[float] = None
        self._last_event_at: Optional[float] = None
        self._last_wait_log_at = self._started_at
        # 请求指纹可以复用，轮次用于区分重试后新建的生成任务。
        self.attempt_id = uuid.uuid4().hex[:8]
        self.phase = "等待上游响应"
        self._pump_task: Optional[asyncio.Task] = None

    def remaining_timeout(self) -> float:
        """整个生成共用截止时间，不因续传或保活而延期。"""
        return max(0.0, self._deadline - time.monotonic())

    def log_lifecycle(self, event: str, detail: str = "") -> None:
        """仅记录生命周期元数据，不输出凭据、请求体或模型回复。"""
        now = time.monotonic()
        elapsed = (self._finished_monotonic or now) - self._started_at
        status = {"running": "进行中", "done": "已完成", "error": "已失败"}[self.status]
        mode = "流式" if self.kind == "stream" else "非流式"
        remaining = self.remaining_timeout() if not self.done else 0.0
        fields = (f"轮次={self.attempt_id} | 模式={mode} | 阶段={self.phase} | 状态={status}"
                  f" | 已用={elapsed:.1f}秒 | 剩余={remaining:.1f}秒 | 事件={self.next_seq}"
                  f" | 读者={self.readers}")
        if not self.done:
            fields += f" | 无事件={now - (self._last_event_at or self._started_at):.1f}秒"
        try:
            print(f"[{event}] {self.id} | {fields}" + (f" | {detail}" if detail else ""))
        except (OSError, UnicodeError):
            # 终端关闭或编码不兼容时，诊断日志不能中断请求生命周期。
            pass

    def log_wait_if_due(self) -> None:
        """长时间无数据时至多每分钟提示一次，不为每个保活刷日志。"""
        now = time.monotonic()
        if (not self.done
                and now - self._last_wait_log_at >= WAIT_LOG_INTERVAL_SECONDS
                and now - (self._last_event_at or self._started_at) >= WAIT_LOG_INTERVAL_SECONDS):
            self._last_wait_log_at = now
            self.log_lifecycle("上游等待", "尚未收到新数据；下游保活不计作上游事件")

    async def fail(self, message: str, code: int = 504, error_type: str = "timeout") -> bool:
        """原子结束缓冲并唤醒读者；取消泵但不等待可能失去响应的上游。"""
        async with self._cond:
            if self.done:
                return False
            error = {"error": {"message": message, "type": error_type, "code": code}}
            if self.kind == "stream":
                self.events.append(frame_with_id(len(self.events), f"data: {json.dumps(error, ensure_ascii=False)}\n\n"))
                self.events.append(frame_with_id(len(self.events), DONE_FRAME))
            else:
                self.payload = error
            self.status = "error"
            self.error = message
            self.status_code = code
            self.finished_at = time.time()
            self._finished_monotonic = time.monotonic()
            self._cond.notify_all()
        event = "上游超时" if code == 504 else "请求失败"
        self.log_lifecycle(event, f"HTTP {code}；错误类型={error_type}；缓冲已结束并唤醒等待者")
        task = self._pump_task
        if task and task is not asyncio.current_task() and not task.done():
            task.cancel()
        return True

    async def expire_if_stalled(self, *, timed_out: bool = False) -> bool:
        if self.done or (not timed_out and self.remaining_timeout() > 0):
            return False
        message = f"上游生成或读取超时（整轮总时限 {self._timeout_seconds:g} 秒），本轮已结束，请重试。"
        expired = await self.fail(message)
        return expired

    async def await_upstream(self, operation):
        """约束上游建流、读取及非流式调用，使用同一个截止时间。"""
        try:
            result = await asyncio.wait_for(operation, self.remaining_timeout())
        except asyncio.TimeoutError:
            # 底层读取可能先于整轮截止时间超时，计时器也可能按系统时钟精度提前唤醒。
            await self.expire_if_stalled(timed_out=True)
            raise ResumableTimeoutError(self.error or "上游生成已结束。") from None
        if await self.expire_if_stalled() or self.status == "error":
            raise ResumableTimeoutError(self.error or "上游生成已结束。")
        return result

    # ---------- 写入端（后台泵任务持有） ----------

    async def append(self, frame: str) -> int:
        async with self._cond:
            if self.done:
                return -1
            self.events.append(frame)
            self._last_event_at = time.monotonic()
            self._cond.notify_all()
            return len(self.events) - 1

    async def finish(self, status: str = "done", error: Optional[str] = None,
                     payload: Optional[dict] = None, status_code: Optional[int] = None) -> None:
        async with self._cond:
            if self.done:
                return
            self.status = status
            self.error = error
            if payload is not None:
                self.payload = payload
            if status_code is not None:
                self.status_code = status_code
            self.finished_at = time.time()
            self._finished_monotonic = time.monotonic()
            self._cond.notify_all()
        self.log_lifecycle("响应落缓冲", f"HTTP {self.status_code}；仅表示代理接收结束，不代表客户端已显示")

    # ---------- 读取端（可以有多个、可以中途离开再回来） ----------

    @property
    def next_seq(self) -> int:
        return len(self.events)

    @property
    def done(self) -> bool:
        return self.status != "running"

    def snapshot(self, start: int, limit: int) -> List[str]:
        start = max(0, start)
        return self.events[start:start + max(1, limit)]

    async def wait_for(self, seq: int, timeout: float) -> None:
        """等到出现 seq 号事件或本轮结束（或超时）。"""
        if timeout <= 0:
            return
        deadline = time.monotonic() + timeout
        while True:
            await self.expire_if_stalled()
            async with self._cond:
                if len(self.events) > seq or self.done:
                    return
                remain = min(deadline - time.monotonic(), self.remaining_timeout())
                if remain <= 0:
                    break
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=remain)
                except asyncio.TimeoutError:
                    pass
        await self.expire_if_stalled()

    async def iter_from(self, seq: int = 0) -> AsyncIterator[Tuple[int, str]]:
        """从 seq 开始跟随读取，直到本轮结束且读完所有事件。"""
        i = max(0, seq)
        self.readers += 1
        try:
            while True:
                await self.expire_if_stalled()
                async with self._cond:
                    if len(self.events) <= i and not self.done:
                        try:
                            await asyncio.wait_for(self._cond.wait(), self.remaining_timeout())
                        except asyncio.TimeoutError:
                            pass
                        continue
                    if len(self.events) <= i:
                        break                     # 已结束且无新事件
                    frame = self.events[i]
                self.last_access = time.time()
                yield i, frame
                i += 1
        finally:
            self.readers = max(0, self.readers - 1)

    def mark_delivered(self) -> None:
        """读者已把终止帧交付出去：下一次同指纹请求应视为新生成，而不是重放。"""
        self.fully_delivered = True
        self.last_access = time.time()

    def meta(self) -> dict:
        return {
            "id": self.id,
            "model": self.model,
            "kind": self.kind,
            "status": self.status,
            "error": self.error,
            "events": len(self.events),
            "next": len(self.events),
            "done": self.done,
            "fully_delivered": self.fully_delivered,
            "readers": self.readers,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "age_seconds": round(time.time() - self.created_at, 1),
        }


class ResumableStore:
    """缓冲区登记处：并发安全、带 TTL 与容量上限。"""

    def __init__(self):
        self._buffers: Dict[str, ResumableBuffer] = {}
        self._lock = asyncio.Lock()
        self._pumps: Dict[str, asyncio.Task] = {}

    async def get(self, stream_id: str) -> Optional[ResumableBuffer]:
        async with self._lock:
            buf = self._buffers.get(stream_id)
            if buf:
                await buf.expire_if_stalled()
                buf.last_access = time.time()
            return buf

    async def create(self, stream_id: str, model: str = "", kind: str = "stream") -> ResumableBuffer:
        async with self._lock:
            buf = ResumableBuffer(stream_id, model=model, kind=kind)
            self._buffers[stream_id] = buf
            await self._evict_locked()
            return buf

    async def create_if_absent(self, stream_id: str, model: str = "",
                               kind: str = "stream") -> Tuple[ResumableBuffer, bool]:
        """原子地「占位建缓冲」。返回 (缓冲区, 是否是本次新建的)。

        `get` → 调上游 → `create` 三步之间有竞态窗口：并发的同一请求会各打一次上游，
        后建的缓冲还会把先建的覆盖掉（孤儿泵任务停机时也取消不到）。这里把「查 + 建」
        放进同一把锁：只有拿到 is_new=True 的那个请求去调上游，其余请求直接跟读同一份缓冲。
        """
        async with self._lock:
            buf = self._buffers.get(stream_id)
            if buf is not None:
                await buf.expire_if_stalled()
                if buf.kind != kind or buf.fully_delivered or buf.status == "error":
                    await self._drop_locked(stream_id)
                    buf = None
            if buf is not None:
                buf.last_access = time.time()
                return buf, False
            buf = ResumableBuffer(stream_id, model=model, kind=kind)
            self._buffers[stream_id] = buf
            await self._evict_locked()
            return buf, True

    async def _drop_locked(self, stream_id: str) -> bool:
        buf = self._buffers.pop(stream_id, None)
        if buf and not buf.done:
            await buf.fail("后台任务已撤销，请重试。", 503, "upstream_error")
        task = self._pumps.pop(stream_id, None)
        if task and not task.done():
            task.cancel()
        return buf is not None

    async def drop(self, stream_id: str, expected: Optional[ResumableBuffer] = None) -> bool:
        async with self._lock:
            if expected is not None and self._buffers.get(stream_id) is not expected:
                return False
            return await self._drop_locked(stream_id)

    async def list_meta(self) -> List[dict]:
        async with self._lock:
            return [b.meta() for b in sorted(self._buffers.values(),
                                             key=lambda x: x.created_at, reverse=True)]

    def register_pump(self, stream_id: str, task: asyncio.Task,
                      expected: Optional[ResumableBuffer] = None) -> None:
        buf = self._buffers.get(stream_id)
        if buf is None or (expected is not None and buf is not expected):
            task.cancel()
            return
        buf._pump_task = task
        self._pumps[stream_id] = task

        def on_done(completed):
            # 旧任务稍后退出时，不能移除同指纹新任务的登记。
            if self._pumps.get(stream_id) is completed:
                self._pumps.pop(stream_id, None)
            if not completed.cancelled():
                completed.exception()
            if not buf.done:
                asyncio.create_task(buf.fail("后台任务意外结束，请重试。", 503, "upstream_error"))

        task.add_done_callback(on_done)

    def cancel_all_pumps(self) -> None:
        """取消所有后台泵任务（服务停机时调用）。"""
        for task in list(self._pumps.values()):
            if not task.done():
                task.cancel()
        self._pumps.clear()

    async def _evict_locked(self) -> None:
        """容量上限：优先淘汰最旧的「已完成」缓冲；实在没有就淘汰最旧的。"""
        limit = max_streams()
        if len(self._buffers) <= limit:
            return
        finished = sorted((b for b in self._buffers.values() if b.done and b.readers == 0),
                          key=lambda x: x.finished_at or x.created_at)
        for buf in finished:
            if len(self._buffers) <= limit:
                return
            await self._drop_locked(buf.id)
        while len(self._buffers) > limit:
            oldest = min(self._buffers.values(), key=lambda x: x.created_at)
            await self._drop_locked(oldest.id)

    async def sweep(self) -> int:
        """清掉超过 TTL 的已完成缓冲（有读者在读的不动）。"""
        ttl = ttl_seconds()
        now = time.time()
        removed = 0
        async with self._lock:
            for sid, buf in list(self._buffers.items()):
                # 生成截止时间优先于读者数量，避免活动连接让悬挂任务一直存活。
                await buf.expire_if_stalled()
                buf.log_wait_if_due()
                if buf.readers > 0:
                    continue
                ref = buf.finished_at or buf.last_access
                if buf.done and (now - ref) > ttl:
                    self._buffers.pop(sid, None)
                    removed += 1
            await self._evict_locked()
        if removed:
            print(f"🧹 [续传缓冲] 已清理 {removed} 个过期响应缓冲。")
        return removed

    async def sweeper_loop(self, interval: float = 60.0) -> None:
        while True:
            try:
                await asyncio.sleep(interval)
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"⚠️ [续传缓冲] 清理任务异常（已忽略）：{type(e).__name__}")


resumable_store = ResumableStore()


class DetachedRequest:
    """把上游调用与前端连接解绑的请求壳子。

    上游代码用 `fastapi_request.is_disconnected()` 决定要不要放弃调用/重试。
    弱网下前端断开后仍需在总时限内接收回复，所以这里恒返回 False；
    其余属性（app.state、headers 等）原样透传给被包装的真实 Request。
    """

    def __init__(self, request: Any):
        self._request = request

    async def is_disconnected(self) -> bool:
        return False

    def __getattr__(self, item):
        return getattr(self._request, item)


def _as_text(chunk: Any) -> str:
    if isinstance(chunk, bytes):
        return chunk.decode("utf-8", errors="replace")
    return str(chunk)


def frame_with_id(seq: int, frame: str) -> str:
    """给 SSE 数据帧加上事件 ID，供 Last-Event-ID 续传。

    注释帧（以 `:` 开头）不带 id —— 它不是事件，带 id 会污染客户端游标。
    """
    if frame.startswith("data:"):
        return f"id: {seq}\n{frame}"
    return frame


async def pump_stream(buf: ResumableBuffer, body_iterator: AsyncIterator[Any]) -> None:
    """后台把上游 SSE 帧抽进缓冲区。**不依赖前端连接**。

    每个 data 帧按落缓冲的顺序分配单调序列号并写入 `id:` 行，
    前端凭 Last-Event-ID / resume_from 续传时与这里的序号对齐。
    """
    try:
        iterator = aiter(body_iterator)
        while not buf.done:
            try:
                chunk = await buf.await_upstream(anext(iterator))
            except StopAsyncIteration:
                break
            text = _as_text(chunk)
            if not text:
                continue
            if text.lstrip().startswith(":"):
                # 注释帧（旧心跳等）不是事件：不进缓冲、不占序号，保持 id == 下标
                continue
            await buf.append(frame_with_id(buf.next_seq, text))
            if buf.next_seq == 1:
                buf.phase = "读取上游事件"
                buf.log_lifecycle("首个上游事件", "数据帧可能包含思考、工具调用或错误，尚不能判定正文已显示")
        if buf.done:
            return
        if not buf.events or not buf.events[-1].strip().endswith("[DONE]"):
            await buf.append(frame_with_id(buf.next_seq, DONE_FRAME))
        await buf.finish("done")
    except asyncio.CancelledError:
        await buf.fail("上游读取任务被取消，请重试。", 499, "upstream_error")
        raise
    except ResumableTimeoutError:
        pass  # 截止时间处理已经写入错误及终止帧。
    except Exception as e:
        buf.log_lifecycle("上游读取异常", f"异常类型={type(e).__name__}")
        await buf.fail(str(e)[:1024], 500, "upstream_error")
    finally:
        aclose = getattr(body_iterator, "aclose", None)
        if aclose:
            try:
                await aclose()
            except Exception:
                pass


async def sse_from_buffer(buf: ResumableBuffer, start_seq: int = 0) -> AsyncIterator[str]:
    """把缓冲区内容按 SSE 吐给当前这条前端连接（可从任意 seq 续取）。

    事件落缓冲时已带 `id: <seq>`（见 pump_stream），这里原样吐出，不再二次加工。
    """
    if start_seq > 0:
        buf.log_lifecycle("续传起点", f"从事件 #{start_seq} 开始")
    delivered_done = False
    completed = False
    sent_events = 0
    iterator = buf.iter_from(start_seq)
    pending = None
    try:
        # 立即建立下游响应，等待期间也定期保活；不污染续传游标。
        yield ": keep-alive\n\n"
        while True:
            if pending is None:
                pending = asyncio.create_task(anext(iterator))
            ready, _ = await asyncio.wait({pending}, timeout=HEARTBEAT_INTERVAL_SECONDS)
            if not ready:
                yield ": keep-alive\n\n"
                continue
            try:
                _seq, frame = pending.result()
            except StopAsyncIteration:
                completed = True
                break
            finally:
                pending = None
            sent_events += 1
            yield frame
            if frame.strip().endswith("[DONE]"):
                delivered_done = True
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await iterator.aclose()
        if not completed:
            action = "后台任务仍在运行，可在截止时间前续传" if not buf.done else "已结束的缓冲仍按保留期保存"
            buf.log_lifecycle("下游读取中断", f"本连接已输出={sent_events}个事件；{action}")
    if delivered_done:
        buf.mark_delivered()
    buf.log_lifecycle("下游发送结束", f"本连接已输出={sent_events}个事件；终止帧={'已输出' if delivered_done else '未输出'}；不代表客户端已显示")
