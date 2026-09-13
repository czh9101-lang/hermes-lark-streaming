"""通用节流调度器 — FlushController."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable

_logger = logging.getLogger("hermes_lark_streaming")


CARDKIT_MS = 0.100  # CardKit 流式 API 的刷新间隔
LONG_GAP_MS = 2.000  # 超过此间隔 → 认为是长时间空闲
BATCH_AFTER_GAP_MS = 0.300  # 长时间空闲后等待这个时间再 flush


class FlushController:
    """带互斥锁 + 延迟刷新的通用节流调度器.

    不包含飞书业务逻辑，只负责决定何时执行回调.
    """

    def __init__(self, throttle_ms: float = CARDKIT_MS, *, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._throttle_ms = throttle_ms
        self._flush_in_progress = False
        self._needs_reflush = False
        self._pending_timer: asyncio.TimerHandle | None = None
        self._timer_pending = False
        self._last_update_time = 0.0
        self._completed = False
        self._card_message_ready = False
        self._flush_resolvers: list[asyncio.Future[None]] = []
        self._loop = loop if loop is not None else asyncio.get_running_loop()
        # 串行化跨线程调度：timer 句柄与节流状态只在这个锁内读写，
        # 避免与控制权所在线程的 _run_once 竞争 _scheduled 堆（见 _schedule）。
        # 用 RLock：schedule_update 持锁时会同步调用 _do_flush_task（立即 flush
        # 分支），后者同样需要清理 timer 状态——普通 Lock 会在同线程自锁。
        self._schedule_lock = threading.RLock()

    @property
    def throttle_ms(self) -> float:
        return self._throttle_ms

    @throttle_ms.setter
    def throttle_ms(self, value: float) -> None:
        self._throttle_ms = value

    @property
    def last_update_time(self) -> float:
        return self._last_update_time

    def schedule_update(self, do_flush: Callable[[], Awaitable[None]]) -> None:
        """请求一次节流后的卡片刷新.

        do_flush: async callable，执行实际 API 调用.
        """
        with self._schedule_lock:
            if self._completed or not self._card_message_ready:
                return
            now = time.monotonic()
            elapsed = now - self._last_update_time

            if elapsed >= self._throttle_ms:
                # 超出节流窗口
                if elapsed > LONG_GAP_MS:
                    # 长时间空闲 → 延迟一小批让内容更完整
                    if not self._timer_pending:
                        self._schedule(delay=BATCH_AFTER_GAP_MS, do_flush=do_flush)
                else:
                    # 立即 flush
                    self._do_flush_task(do_flush)
            else:
                # 仍在节流窗口内 → 延迟到窗口边界
                if not self._timer_pending:
                    delay = self._throttle_ms - elapsed
                    self._schedule(delay=delay, do_flush=do_flush)

    async def flush_now(self, do_flush: Callable[[], Awaitable[None]]) -> None:
        """立即执行一次 flush，等待完成."""
        with self._schedule_lock:
            if self._completed or not self._card_message_ready:
                return
            self._cancel_timer()
        await self._do_flush(do_flush)

    async def wait_for_flush(self) -> None:
        """等待进行中的 flush 完成."""
        if not self._flush_in_progress:
            return
        future: asyncio.Future[None] = self._loop.create_future()
        self._flush_resolvers.append(future)
        await future

    def mark_completed(self) -> None:
        """标记完成，不再接受新更新."""
        with self._schedule_lock:
            self._completed = True
            self._cancel_timer()
        for r in self._flush_resolvers:
            if not r.done():
                r.set_result(None)
        self._flush_resolvers.clear()

    def set_throttle(self, ms: float) -> None:
        self._throttle_ms = ms

    def set_card_message_ready(self, ready: bool) -> None:
        """设置卡片消息已就绪，初始化时间戳."""
        with self._schedule_lock:
            self._card_message_ready = ready
            if ready:
                self._last_update_time = time.monotonic()

    def _is_loop_thread(self) -> bool:
        """判断当前线程是否为事件循环所在线程."""
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def _schedule(self, delay: float, do_flush: Callable[[], Awaitable[None]]) -> None:
        """安排一次延迟 flush（调用方持有 _schedule_lock）.

        ``loop.call_later`` 会 heappush ``loop._scheduled``，而该堆只在循环
        所在线程安全。流式回调可能在 agent 工作线程（gateway 的
        hermes-gateway_* 线程）里执行，直接在那些线程 heappush 会与循环
        线程的 heappop 竞争，抛出
        ``RuntimeError: list changed size during iteration`` 并**让整个
        事件循环崩掉**。所以非循环线程一律用 call_soon_threadsafe 把
        "装载定时器"这一步交回循环线程执行。
        """
        self._cancel_timer()
        self._timer_pending = True
        if self._is_loop_thread():
            self._arm_timer(delay, do_flush)
            return
        try:
            self._loop.call_soon_threadsafe(self._arm_timer, delay, do_flush)
        except RuntimeError:  # 循环已关闭
            self._timer_pending = False

    def _arm_timer(self, delay: float, do_flush: Callable[[], Awaitable[None]]) -> None:
        """在事件循环线程内装载定时器（唯一允许 heappush 的地方）."""
        try:
            self._pending_timer = self._loop.call_later(
                delay,
                self._do_flush_task,
                do_flush,
            )
        except RuntimeError:  # 循环已关闭
            self._timer_pending = False

    def _start_flush(self, do_flush: Callable[[], Awaitable[None]]) -> None:
        """把 flush 协程交给事件循环线程，调用方线程不限."""
        coro = self._do_flush(do_flush)
        if self._is_loop_thread():
            self._loop.create_task(coro)
            return
        try:
            asyncio.run_coroutine_threadsafe(coro, self._loop)
        except RuntimeError:
            coro.close()

    def _do_flush_task(self, do_flush: Callable[[], Awaitable[None]]) -> None:
        with self._schedule_lock:
            self._pending_timer = None
            self._timer_pending = False
        self._start_flush(do_flush)

    async def _do_flush(self, do_flush: Callable[[], Awaitable[None]]) -> None:
        if self._completed or self._flush_in_progress:
            self._needs_reflush = True
            return

        self._flush_in_progress = True
        self._needs_reflush = False
        try:
            await do_flush()
        except Exception:
            _logger.debug("flush error suppressed", exc_info=True)
        finally:
            self._flush_in_progress = False
            self._last_update_time = time.monotonic()
            # 唤醒等待者
            resolvers = self._flush_resolvers
            self._flush_resolvers = []
            for r in resolvers:
                if not r.done():
                    r.set_result(None)

        # 如果 flush 期间又有新数据 → 立即重刷
        if self._needs_reflush and not self._completed:
            self._needs_reflush = False
            self._start_flush(do_flush)

    def _cancel_timer(self) -> None:
        if self._pending_timer is not None:
            self._pending_timer.cancel()
            self._pending_timer = None
        self._timer_pending = False
