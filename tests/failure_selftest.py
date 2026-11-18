"""
主端故障收尾回归自测 (无 pytest, 仅依赖运行期依赖 aiohttp)。

覆盖:
  A. 主端在影子任务首次执行前就报错: 502 + primary_error, 每次名额立即归还,
     连续 8 次也不再卡在 4/4; 影子状态写明 "cancelled before start"。
  B. 主端在影子在途时故障: 502 保留故障记录, 已启动的影子等通信退出后归还;
     取消与正常结束交错时不重复归还、不越界, 主端故障不被写成客户端断连。
  C. 主端恢复: 网页发起的 /quote 与 /quote-big 继续逐块透传, 并重新得到完整
     双端比对 (match / mismatch)。
  D. 客户端断连仍是客户端断连, 名额归还, 之后恢复正常;
     硬断连风暴 (取消与影子完成在同一 tick 交错) 也不会漏名额或重复记录。

启动方式 (与 selftest.py 相同, 需先安装 requirements.txt):
    python tests/failure_selftest.py
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import threading
import time
import urllib.parse
from pathlib import Path

import aiohttp
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import gateway as gw          # noqa: E402
from app import quote_service as qs    # noqa: E402

PASS, FAIL = [], []
PENDING = {"pending", "shadow_pending", "shadow_completed_primary_incomplete"}


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


class ThreadServer:
    """在独立线程 + 独立事件循环里跑一个 uvicorn 应用, 用 Event 优雅停止。"""

    def __init__(self, app):
        self.config = uvicorn.Config(app, host="127.0.0.1", port=0,
                                     log_level="warning", lifespan="on",
                                     access_log=False)
        self.server = uvicorn.Server(self.config)
        self.port = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._error = None

    def _run(self):
        try:
            asyncio.run(self.server.serve())
        except BaseException as exc:  # noqa: BLE001
            self._error = exc

    def start(self, timeout=20.0):
        self._thread.start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._error is not None:
                raise RuntimeError(f"server failed: {self._error!r}")
            if self.server.started:
                self.port = self.server.servers[0].sockets[0].getsockname()[1]
                return self
            time.sleep(0.02)
        raise RuntimeError("server did not start")

    def stop(self, timeout=10.0):
        self.server.should_exit = True
        self._thread.join(timeout)
        if self.server.started:
            time.sleep(0.05)

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"


class StubPrimary:
    """
    可控主端桩 (独立线程 + 原始 asyncio TCP), 用于制造确定的故障形态:
      mode=ok / 缺省   正常返回与真实报价服务一致的确定性字节
      mode=close       收到请求后立刻断开 (主端故障)
      mode=delay_close 等待 close_after 秒后断开 (保证影子已在途)
      mode=partial     先发响应头与部分 body 再断开 (主端传输中断)
    正常返回时 /quote 的 body 与 app.quote_service 的序列化完全一致,
    /quote-big 复用 quote_service._deterministic_block, 因此双端可判定为 match。
    """

    def __init__(self):
        self.port = None
        self._ready = threading.Event()
        self._loop = None
        self._server = None
        self._stop_evt = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self, timeout=10.0):
        self._thread.start()
        if not self._ready.wait(timeout):
            raise RuntimeError("stub primary did not start")
        return self

    def _run(self):
        asyncio.run(self._main())

    async def _main(self):
        self._loop = asyncio.get_running_loop()
        self._stop_evt = asyncio.Event()
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        self._ready.set()
        await self._stop_evt.wait()
        self._server.close()
        await self._server.wait_closed()

    def stop(self, timeout=5.0):
        if self._loop is None:
            return
        try:
            self._loop.call_soon_threadsafe(self._stop_evt.set)
        except RuntimeError:
            return
        self._thread.join(timeout)

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    async def _handle(self, reader, writer):
        try:
            try:
                raw = await reader.readuntil(b"\r\n\r\n")
            except (asyncio.IncompleteReadError, ConnectionError):
                return
            try:
                target = raw.split(b"\r\n", 1)[0].decode().split(" ")[1]
            except Exception:
                return
            split = urllib.parse.urlsplit(target)
            path = split.path
            q = urllib.parse.parse_qs(split.query)
            mode = q.get("mode", ["ok"])[0]

            if mode == "close":
                return
            if mode == "delay_close":
                await asyncio.sleep(float(q.get("close_after", ["0.2"])[0]))
                return
            if mode == "partial":
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Content-Length: 100\r\nConnection: close\r\n\r\n"
                    b'{"partial":true}'
                )
                await writer.drain()
                await asyncio.sleep(0.15)
                return

            delay = float(q.get("p_delay", ["0"])[0])
            if delay:
                await asyncio.sleep(delay)
            status = int(q.get("status", ["200"])[0])
            if q.get("fail") == ["1"]:
                payload = json.dumps({"error": "forced failure"},
                                     separators=(",", ":")).encode()
                status = 500
                ctype = "application/json"
            elif path == "/quote-big":
                size = max(0, min(int(q.get("size", ["33554432"])[0]),
                                  64 * 1024 * 1024))
                await self._stream_big(writer, size, q)
                return
            else:
                payload = json.dumps({
                    "symbol": q.get("symbol", ["ACME"])[0],
                    "currency": "USD",
                    "price": "128.50",
                    "qty": 1000,
                    "tier": "standard",
                    "conditions": ["firm", "spot", "net"],
                }, separators=(",", ":"), sort_keys=True).encode()
                ctype = "application/json"

            reason = "OK" if status == 200 else "ERR"
            writer.write(
                f"HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\n"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                + payload
            )
            await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _stream_big(self, writer, size, q):
        chunk_ms = int(q.get("chunk_ms", ["0"])[0])
        status = int(q.get("status", ["200"])[0])
        writer.write(
            f"HTTP/1.1 {status} OK\r\nContent-Type: application/octet-stream\r\n"
            f"X-Response-Bytes: {size}\r\n"
            f"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n".encode()
        )
        remaining, index = size, 0
        while remaining > 0:
            n = min(256 * 1024, remaining)
            block = qs._deterministic_block(index, n)
            writer.write(f"{len(block):x}\r\n".encode() + block + b"\r\n")
            await writer.drain()
            remaining -= n
            index += 1
            if chunk_ms:
                await asyncio.sleep(chunk_ms / 1000.0)
        writer.write(b"0\r\n\r\n")
        await writer.drain()


# --------------------------------------------------------------- 工具函数

async def get_records(s, base):
    return await (await s.get(f"{base}/api/records")).json()


async def get_state(s, base):
    return await (await s.get(f"{base}/api/state")).json()


async def wait_record(s, base, marker, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        recs = await get_records(s, base)
        hit = [x for x in recs if marker in x["target"]]
        if hit and hit[0]["result"] not in PENDING:
            return hit[0]
        await asyncio.sleep(0.05)
    raise AssertionError(f"terminal record for {marker} not found")


async def wait_zero_slots(s, base, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = await get_state(s, base)
        if st["shadow_in_flight"] == 0:
            return True
        await asyncio.sleep(0.05)
    return False


# ------------------------------------------------------- 进程内交错单元测试

async def unit_never_started_task_releases_once():
    gw.state["limiter"] = gw.ShadowLimiter(capacity=4)
    assert await gw.state["limiter"].acquire()
    rec = {"id": "u1", "target": "/quote", "started_at": gw.now_ms(),
           "result": "primary_error", "primary": "pending", "shadow": "started"}
    ctx = gw.CallContext(rid="u1", target="/quote", started_at=gw.now_ms(),
                         record=rec, slot_held=True)

    async def body():
        try:
            await asyncio.sleep(3600)
        finally:
            gw.mark_shadow_exited(ctx)  # 首次执行前取消时不会执行

    task = asyncio.create_task(body())
    ctx.shadow_task = task
    task.add_done_callback(lambda t: gw.on_shadow_task_done(ctx, t))
    task.cancel()  # 影子尚未获得首次执行机会
    await gw.stop_shadow(ctx, "primary_error")
    check("A0 未启动影子取消后名额归还且只一次",
          gw.state["limiter"].in_flight == 0 and not ctx.shadow_started,
          f"in_flight={gw.state['limiter'].in_flight}")
    check("A0 未启动影子状态写明原因",
          rec["shadow"] == "cancelled before start (primary_error)", rec["shadow"])
    # 取消与完成回调交错: 再收尾也不得二次归还
    task.cancel()
    gw.on_shadow_task_done(ctx, task)
    await gw.stop_shadow(ctx, "primary_error")
    check("A0 二次收尾不重复归还", gw.state["limiter"].in_flight == 0,
          str(gw.state["limiter"].in_flight))


async def unit_started_shadow_waits_for_comm_exit():
    class Resp:
        status = 200

        class content:
            @staticmethod
            async def iter_chunked(_n):
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    await asyncio.sleep(0.25)  # 取消后通信仍在收尾
                    raise
                yield b""  # pragma: no cover

    class Ctx:
        async def __aenter__(self):
            return Resp()

        async def __aexit__(self, *a):
            return False

        def __await__(self):
            async def _open():
                return self
            return _open().__await__()

    class FakeSession:
        def get(self, *a, **k):
            return Ctx()

    gw.state["limiter"] = gw.ShadowLimiter(capacity=4)
    real_session = gw.state.get("session")
    gw.state["session"] = FakeSession()
    try:
        assert await gw.state["limiter"].acquire()
        rec = {"id": "u2", "target": "/quote", "started_at": gw.now_ms(),
               "result": "primary_error", "primary": "pending",
               "shadow": "started"}
        ctx = gw.CallContext(rid="u2", target="/quote", started_at=gw.now_ms(),
                             record=rec, slot_held=True)
        task = asyncio.create_task(gw.run_shadow(ctx))
        ctx.shadow_task = task
        task.add_done_callback(lambda t: gw.on_shadow_task_done(ctx, t))
        await asyncio.sleep(0.05)
        check("B0 影子确实已启动",
              ctx.shadow_started and gw.state["limiter"].in_flight == 1)

        stopper = asyncio.create_task(gw.stop_shadow(ctx, "primary_error"))
        await asyncio.sleep(0.08)
        check("B0 取消已发出但通信未退出时名额不提前归还",
              gw.state["limiter"].in_flight == 1,
              str(gw.state["limiter"].in_flight))
        await stopper
        check("B0 通信退出后名额才归还",
              gw.state["limiter"].in_flight == 0,
              str(gw.state["limiter"].in_flight))
        check("B0 主端故障未被写成客户端断连",
              rec["result"] == "primary_error", rec["result"])
        check("B0 影子侧写明取消原因",
              rec["shadow"] == "cancelled: primary_error", rec["shadow"])
        await gw.stop_shadow(ctx, "primary_error")
        check("B0 取消与正常结束交错不重复归还",
              gw.state["limiter"].in_flight == 0,
              str(gw.state["limiter"].in_flight))
    finally:
        gw.state["session"] = real_session


async def unit_shadow_completed_then_primary_error():
    class Resp:
        status = 200

        class content:
            @staticmethod
            async def iter_chunked(_n):
                yield b'{"price":"128.50"}'

    class Ctx:
        async def __aenter__(self):
            return Resp()

        async def __aexit__(self, *a):
            return False

        def __await__(self):
            async def _open():
                return self
            return _open().__await__()

    class FakeSession:
        def get(self, *a, **k):
            return Ctx()

    gw.state["limiter"] = gw.ShadowLimiter(capacity=4)
    real_session = gw.state.get("session")
    gw.state["session"] = FakeSession()
    try:
        assert await gw.state["limiter"].acquire()
        rec = {"id": "u3", "target": "/quote", "started_at": gw.now_ms(),
               "result": "pending", "primary": "pending", "shadow": "started"}
        ctx = gw.CallContext(rid="u3", target="/quote", started_at=gw.now_ms(),
                             record=rec, slot_held=True)
        task = asyncio.create_task(gw.run_shadow(ctx))
        ctx.shadow_task = task
        task.add_done_callback(lambda t: gw.on_shadow_task_done(ctx, t))
        await task
        check("B1 影子先完成时不提前报一致",
              rec["result"] == "shadow_completed_primary_incomplete",
              rec["result"])

        gw.set_result(rec, "primary_error", force=True)
        await gw.stop_shadow(ctx, "primary_error")
        check("B1 影子已完成时主端故障仍是主端故障",
              rec["result"] == "primary_error" and rec["shadow"] == "completed",
              f"{rec['result']}/{rec['shadow']}")
        gw.set_result(rec, "client_disconnected")
        check("B1 客户端取消不覆盖主端故障",
              rec["result"] == "primary_error", rec["result"])
        check("B1 名额归还", gw.state["limiter"].in_flight == 0,
              str(gw.state["limiter"].in_flight))
    finally:
        gw.state["session"] = real_session


# ------------------------------------------------------------- HTTP 集成测试

async def http_prestart_failure(base, stub_url):
    """主端在影子首次运行前报错: 连续/并发请求都不能泄漏名额。"""
    gw.PRIMARY_URL = "bad-url"  # 同步抛 InvalidUrlClientError
    try:
        async with aiohttp.ClientSession() as s:
            for i in range(8):
                async with s.get(f"{base}/quote?marker=pre{i}") as r:
                    body = await r.read()
                    status = r.status
                check(f"A 第{i + 1}次主端失败返回502", status == 502, str(status))
                if i == 0:
                    check("A 502 响应体是明确的主端不可用",
                          b"primary quote service unavailable" in body)
                st = await get_state(s, base)
                check(f"A 第{i + 1}次后名额立即归还",
                      st["shadow_in_flight"] == 0, str(st["shadow_in_flight"]))

            async def one(i):
                async with aiohttp.ClientSession() as cs:
                    async with cs.get(f"{base}/quote?marker=conc{i}") as r:
                        await r.read()
                        return r.status

            statuses = await asyncio.gather(*(one(i) for i in range(6)))
            check("A 并发主端失败全部502", statuses == [502] * 6, str(statuses))
            check("A 并发失败后名额为0", await wait_zero_slots(s, base))
            recs = [x for x in await get_records(s, base)
                    if "marker=conc" in x["target"]]
            check("A 并发请求都保留了故障记录", len(recs) == 6, str(len(recs)))
            check("A 故障记录为 primary_error",
                  all(x["result"] == "primary_error" for x in recs),
                  str({x["result"] for x in recs}))
            started = [x for x in recs if x["shadow"].startswith("cancelled before start")]
            skipped = [x for x in recs if x["shadow"] == "skipped"]
            check("A 被发起的影子均因未启动而及时归还",
                  all(x["shadow"] == "cancelled before start (primary_error)"
                      for x in started), str({x["shadow"] for x in started}))
            check("A 跳过的影子注明了原因",
                  all("already in flight" in x.get("detail", "") for x in skipped),
                  str([x.get("detail") for x in skipped]))
            check("A 主端故障下不再出现 4/4 卡死",
                  (await get_state(s, base))["shadow_in_flight"] == 0,
                  f"started={len(started)} skipped={len(skipped)}")

            # 主端恢复: 名额空闲, 影子重新被调用
            gw.PRIMARY_URL = stub_url
            async with s.get(f"{base}/quote?marker=pre-recover") as r:
                await r.read()
            rec = await wait_record(s, base, "marker=pre-recover")
            check("A 主端恢复后影子重新被调用",
                  rec["result"] == "match" and rec["shadow"] == "completed",
                  f"{rec['result']}/{rec['shadow']}")
    finally:
        gw.PRIMARY_URL = stub_url


async def http_inflight_failure(base, marker):
    """主端在影子在途时故障: 故障记录保留、影子退出后归还。"""
    async def one(i):
        async with aiohttp.ClientSession() as cs:
            async with cs.get(
                f"{base}/quote?mode=delay_close&close_after=0.25"
                f"&s_delay=0.8&marker={marker}-{i}"
            ) as r:
                await r.read()
                return r.status

    statuses = await asyncio.gather(*(one(i) for i in range(3)))
    check("B 主端在途故障返回502", statuses == [502, 502, 502], str(statuses))
    async with aiohttp.ClientSession() as s:
        check("B 影子通信退出后名额归零", await wait_zero_slots(s, base))
        recs = [x for x in await get_records(s, base)
                if f"marker={marker}-" in x["target"]]
    check("B 三条故障记录齐全", len(recs) == 3, str(len(recs)))
    check("B 记录为 primary_error 而非客户端断连",
          all(x["result"] == "primary_error" for x in recs),
          str({x["result"] for x in recs}))
    # aiohttp 对幂等 GET 会重试一次, 主端故障可能晚于快影子被发现, 此时影子
    # 合法地已完成; 关键是不能出现半开状态, 也不能靠文档撒谎。
    check("B 影子终态明确 (取消原因或已完成)",
          all(x["shadow"] == "completed"
              or x["shadow"].startswith("cancelled: primary_error")
              for x in recs), str({x["shadow"] for x in recs}))
    check("B 故障记录不含半开影子状态",
          all(x["shadow"] not in ("started", "in flight") for x in recs),
          str({x["shadow"] for x in recs}))


async def http_partial_primary(base, marker):
    """主端传输中途断开: 客户端已收到部分主端字节, 记录为主端不完整。"""
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(f"{base}/quote?mode=partial&marker={marker}") as r:
                body = await r.read()
        except aiohttp.ClientError:
            body = b""
        rec = await wait_record(s, base, marker)
        check("B 主端中途断开记录为 primary_incomplete",
              rec["result"] == "primary_incomplete", rec["result"])
        check("B 主端中途断开不误记客户端断连",
              rec["result"] != "client_disconnected")
        check("B 主端中途断开后名额归零", await wait_zero_slots(s, base))
        # 影子可能已先于主端完整结束(合法), 也可能被取消; 两种情况都不得
        # 让记录变成 match 或客户端断连
        check("B 影子状态符合事实", rec["shadow"] in ("completed",)
              or rec["shadow"].startswith("cancelled: primary_incomplete"),
              rec["shadow"])
        check("B 主端不完整不产生一致性结论",
              rec["result"] not in ("match", "mismatch"), rec["result"])


async def http_interleaving(base):
    """取消/正常结束交错: 名额不越界、不重复归还、不误记断连。"""
    observed = []
    stop_probe = False

    async def probe():
        async with aiohttp.ClientSession() as ps:
            while not stop_probe:
                try:
                    st = await get_state(ps, base)
                    observed.append(st["shadow_in_flight"])
                except aiohttp.ClientError:
                    pass
                await asyncio.sleep(0.01)

    async def one(i):
        close_after = 0.04 if i % 2 == 0 else 0.18
        s_delay = 0.0 if i % 3 == 0 else (0.03 if i % 3 == 1 else 0.4)
        async with aiohttp.ClientSession() as cs:
            async with cs.get(
                f"{base}/quote?mode=delay_close&close_after={close_after}"
                f"&s_delay={s_delay}&marker=race-{i}"
            ) as r:
                await r.read()
                return r.status

    probe_task = asyncio.create_task(probe())
    try:
        statuses = await asyncio.gather(*(one(i) for i in range(12)))
    finally:
        stop_probe = True
        await probe_task
    check("B 交错场景全部返回502", statuses == [502] * 12, str(statuses))
    async with aiohttp.ClientSession() as s:
        check("B 交错场景名额最终归零",
              await wait_zero_slots(s, base, timeout=10))
        recs = [x for x in await get_records(s, base)
                if "marker=race-" in x["target"]]
    if observed:
        check("B 名额采样始终在 0..4 之间",
              all(0 <= v <= 4 for v in observed),
              f"min={min(observed)} max={max(observed)} n={len(observed)}")
    else:
        check("B 名额采样始终在 0..4 之间", False, "no samples")
    check("B 交错场景记录齐全", len(recs) == 12, str(len(recs)))
    check("B 交错场景全部是主端故障",
          all(x["result"] == "primary_error" for x in recs),
          str({x["result"] for x in recs}))
    check("B 无重复记录", len({x["id"] for x in recs}) == 12)


async def http_recovery_and_streaming(base):
    """主端恢复: 网页路径逐块透传 + 完整双端比对 (一致/不一致/大响应)。"""
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{base}/") as r:
            html = await r.text()
            check("C 网页控制台可访问",
                  r.status == 200 and "影子流量网关" in html, str(r.status))

        async with s.get(f"{base}/quote?symbol=ZZZ&marker=recover-json") as r:
            check("C 恢复后主端状态码透传", r.status == 200, str(r.status))
            h, total, chunks = hashlib.sha256(), 0, 0
            async for chunk in r.content.iter_chunked(65536):
                h.update(chunk)
                total += len(chunk)
                chunks += 1
        check("C 恢复后收到完整主端字节", total > 0 and chunks >= 1,
              f"{total}B/{chunks}chunks")
        rec = await wait_record(s, base, "marker=recover-json")
        check("C 恢复后重新得到双端一致", rec["result"] == "match", rec["result"])
        check("C 主端摘要与客户端收到字节一致",
              rec["primary_sha256"] == h.hexdigest())
        check("C 影子端完整结束", rec["shadow"] == "completed", rec["shadow"])
        check("C 双端状态码相同",
              rec.get("primary_status") == rec.get("shadow_status") == 200)

        async with s.get(f"{base}/quote?diff=1&marker=recover-diff") as r:
            await r.read()
        rec = await wait_record(s, base, "marker=recover-diff")
        check("C 恢复后仍可发现两端差异", rec["result"] == "mismatch",
              rec.get("detail", ""))

        h, total, chunks = hashlib.sha256(), 0, 0
        t0 = time.monotonic()
        first_at = None
        async with s.get(f"{base}/quote-big?size=1048576&chunk_ms=20"
                         f"&marker=recover-big") as r:
            async for chunk in r.content.iter_chunked(65536):
                if first_at is None:
                    first_at = time.monotonic() - t0
                h.update(chunk)
                total += len(chunk)
                chunks += 1
            elapsed = time.monotonic() - t0
        check("C 大响应字节数正确", total == 1048576, str(total))
        check("C 大响应逐块透传", chunks >= 2, f"{chunks}chunks")
        if first_at is not None:
            check("C 首块先于整体结束到达", first_at < elapsed,
                  f"first={first_at * 1000:.0f}ms total={elapsed * 1000:.0f}ms")
        else:
            check("C 首块先于整体结束到达", False, "no chunk")
        rec = await wait_record(s, base, "marker=recover-big")
        check("C 大响应两端一致", rec["result"] == "match", rec["result"])
        check("C 大响应客户端摘要=主端摘要",
              rec["primary_sha256"] == h.hexdigest())
        check("C 恢复后名额重新可用",
              (await get_state(s, base))["shadow_in_flight"] == 0)


async def http_client_disconnect(base, marker):
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(f"{base}/quote-big?chunk_ms=50&marker={marker}") as r:
                n = 0
                async for _ in r.content.iter_chunked(65536):
                    n += 1
                    if n >= 3:
                        break
        except Exception:
            pass
        check("D 断连后名额归还", await wait_zero_slots(s, base))
        rec = await wait_record(s, base, marker)
        check("D 断连记录仍是客户端断连",
              rec["result"] == "client_disconnected", rec["result"])
        async with s.get(f"{base}/quote?marker={marker}-after") as r:
            await r.read()
        rec = await wait_record(s, base, f"{marker}-after")
        check("D 断连后仍能完成双端比对", rec["result"] == "match", rec["result"])


def _abrupt_get(port, path):
    """读到首块后立刻硬关 socket, 触发 uvicorn 的取消式断连。"""
    import socket as socket_mod
    sock = socket_mod.create_connection(("127.0.0.1", port), timeout=5)
    sock.sendall(
        f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        .encode()
    )
    sock.recv(65536)
    sock.close()


async def http_abrupt_disconnect_race(base, port, rounds=12):
    """
    硬断连风暴: 取消与影子请求完成可能在同一事件循环 tick 交错。
    (旧实现用 wait_for 会吞掉这次取消, 影子继续读流并长期占住名额。)
    要求: 每轮结束后名额都回到 0, 记录不出现重复或误判。
    """
    async with aiohttp.ClientSession() as s:
        worst = 0
        for i in range(rounds):
            await asyncio.get_running_loop().run_in_executor(
                None, _abrupt_get, port,
                f"/quote-big?chunk_ms=20&marker=abrupt-{i}")
            await asyncio.sleep(0.03)
            st = await get_state(s, base)
            worst = max(worst, st["shadow_in_flight"])
        zero = await wait_zero_slots(s, base, timeout=10)
        check("D 硬断连风暴后名额归零", zero, f"worst={worst}")
        recs = [x for x in await get_records(s, base)
                if "marker=abrupt-" in x["target"]]
        check("D 硬断连风暴无重复记录",
              len(recs) == len({x["id"] for x in recs}), str(len(recs)))
        check("D 硬断连风暴均为客户端断连",
              all(x["result"] == "client_disconnected" for x in recs),
              str({x["result"] for x in recs}))
        check("D 硬断连风暴影子状态是取消而非 started",
              all(x["shadow"].startswith("cancelled")
                  for x in recs), str({x["shadow"] for x in recs}))
        async with s.get(f"{base}/quote?marker=abrupt-after") as r:
            await r.read()
        rec = await wait_record(s, base, "marker=abrupt-after")
        check("D 硬断连后仍能完成双端比对", rec["result"] == "match", rec["result"])


async def main():
    servers = []
    # 影子端用真实报价服务 (SERVICE_ROLE=shadow), 主端用可控故障桩
    qs.ROLE = "shadow"
    shadow = ThreadServer(qs.app).start()
    servers.append(shadow)
    stub = StubPrimary().start()

    # 进程内交错单元测试 (先于网关启动, 便于替换 state["session"])
    await unit_never_started_task_releases_once()
    await unit_started_shadow_waits_for_comm_exit()
    await unit_shadow_completed_then_primary_error()

    gw.PRIMARY_URL = stub.url
    gw.SHADOW_URL = shadow.url
    gateway = ThreadServer(gw.app).start()
    servers.append(gateway)
    base = gateway.url

    try:
        await http_prestart_failure(base, stub.url)
        await http_inflight_failure(base, "inflight")
        await http_partial_primary(base, "partial")
        await http_interleaving(base)
        await http_recovery_and_streaming(base)
        await http_client_disconnect(base, "disconnect")
        await http_abrupt_disconnect_race(base, gateway.port)
    finally:
        for srv in servers:
            srv.stop()
        stub.stop()

    print(f"\n汇总: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("失败项:", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
