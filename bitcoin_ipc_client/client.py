"""Connection lifecycle and the reusable public client."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from .errors import IPCError, ConnectionLost, UnsupportedInterface, BlockUnavailable, RPCError
from .experimental import ExperimentalChain
from .notifications import make_callback_servers
from .rpc import execute_rpc
from .schemas import DEFAULT_CORE, load_schemas, require_capnp
from .types import BlockData, BlockRef, ChainEvent

LOG = logging.getLogger("btc-ipc")

class BitcoinIPCClient:
    """Reusable asynchronous IPC connection. Owns all remote capabilities.

    One server request context, serialized calls, no implicit RPC retries.
    Reconnect is explicit. Application code reconciles state after reconnect.
    """
    def __init__(self, socket: str | Path, core_source: Path = DEFAULT_CORE, *, timeout: float = 30, queue_size: int = 256):
        if timeout <= 0 or queue_size <= 0:
            raise ValueError("timeout and queue size must be positive")
        self.socket = str(Path(socket).resolve())
        self.core_source = Path(core_source)
        self.timeout = timeout
        self.events: asyncio.Queue[ChainEvent] = asyncio.Queue(queue_size)
        self.experimental = ExperimentalChain(self)
        self.connected = False
        self.transport = self.stream = self.init = self.ctx = None
        self.chain = self.rpc_cap = self.echo_cap = self.handler = None
        self.callback_servers = None
        self.disconnect_task = None
        self._lock = asyncio.Lock()
        self._closing = False
        self.generation = 0

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def connect(self):
        if self.connected:
            return
        await self.close()
        self._closing = False
        modules = load_schemas(self.core_source)
        self.modules = modules
        capnp = require_capnp()
        try:
            try:
                self.stream = await asyncio.wait_for(capnp.AsyncIoStream.create_unix_connection(self.socket), self.timeout)
            except FileNotFoundError as exc:
                raise ConnectionLost(
                    f"IPC socket not found: {self.socket}. Start an IPC-enabled node "
                    f"with -ipcbind=unix:{self.socket}, or select its socket with --socket."
                ) from exc
            except ConnectionRefusedError as exc:
                raise ConnectionLost(f"No node is listening at IPC socket: {self.socket}") from exc
            # Larger traversal limit permits full blocks; decoded values are copied.
            self.transport = capnp.TwoPartyClient(self.stream, traversal_limit_in_words=16 * 1024 * 1024)
            self.init = self.transport.bootstrap().cast_as(modules["init"].Init)
            self.connected = True
            self.callback_servers = make_callback_servers(modules, self)
            local_map, local_thread, _ = self.callback_servers
            response = await self.call(lambda: self.init.construct(threadMap=local_map))
            # pycapnp reserves `name` in its dynamic call helper; pass this
            # schema field positionally, as Core's Python IPC tests do.
            thread = (await self.call(lambda: response.threadMap.makeThread("bitcoin-ipc-python"))).result
            self.ctx = modules["proxy"].Context.new_message(thread=thread, callbackThread=local_thread)
            self.generation += 1
            self.disconnect_task = asyncio.create_task(self._monitor_disconnect())
        except BaseException:
            await self.close()
            raise

    async def reconnect(self):
        await self.close()
        await self.connect()

    async def _monitor_disconnect(self):
        try:
            await self.transport.on_disconnect()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.debug("Disconnect monitor: %s", exc)
        finally:
            if not self._closing:
                self.connected = False
                self.interrupt_events("IPC connection lost")

    async def call(self, request):
        try:
            async with self._lock:
                if not self.connected:
                    raise ConnectionLost("IPC is disconnected; reconnect and reconcile")
                # Build/send the request only after acquiring the context lock.
                return await asyncio.wait_for(request(), self.timeout)
        except asyncio.TimeoutError as exc:
            # A timed-out mutation may already have executed; no automatic replay.
            self.connected = False
            if self.transport is not None:
                self.transport.close()
            self.interrupt_events("IPC call timed out; outcome may be unknown")
            raise ConnectionLost("IPC call timed out; outcome may be unknown") from exc
        except Exception as exc:
            if isinstance(exc, IPCError):
                raise
            kind = str(getattr(exc, "type", ""))
            if "DISCONNECTED" in kind:
                self.connected = False
                self.interrupt_events("IPC connection lost")
                raise ConnectionLost(str(exc)) from exc
            if "UNIMPLEMENTED" in kind:
                raise UnsupportedInterface(str(exc)) from exc
            raise IPCError(f"IPC call failed: {exc}") from exc

    async def chain_capability(self):
        if self.chain is None:
            self.chain = (await self.call(lambda: self.init.makeChain(context=self.ctx))).result
        return self.chain

    async def echo(self, text: str) -> str:
        if self.echo_cap is None:
            self.echo_cap = (await self.call(lambda: self.init.makeEcho(context=self.ctx))).result
        return str((await self.call(lambda: self.echo_cap.echo(context=self.ctx, echo=text))).result)

    async def rpc(self, method: str, params: list | dict | None = None) -> Any:
        """Execute RPC-over-IPC. A failed call is never automatically replayed."""
        return await execute_rpc(self, method, params)

    async def tip(self) -> BlockRef:
        # Chain.getHeight + getBlockHash is not an atomic snapshot. Deliberately
        # use RPC until a suitable typed external tip operation is evaluated.
        info = await self.rpc("getblockchaininfo")
        return BlockRef(info["bestblockhash"], int(info["blocks"]))

    async def block(self, block_hash: str, *, mode: str = "auto") -> BlockData:
        if mode not in ("auto", "typed", "rpc"):
            raise ValueError("mode must be auto, typed, or rpc")
        if mode != "rpc":
            try:
                return await self.experimental.find_block(block_hash)
            except UnsupportedInterface:
                if mode == "typed":
                    raise
                LOG.info("Typed block lookup unavailable; using RPC-over-IPC")
        header = await self.rpc("getblockheader", [block_hash])
        try:
            raw = bytes.fromhex(await self.rpc("getblock", [block_hash, 0]))
        except RPCError as exc:
            if exc.code in (-1, -5):
                raise BlockUnavailable(f"Block data unavailable: {block_hash}: {exc}") from exc
            raise
        return BlockData(block_hash, int(header["height"]), int(header["time"]), int(header["confirmations"]) > 0, raw)

    async def subscribe(self):
        if self.handler is None:
            chain = await self.chain_capability()
            self.handler = (await self.call(lambda: chain.handleNotifications(
                context=self.ctx, notifications=self.callback_servers[2],
            ))).result

    def emit(self, event: ChainEvent):
        if self._closing:
            return
        try:
            self.events.put_nowait(event)
        except asyncio.QueueFull:
            self.interrupt_events("callback queue overflow; reconciliation required")

    def interrupt_events(self, reason: str):
        while not self.events.empty():
            self.events.get_nowait()
        self.events.put_nowait(ChainEvent("interrupted", reason=reason))

    async def close(self):
        self._closing = True
        if self.connected and self.handler is not None:
            try:
                await self.call(lambda: self.handler.disconnect(context=self.ctx))
                await self.call(lambda: self.chain.waitForNotifications(context=self.ctx))
                await self.call(lambda: self.handler.destroy(context=self.ctx))
            except IPCError as exc:
                LOG.debug("Subscription cleanup: %s", exc)
        if self.connected:
            for capability in (self.chain, self.echo_cap):
                if capability is not None:
                    try:
                        await self.call(lambda: capability.destroy(context=self.ctx))
                    except IPCError as exc:
                        LOG.debug("Capability cleanup: %s", exc)
        self.connected = False
        if self.disconnect_task is not None:
            self.disconnect_task.cancel()
            await asyncio.gather(self.disconnect_task, return_exceptions=True)
        if self.transport is not None:
            self.transport.close()
        if self.stream is not None:
            self.stream.close()
        self.transport = self.stream = self.init = self.ctx = None
        self.chain = self.rpc_cap = self.echo_cap = self.handler = None
        self.callback_servers = self.disconnect_task = None
        while not self.events.empty():
            self.events.get_nowait()
