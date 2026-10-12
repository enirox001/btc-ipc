"""Real pycapnp dispatch tests using local capabilities, with no socket binding.

These validate Python binding behavior, not communication with Bitcoin Core.
Skipped when pycapnp or the matching local schemas are unavailable.
"""
from __future__ import annotations

import asyncio
import json
import unittest

from bitcoin_ipc_client import BitcoinIPCClient, ConnectionLost
from bitcoin_ipc_client.notifications import make_callback_servers
from bitcoin_ipc_client.schemas import DEFAULT_CORE, load_schemas

try:
    import capnp
except ImportError:
    capnp = None


@unittest.skipUnless(capnp is not None and (DEFAULT_CORE / "src/ipc/capnp/chain.capnp").is_file(),
                     "pycapnp and the experimental Core schemas are required")
class CapnpTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_echo_and_rpc_roundtrips(self):
        async with capnp.kj_loop():
            modules = load_schemas(DEFAULT_CORE)
            counts = {"echo_factory": 0, "rpc_factory": 0, "echo_destroy": 0}

            class Echo(modules["echo"].Echo.Server):
                async def echo(self, context, echo, **kwargs):
                    return str(echo)

                async def destroy(self, context, **kwargs):
                    counts["echo_destroy"] += 1

            class Rpc(modules["rpc"].Rpc.Server):
                async def executeRpc(self, context, request, uri, user, **kwargs):
                    parsed = json.loads(str(request))
                    return json.dumps({"jsonrpc": "2.0", "id": parsed["id"], "result": parsed["params"]})

            class Init(modules["init"].Init.Server):
                async def makeEcho(self, context, **kwargs):
                    counts["echo_factory"] += 1
                    return Echo()

                async def makeRpc(self, context, **kwargs):
                    counts["rpc_factory"] += 1
                    return Rpc()

            client = BitcoinIPCClient("unused")
            client.connected = True
            client.ctx = modules["proxy"].Context.new_message()
            client.init = modules["init"].Init._new_client(Init())
            try:
                tasks = [client.echo(f"echo-{index}") for index in range(8)]
                tasks += [client.rpc("roundtrip", [index]) for index in range(8)]
                results = await asyncio.gather(*tasks)
                self.assertEqual(results[:8], [f"echo-{index}" for index in range(8)])
                self.assertEqual(results[8:], [[index] for index in range(8)])
                self.assertEqual(counts["echo_factory"], 1)
                self.assertEqual(counts["rpc_factory"], 1)
            finally:
                await client.close()
            self.assertEqual(counts["echo_destroy"], 1)

    async def test_thread_name_and_callback_dispatch(self):
        async with capnp.kj_loop():
            modules = load_schemas(DEFAULT_CORE)
            client = BitcoinIPCClient("unused", queue_size=4)
            local_map, local_thread, notifications = make_callback_servers(modules, client)
            thread_map = modules["proxy"].ThreadMap._new_client(local_map)
            thread = (await thread_map.makeThread("native-test")).result
            self.assertEqual((await thread.getName()).result, "native-test")
            context = modules["proxy"].Context.new_message(callbackThread=local_thread)
            callback = modules["chain"].ChainNotifications._new_client(notifications)
            role = {"validated": True, "historical": False}
            for height in range(8):
                await callback.blockConnected(context=context, role=role,
                    block={"hash": bytes(32), "prevHash": bytes(32), "height": height, "data": b"owned data"})
                self.assertLessEqual(client.events.qsize(), 4)
            events = [client.events.get_nowait() for _ in range(client.events.qsize())]
            self.assertTrue(any(event.kind == "interrupted" for event in events))
            self.assertEqual(events[-1].height, 7)
            self.assertEqual(events[-1].raw, b"owned data")

    async def test_timeout_cancels_native_request(self):
        async with capnp.kj_loop():
            modules = load_schemas(DEFAULT_CORE)

            class SlowEcho(modules["echo"].Echo.Server):
                async def echo(self, context, echo, **kwargs):
                    await asyncio.sleep(1)
                    return str(echo)

            client = BitcoinIPCClient("unused", timeout=.02)
            client.connected = True
            client.ctx = modules["proxy"].Context.new_message()
            client.echo_cap = modules["echo"].Echo._new_client(SlowEcho())
            with self.assertRaises(ConnectionLost):
                await client.echo("timeout")
            self.assertFalse(client.connected)
            await client.close()


if __name__ == "__main__":
    unittest.main()
