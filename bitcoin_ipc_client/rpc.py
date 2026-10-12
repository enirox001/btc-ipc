"""Execute JSON RPC through Core IPC without implicit retries."""

from __future__ import annotations

import json
from typing import Any, TYPE_CHECKING

from .errors import IPCError, RPCError

if TYPE_CHECKING:
    from .client import BitcoinIPCClient

async def execute_rpc(client: BitcoinIPCClient, method: str, params: list | dict | None = None) -> Any:
    capability = await client._capability("rpc_cap", "makeRpc")
    request = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params if params is not None else []}
    response = await client.call(lambda: capability.executeRpc(
        context=client.ctx, request=json.dumps(request), uri="", user="",
    ))
    payload = json.loads(str(response.result))
    if payload.get("error") is not None:
        raise RPCError(payload["error"])
    if "result" not in payload:
        raise IPCError("Invalid RPC response: missing result")
    return payload["result"]
