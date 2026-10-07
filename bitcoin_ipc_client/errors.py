"""Failures exposed to external IPC client callers."""

from __future__ import annotations

class IPCError(Exception):
    pass

class ConnectionLost(IPCError):
    pass

class UnsupportedInterface(IPCError):
    pass

class BlockUnavailable(IPCError):
    pass

class RPCError(IPCError):
    def __init__(self, error: dict):
        self.code = error.get("code")
        self.message = error.get("message", str(error))
        super().__init__(f"RPC {self.code}: {self.message}")
