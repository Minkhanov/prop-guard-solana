"""Chaos proxy: a TLS-passthrough TCP forwarder for grpc.solami.dev that you can pause.

Lets you demonstrate (and test) the guard's resilience against a *silent* stream without
touching your network: the TLS session stays up, bytes just stop flowing while paused —
exactly the failure mode the RPC fallback exists for. Resuming shows the stream recovering.

    python scripts/chaos_proxy.py --listen 127.0.0.1:9443 --target grpc.solami.dev:443

    # in .env (or the environment) for the guard:
    SOLAMI_GRPC_ENDPOINT=127.0.0.1:9443
    SOLAMI_GRPC_TLS_SERVER_NAME=grpc.solami.dev

Controls (the proxy watches a control file, default `chaos_proxy.ctl` next to this script):
    echo pause  > scripts/chaos_proxy.ctl    # freeze all forwarding (stream goes silent)
    echo resume > scripts/chaos_proxy.ctl    # let bytes flow again
    echo drop   > scripts/chaos_proxy.ctl    # close every connection once (forces a gRPC reconnect + from_slot replay)

Pure asyncio, no dependencies. Never sees plaintext (TLS terminates at Solami).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from pathlib import Path

log = logging.getLogger("chaos")


class Proxy:
    def __init__(self, target_host: str, target_port: int, ctl: Path):
        self.target = (target_host, target_port)
        self.ctl = ctl
        self.paused = asyncio.Event()
        self.paused.set()                        # set = flowing; cleared = paused
        self.conns: set[asyncio.Task] = set()
        self.writers: set[asyncio.StreamWriter] = set()
        self.bytes_up = self.bytes_down = 0

    async def pipe(self, src: asyncio.StreamReader, dst: asyncio.StreamWriter, direction: str) -> None:
        try:
            while True:
                data = await src.read(65536)
                if not data:
                    break
                await self.paused.wait()         # frozen while paused
                dst.write(data)
                await dst.drain()
                if direction == "up":
                    self.bytes_up += len(data)
                else:
                    self.bytes_down += len(data)
        except (ConnectionError, asyncio.CancelledError, OSError):
            pass
        finally:
            try:
                dst.close()
            except Exception:  # noqa: BLE001
                pass

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        try:
            r2, w2 = await asyncio.open_connection(*self.target)
        except OSError as exc:
            log.warning("cannot reach %s: %s", self.target, exc)
            writer.close()
            return
        log.info("connection %s -> %s:%d", peer, *self.target)
        self.writers.update({writer, w2})
        try:
            await asyncio.gather(self.pipe(reader, w2, "up"), self.pipe(r2, writer, "down"))
        finally:
            self.writers.difference_update({writer, w2})
            log.info("connection %s closed", peer)

    async def watch_ctl(self) -> None:
        last = ""
        while True:
            await asyncio.sleep(0.5)
            if not self.ctl.is_file():
                continue
            cmd = self.ctl.read_text(encoding="utf-8").strip().lower()
            if cmd == last:
                continue
            last = cmd
            if cmd == "pause":
                self.paused.clear()
                log.warning("PAUSED — stream is now silent (TLS session still open)")
            elif cmd == "resume":
                self.paused.set()
                log.warning("RESUMED — bytes flow again")
            elif cmd == "drop":
                n = len(self.writers)
                for w in list(self.writers):
                    w.close()
                self.paused.set()
                log.warning("DROPPED %d connection(s) — client must reconnect", n)
                self.ctl.write_text("resume", encoding="utf-8")
                last = "resume"

    async def stats(self) -> None:
        while True:
            await asyncio.sleep(10)
            log.info("up %d B, down %d B, %s", self.bytes_up, self.bytes_down, "PAUSED" if not self.paused.is_set() else "flowing")


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--listen", default="127.0.0.1:9443")
    ap.add_argument("--target", default="grpc.solami.dev:443")
    ap.add_argument("--ctl", default=str(Path(__file__).with_name("chaos_proxy.ctl")))
    args = ap.parse_args()
    lh, lp = args.listen.rsplit(":", 1)
    th, tp = args.target.rsplit(":", 1)
    ctl = Path(args.ctl)
    ctl.write_text("resume", encoding="utf-8")
    proxy = Proxy(th, int(tp), ctl)
    server = await asyncio.start_server(proxy.handle, lh, int(lp))
    log.info("chaos proxy %s -> %s:%s  (control file: %s — write pause | resume | drop)", args.listen, th, tp, ctl)
    async with server:
        await asyncio.gather(server.serve_forever(), proxy.watch_ctl(), proxy.stats())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s", datefmt="%H:%M:%S")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    _ = time
