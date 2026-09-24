"""Generate Python stubs from the vendored Yellowstone protos.

    python -m propguard.transports.yellowstone.gen

Protos: github.com/rpcpool/yellowstone-grpc (yellowstone-grpc-proto/proto), vendored
2026-09-24. The generated `geyser_pb2.py` imports `solana_storage_pb2` absolutely;
we rewrite it to a relative import so the package stays self-contained.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).parent
PROTO_DIR = HERE / "proto"


def main() -> int:
    try:
        import grpc_tools
        from grpc_tools import protoc
    except ImportError:
        print("grpcio-tools is not installed: pip install grpcio-tools", file=sys.stderr)
        return 2
    well_known = Path(grpc_tools.__file__).parent / "_proto"   # google/protobuf/timestamp.proto
    args = [
        "protoc",
        f"-I{PROTO_DIR}",
        f"-I{well_known}",
        f"--python_out={HERE}",
        f"--grpc_python_out={HERE}",
        str(PROTO_DIR / "solana-storage.proto"),
        str(PROTO_DIR / "geyser.proto"),
    ]
    rc = protoc.main(args)
    if rc != 0:
        print(f"protoc failed with code {rc}", file=sys.stderr)
        return rc
    for name in ("geyser_pb2.py", "geyser_pb2_grpc.py", "solana_storage_pb2_grpc.py"):
        p = HERE / name
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8")
        text = re.sub(r"^import solana_storage_pb2 as", "from . import solana_storage_pb2 as", text, flags=re.M)
        text = re.sub(r"^import geyser_pb2 as", "from . import geyser_pb2 as", text, flags=re.M)
        # geyser.proto declares `import public "solana-storage.proto"`, so protoc also emits a star import
        text = re.sub(r"^from solana_storage_pb2 import \*", "from .solana_storage_pb2 import *", text, flags=re.M)
        p.write_text(text, encoding="utf-8")
    print("generated:", ", ".join(sorted(p.name for p in HERE.glob("*_pb2*.py"))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
