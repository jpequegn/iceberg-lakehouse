"""Compatibility shim for vortex-data + substrait version mismatch.

vortex-data 0.58 imports from substrait.gen.proto.* but substrait >= 0.23
moved those modules to substrait.* directly. This shim creates the expected
module structure so vortex can import successfully.
"""

import importlib
import sys
import types


def _patch_substrait():
    """Patch substrait module to provide gen.proto.* aliases."""
    if "substrait.gen.proto.algebra_pb2" in sys.modules:
        return  # Already patched or not needed

    _substrait = importlib.import_module("substrait")

    # Check if gen.proto already exists (older substrait)
    try:
        importlib.import_module("substrait.gen.proto.algebra_pb2")
        return
    except (ImportError, ModuleNotFoundError):
        pass

    # Create gen package
    gen = types.ModuleType("substrait.gen")
    gen.__path__ = _substrait.__path__
    sys.modules["substrait.gen"] = gen

    # Create gen.proto package
    proto = types.ModuleType("substrait.gen.proto")
    proto.__path__ = _substrait.__path__
    sys.modules["substrait.gen.proto"] = proto

    # Map top-level protobuf modules
    sys.modules["substrait.gen.proto.algebra_pb2"] = importlib.import_module("substrait.algebra_pb2")
    sys.modules["substrait.gen.proto.extended_expression_pb2"] = importlib.import_module("substrait.extended_expression_pb2")
    sys.modules["substrait.gen.proto.type_pb2"] = importlib.import_module("substrait.type_pb2")

    # Map extensions subpackage
    extensions = types.ModuleType("substrait.gen.proto.extensions")
    extensions.__path__ = [p + "/extensions" for p in _substrait.__path__]
    sys.modules["substrait.gen.proto.extensions"] = extensions

    sys.modules["substrait.gen.proto.extensions.extensions_pb2"] = importlib.import_module("substrait.extensions.extensions_pb2")


_patch_substrait()
