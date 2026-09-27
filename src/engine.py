"""兼容入口: 实现位于 level/streaming.py"""
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from level.streaming import LevelChunk, LevelStreaming

__all__ = ["LevelChunk", "LevelStreaming"]
