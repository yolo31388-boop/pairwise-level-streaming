"""关卡流式加载测试"""
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.engine import LevelStreaming, LevelChunk

class TestPriority:
    def test_load_priority_by_distance(self):
        ls = LevelStreaming()
        ls.chunks["near"] = LevelChunk("near", [10, 0])
        ls.chunks["far"] = LevelChunk("far", [100, 0])
        ls.player_pos = [0, 0]
        ls.request_load("far")
        ls.request_load("near")
        result = ls.process_queue()
        assert result == "near", f"无优先级: {result}"

class TestUnload:
    def test_unload_checks_refcount(self):
        ls = LevelStreaming()
        ls.chunks["c"] = LevelChunk("c", [200, 0], refs=1)
        ls.unload("c", [0, 0])
        assert ls.chunks["c"].loaded or ls.chunks["c"].refs > 0, "有引用时被卸载"
