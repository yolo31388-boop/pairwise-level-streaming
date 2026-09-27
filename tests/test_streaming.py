"""关卡流式加载测试 - 覆盖五个修复点"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from level.streaming import LevelStreaming, LevelChunk


class TestPriority:
    def test_load_priority_by_distance(self):
        ls = LevelStreaming()
        ls.chunks["near"] = LevelChunk("near", [10, 0])
        ls.chunks["far"] = LevelChunk("far", [100, 0])
        ls.player_pos = [0, 0]
        ls.request_load("far")
        ls.request_load("near")
        assert ls.process_queue() == "near"

    def test_load_priority_by_direction(self):
        # 等距离时, 玩家面前(速度方向上)的块优先加载
        ls = LevelStreaming()
        ls.chunks["front"] = LevelChunk("front", [50, 0])
        ls.chunks["back"] = LevelChunk("back", [-50, 0])
        ls.player_pos = [0, 0]
        ls.player_vel = [1, 0]
        ls.request_load("back")
        ls.request_load("front")
        assert ls.process_queue() == "front"


class TestUnload:
    def test_unload_checks_refcount(self):
        ls = LevelStreaming()
        chunk = LevelChunk("c", [200, 0], loaded=True, refs=1)
        ls.chunks["c"] = chunk
        assert ls.unload("c", [0, 0]) is False
        assert chunk.loaded is True
        assert "c" in ls.pending_unloads
        # 引用释放后, 推迟的卸载得以执行
        assert ls.release_ref("c") is True
        assert chunk.loaded is False


class TestCallback:
    def test_callback_runs_on_main_thread(self):
        ls = LevelStreaming()
        calls = []
        ls.chunks["c"] = LevelChunk("c", [0, 0])

        def on_done(chunk_id):
            calls.append(chunk_id)

        ls.on_load_complete("c", on_done)
        # 完成回调不会在加载线程里立即执行
        assert calls == []
        # 主线程统一执行
        ls.pump_callbacks()
        assert calls == ["c"]


class TestDependencies:
    def test_dependency_topological_order(self):
        ls = LevelStreaming()
        ls.chunks["a"] = LevelChunk("a", [0, 0])
        ls.chunks["b"] = LevelChunk("b", [0, 0], dependencies=["a"])
        ls.chunks["c"] = LevelChunk("c", [0, 0], dependencies=["b"])
        ls.request_load("c")
        order = [ls.process_queue(), ls.process_queue(), ls.process_queue()]
        assert order == ["a", "b", "c"]

    def test_ready_only_after_all_dependencies_loaded(self):
        ls = LevelStreaming()
        a = LevelChunk("a", [0, 0])
        b = LevelChunk("b", [0, 0], dependencies=["a"])
        ls.chunks["a"] = a
        ls.chunks["b"] = b
        ls.request_load("b")
        ls.process_queue()  # 先加载依赖 a
        assert a.loaded and a.ready
        assert not b.loaded and not b.ready
        ls.process_queue()  # 再加载 b
        assert b.loaded and b.ready


class TestPrediction:
    def test_prediction_uses_velocity_and_keeps_behind_buffer(self):
        ls = LevelStreaming()
        ls.player_pos = [0, 0]
        ls.player_vel = [10, 0]
        # 身后已加载的块: 超过卸载距离但在身后缓冲内
        behind = LevelChunk("behind", [-120, 0], loaded=True)
        # 前进方向上的块: 当前距离 > 预加载半径, 但预测未来位置在半径内
        ahead = LevelChunk("ahead", [60, 0])
        ls.chunks["behind"] = behind
        ls.chunks["ahead"] = ahead

        ls.update()

        queued = [cid for _, _, cid in ls.load_queue]
        assert "ahead" in queued
        # 身后缓冲区内的块不被卸载, 玩家折返无需重新加载
        assert behind.loaded is True
