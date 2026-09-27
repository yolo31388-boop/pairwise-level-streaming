"""关卡流式加载核心。

修复点:
1. 加载优先级 = 距离 + 方向加权, 面前的块优先;
2. 卸载前检查引用计数, 有引用的推迟卸载;
3. 加载完成回调投递到主线程队列, 由主线程执行;
4. 按依赖拓扑排序加载, 所有依赖就绪后才标记块就绪;
5. 预加载基于位置 + 速度向量预测未来位置, 身后保留缓冲不卸载。
"""
from dataclasses import dataclass, field
import heapq
import itertools
import math


@dataclass
class LevelChunk:
    id: str
    pos: list
    loaded: bool = False
    refs: int = 0
    dependencies: list = field(default_factory=list)
    ready: bool = False  # 自身已加载且所有依赖就绪


def _vec_sub(a, b):
    return [a[i] - b[i] for i in range(min(len(a), len(b)))]


def _vec_dot(a, b):
    return sum(a[i] * b[i] for i in range(min(len(a), len(b))))


def _vec_len(a):
    return math.sqrt(sum(x * x for x in a))


class LevelStreaming:
    # 卸载距离阈值
    UNLOAD_DISTANCE = 100
    # 预加载半径
    PRELOAD_RADIUS = 50
    # 预测未来位置时使用的前瞻时间
    LOOKAHEAD_TIME = 2.0
    # 身后缓冲距离: 该范围内即使超过卸载距离也保留
    BEHIND_BUFFER = 80
    # 方向权重: 面前单位投影对优先级分数的折扣
    DIRECTION_WEIGHT = 0.5

    def __init__(self):
        self.chunks: dict = {}
        # (priority_score, seq, chunk_id)
        self.load_queue: list = []
        self._seq = itertools.count()
        # 主线程回调队列: (chunk_id, callback)
        self.main_thread_callbacks: list = []
        # 因引用计数 > 0 而推迟卸载的块
        self.pending_unloads: set = set()
        self.player_pos = [0, 0]
        self.player_vel = [0, 0]

    # ---- bug1: 距离 + 方向加权的优先级 -------------------------------
    def _priority_score(self, chunk):
        delta = _vec_sub(chunk.pos, self.player_pos)
        dist = _vec_len(delta)
        forward = _vec_len(self.player_vel)
        if forward > 0:
            facing = _vec_dot(delta, self.player_vel) / forward
            # 面前 facing > 0 -> 分数降低(更优先); 身后 facing < 0 -> 更靠后
            return dist - self.DIRECTION_WEIGHT * facing
        return dist

    def request_load(self, chunk_id):
        chunk = self.chunks[chunk_id]
        if chunk.loaded or any(cid == chunk_id for _, _, cid in self.load_queue):
            return
        heapq.heappush(
            self.load_queue,
            (self._priority_score(chunk), next(self._seq), chunk_id),
        )

    # ---- bug4: 依赖拓扑排序 ------------------------------------------
    def _resolve_target(self, chunk_id, seen):
        """沿依赖链找到当前最该先加载的块, 已加载的块不进入返回链。"""
        if chunk_id in seen:
            return [chunk_id]
        seen.add(chunk_id)
        chunk = self.chunks[chunk_id]
        if chunk.loaded:
            return []
        for dep_id in chunk.dependencies:
            dep = self.chunks[dep_id]
            if not dep.loaded:
                return self._resolve_target(dep_id, seen) + [chunk_id]
        return [chunk_id]

    def process_queue(self):
        if not self.load_queue:
            return None
        score, _, cid = heapq.heappop(self.load_queue)
        chain = self._resolve_target(cid, set())
        if not chain:
            # 该块(及依赖)已通过其他途径加载, 继续处理队列
            return self.process_queue()
        # 链首是最深层依赖, 先加载; 其余块重新放回队列
        target = chain[0]
        for deferred in chain[1:]:
            dchunk = self.chunks[deferred]
            heapq.heappush(
                self.load_queue,
                (self._priority_score(dchunk), next(self._seq), deferred),
            )

        chunk = self.chunks[target]
        chunk.loaded = True
        if all(self.chunks[d].loaded for d in chunk.dependencies):
            chunk.ready = True
        self._refresh_ready()
        return target

    def _refresh_ready(self):
        for chunk in self.chunks.values():
            if chunk.loaded and all(self.chunks[d].loaded for d in chunk.dependencies):
                chunk.ready = True

    # ---- bug2: 引用计数 + 身后缓冲 -----------------------------------
    def _is_behind(self, chunk):
        delta = _vec_sub(chunk.pos, self.player_pos)
        return _vec_dot(delta, self.player_vel) < 0

    def _distance(self, chunk):
        return _vec_len(_vec_sub(chunk.pos, self.player_pos))

    def unload(self, chunk_id, player_pos=None):
        if player_pos is not None:
            self.player_pos = player_pos
        chunk = self.chunks[chunk_id]
        dist = self._distance(chunk)
        if dist <= self.UNLOAD_DISTANCE:
            return False
        # bug5: 身后保留一段缓冲, 避免玩家折返后重复加载
        if self._is_behind(chunk) and dist <= self.UNLOAD_DISTANCE + self.BEHIND_BUFFER:
            return False
        # bug2: 仍被引用的块推迟卸载
        if chunk.refs > 0:
            self.pending_unloads.add(chunk_id)
            return False
        chunk.loaded = False
        chunk.ready = False
        self.pending_unloads.discard(chunk_id)
        return True

    def release_ref(self, chunk_id):
        chunk = self.chunks[chunk_id]
        if chunk.refs > 0:
            chunk.refs -= 1
        if chunk.refs == 0 and chunk_id in self.pending_unloads:
            if self.unload(chunk_id):
                return True
        return False

    def process_pending_unloads(self):
        unloaded = []
        for chunk_id in list(self.pending_unloads):
            if self.unload(chunk_id):
                unloaded.append(chunk_id)
        return unloaded

    # ---- bug3: 回调投递到主线程队列 ----------------------------------
    def on_load_complete(self, chunk_id, callback):
        self.main_thread_callbacks.append((chunk_id, callback))

    def pump_callbacks(self):
        results = []
        while self.main_thread_callbacks:
            chunk_id, callback = self.main_thread_callbacks.pop(0)
            results.append(callback(chunk_id))
        return results

    # 常见别名
    run_main_thread_callbacks = pump_callbacks
    process_main_thread_callbacks = pump_callbacks

    # ---- bug5: 位置 + 速度预测预加载 ---------------------------------
    def predicted_pos(self, dt=None):
        dt = self.LOOKAHEAD_TIME if dt is None else dt
        return [
            self.player_pos[i] + self.player_vel[i] * dt
            for i in range(min(len(self.player_pos), len(self.player_vel)))
        ]

    def predict_load(self, dt=None):
        predicted = self.predicted_pos(dt)
        requested = []
        for cid, chunk in self.chunks.items():
            if chunk.loaded:
                continue
            future_dist = _vec_len(_vec_sub(chunk.pos, predicted))
            current_dist = _vec_len(_vec_sub(chunk.pos, self.player_pos))
            if future_dist < self.PRELOAD_RADIUS or current_dist < self.PRELOAD_RADIUS:
                self.request_load(cid)
                requested.append(cid)
        return requested

    def update(self, dt=None, player_pos=None, player_vel=None):
        if player_pos is not None:
            self.player_pos = player_pos
        if player_vel is not None:
            self.player_vel = player_vel
        self.predict_load(dt)
        for cid, chunk in list(self.chunks.items()):
            if chunk.loaded:
                self.unload(cid)
        return self.pump_callbacks()
