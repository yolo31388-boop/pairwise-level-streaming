"""关卡流式加载"""
from dataclasses import dataclass, field
import itertools
import math

UNLOAD_DISTANCE = 100.0
LOAD_RADIUS = 50.0
BEHIND_BUFFER = 50.0
LOOKAHEAD_SECONDS = 1.0


@dataclass
class LevelChunk:
    id: str
    pos: list
    loaded: bool = False
    refs: int = 0
    dependencies: list = field(default_factory=list)


class LevelStreaming:
    def __init__(self):
        self.chunks: dict[str, LevelChunk] = {}
        # 待加载块，出队时按当前玩家位置/速度实时计算优先级
        self._pending: dict[str, int] = {}
        self._seq = itertools.count()
        # 加载线程只投递回调，主线程排空后执行
        self.main_thread_callbacks: list = []
        # 因引用计数>0而推迟的卸载
        self.pending_unloads: set[str] = set()
        self.player_pos = [0.0, 0.0]
        self.player_vel = [0.0, 0.0]
        self.load_radius = LOAD_RADIUS
        self.unload_distance = UNLOAD_DISTANCE
        self.behind_buffer = BEHIND_BUFFER
        self.lookahead = LOOKAHEAD_SECONDS

    # ---- 加载优先级: 距离 + 方向加权 ----

    @staticmethod
    def _distance(a, b) -> float:
        return math.hypot(a[0] - b[0], a[1] - b[1])

    def _priority(self, chunk_id: str) -> float:
        """数值越小优先级越高: 距离为基准, 位于移动方向前方再减权、身后加权。"""
        chunk = self.chunks[chunk_id]
        dx = chunk.pos[0] - self.player_pos[0]
        dy = chunk.pos[1] - self.player_pos[1]
        dist = math.hypot(dx, dy)
        vx, vy = self.player_vel[0], self.player_vel[1]
        speed = math.hypot(vx, vy)
        if speed > 0.0 and dist > 0.0:
            facing = (dx * vx + dy * vy) / (dist * speed)  # 前方1, 身后-1
            dist -= facing * self.load_radius
        return dist

    def _dependencies_ready(self, chunk: LevelChunk) -> bool:
        return all(
            dep in self.chunks and self.chunks[dep].loaded
            for dep in chunk.dependencies
        )

    def request_load(self, chunk_id):
        """请求加载, 依赖以拓扑顺序一并入队。"""
        if chunk_id not in self.chunks:
            raise KeyError(chunk_id)
        if chunk_id in self._pending or self.chunks[chunk_id].loaded:
            return
        self._pending[chunk_id] = next(self._seq)
        chunk = self.chunks[chunk_id]
        for dep in chunk.dependencies:
            if dep in self.chunks:
                self.request_load(dep)

    def process_queue(self):
        """挑出优先级最高且依赖全部就绪的块加载, 无就绪块返回None。"""
        best_id = None
        best_key = None
        for cid, seq in self._pending.items():
            chunk = self.chunks[cid]
            if not self._dependencies_ready(chunk):
                continue
            key = (self._priority(cid), seq)
            if best_key is None or key < best_key:
                best_key = key
                best_id = cid
        if best_id is None:
            return None
        del self._pending[best_id]
        chunk = self.chunks[best_id]
        chunk.loaded = True
        return best_id

    # ---- 卸载: 距离 + 引用计数 + 身后缓冲 ----

    def _is_behind_buffer(self, chunk_pos, player_pos, player_vel) -> bool:
        vx, vy = player_vel[0], player_vel[1]
        speed = math.hypot(vx, vy)
        if speed <= 0.0:
            return False
        dx = chunk_pos[0] - player_pos[0]
        dy = chunk_pos[1] - player_pos[1]
        dist = math.hypot(dx, dy)
        if dist > self.unload_distance + self.behind_buffer:
            return False
        facing = (dx * vx + dy * vy) / (dist * speed) if dist > 0.0 else 1.0
        return facing < 0.0

    def unload(self, chunk_id, player_pos, player_vel=None):
        chunk = self.chunks[chunk_id]
        if not chunk.loaded:
            return False
        if self._distance(chunk.pos, player_pos) <= self.unload_distance:
            return False
        vel = self.player_vel if player_vel is None else player_vel
        if self._is_behind_buffer(chunk.pos, player_pos, vel):
            return False
        if chunk.refs > 0:
            # 仍被引用: 推迟到引用释放后重试
            self.pending_unloads.add(chunk_id)
            return False
        chunk.loaded = False
        self.pending_unloads.discard(chunk_id)
        return True

    def process_pending_unloads(self, player_pos=None, player_vel=None):
        """引用释放后重试此前推迟的卸载。"""
        pos = self.player_pos if player_pos is None else player_pos
        unloaded = []
        for cid in list(self.pending_unloads):
            if self.unload(cid, pos, player_vel):
                unloaded.append(cid)
        return unloaded

    # ---- 加载完成回调: 投递到主线程队列 ----

    def on_load_complete(self, chunk_id, callback):
        """由加载线程调用, 只入队, 不直接执行回调。"""
        self.main_thread_callbacks.append((callback, chunk_id))

    def process_main_thread_callbacks(self):
        """在主线程排空并执行回调。"""
        callbacks = self.main_thread_callbacks
        self.main_thread_callbacks = []
        for callback, chunk_id in callbacks:
            callback(chunk_id)

    def process_callbacks(self):
        return self.process_main_thread_callbacks()

    # ---- 预加载预测: 位置 + 速度向量 ----

    def predict_load(self, dt=None):
        """基于位置+速度预测未来位置, 预加载当前位置与预测位置周围的块。"""
        seconds = self.lookahead if dt is None else dt
        vx, vy = self.player_vel
        predicted = [
            self.player_pos[0] + vx * seconds,
            self.player_pos[1] + vy * seconds,
        ]
        for cid, chunk in self.chunks.items():
            if chunk.loaded or cid in self._pending:
                continue
            if (
                self._distance(chunk.pos, self.player_pos) < self.load_radius
                or self._distance(chunk.pos, predicted) < self.load_radius
            ):
                self.request_load(cid)

    def update(self):
        """每帧驱动: 预加载 -> 排空队列由外部process_queue -> 按规则卸载。"""
        self.predict_load()
        for cid in [cid for cid, c in self.chunks.items() if c.loaded]:
            self.unload(cid, self.player_pos, self.player_vel)
