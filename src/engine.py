"""关卡流式加载 - 带5个bug"""
from dataclasses import dataclass, field
import heapq

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
        self.load_queue: list = []  # bug1: FIFO无优先级
        self.main_thread_callbacks = []
        self.player_pos = [0, 0]
        self.player_vel = [0, 0]

    def request_load(self, chunk_id):
        # bug1: 简单FIFO
        self.load_queue.append(chunk_id)

    def process_queue(self):
        if self.load_queue:
            cid = self.load_queue.pop(0)
            chunk = self.chunks[cid]
            # bug4: 不检查依赖
            chunk.loaded = True
            return cid
        return None

    def unload(self, chunk_id, player_pos):
        chunk = self.chunks[chunk_id]
        dist = abs(chunk.pos[0] - player_pos[0])
        # bug2: 只看距离不看引用
        if dist > 100:
            chunk.loaded = False

    def on_load_complete(self, chunk_id, callback):
        # bug3: 直接在加载线程执行
        callback(chunk_id)

    def predict_load(self):
        # bug5: 只看位置不看方向
        for cid, chunk in self.chunks.items():
            dist = abs(chunk.pos[0] - self.player_pos[0])
            if dist < 50 and not chunk.loaded:
                self.request_load(cid)
