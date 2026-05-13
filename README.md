"""
多Agent协同运营自动化系统 - 修正版
"""

import asyncio
import random
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List

# ---------- 消息/数据定义 ----------
@dataclass
class MarketSignal:
    source: str
    content: str
    value: float
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class Strategy:
    id: str
    action_type: str
    reasoning: str
    params: Dict[str, Any]

@dataclass
class ContentAsset:
    strategy_id: str
    asset_type: str
    data: str

# ---------- Agent基类 ----------
class BaseAgent:
    def __init__(self, name: str, inbox: asyncio.Queue, outbox_map: Dict[str, asyncio.Queue]):
        self.name = name
        self.inbox = inbox
        self.outbox_map = outbox_map

    async def send(self, target: str, message: Any):
        await self.outbox_map[target].put(message)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {self.name} -> {target}: {type(message).__name__}")

    async def run(self):
        raise NotImplementedError

# ---------- 监控Agent ----------
class MonitorAgent(BaseAgent):
    @staticmethod
    async def collect_signals() -> List[MarketSignal]:
        await asyncio.sleep(random.uniform(0.3, 0.6))
        signals = [
            MarketSignal("竞品", "竞争对手A降价5%", -0.05),
            MarketSignal("舆情", "新品话题热度上升", 0.3),
            MarketSignal("销售", "自身转化率下降2%", -0.02),
        ]
        return signals

    async def run(self):
        while True:
            signals = await self.collect_signals()
            for sig in signals:
                await self.send("StrategyAgent", sig)
            await asyncio.sleep(5)

# ---------- 策略Agent ----------
class StrategyAgent(BaseAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.history: List[Any] = []

    async def reasoning_chain(self, signal: MarketSignal) -> List[Strategy]:
        self.history.append(signal)
        print(f"  [{self.name}] 推理：{signal.source}-{signal.content}，历史窗口: {len(self.history)}")

        strategies = []
        if signal.source == "竞品" and signal.value < 0:
            strategies.append(Strategy(
                id=f"strat_{datetime.now().timestamp()}",
                action_type="price",
                reasoning="竞品降价，建议调低5%并附送赠品",
                params={"price_delta": -0.05, "gift": True}
            ))
        elif signal.source == "舆情" and signal.value > 0:
            strategies.append(Strategy(
                id=f"strat_{datetime.now().timestamp()}",
                action_type="content",
                reasoning="舆情热度上升，推出相关话题内容",
                params={"topic": "新品首发", "channel": "短视频"}
            ))
        if signal.source == "销售" and signal.value < 0:
            strategies.append(Strategy(
                id=f"strat_{datetime.now().timestamp()}",
                action_type="promotion",
                reasoning="转化率下滑，做限时满减测试",
                params={"activity": "满200减30", "duration_h": 2}
            ))
        return strategies

    async def run(self):
        while True:
            signal = await self.inbox.get()
            strategies = await self.reasoning_chain(signal)
            for s in strategies:
                await self.send("ContentAgent", s)

# ---------- 内容Agent ----------
class ContentAgent(BaseAgent):
    @staticmethod
    async def generate_asset(strategy: Strategy) -> ContentAsset:
        await asyncio.sleep(random.uniform(0.2, 0.5))
        if strategy.action_type == "price":
            data = f"商品主图：限时闪购，直降{abs(strategy.params['price_delta']*100)}%"
            asset_type = "image"
        elif strategy.action_type == "content":
            data = f"短视频脚本：开头3秒强冲突，突出{strategy.params['topic']}..."
            asset_type = "video"
        else:
            data = f"促销文案：{strategy.params['activity']}，倒计时{strategy.params['duration_h']}小时"
            asset_type = "script"
        return ContentAsset(strategy.id, asset_type, data)

    async def run(self):
        while True:
            strategy = await self.inbox.get()
            asset = await self.generate_asset(strategy)
            # 修正：直接传递目标名和消息，而非字典
            await self.send("ExecutionAgent", (strategy, asset))

# ---------- 执行Agent ----------
class ExecutionAgent(BaseAgent):
    async def execute(self, strategy: Strategy, asset: ContentAsset) -> bool:
        # 修正 asyncio 拼写及参数写法
        await asyncio.sleep(random.uniform(0.3, 0.8))
        success = random.random() > 0.1
        # 修正 print 的 f-string 语法
        print(f"  [{self.name}] 执行 {strategy.action_type}：{strategy.params}，"
              f"内容：{asset.asset_type}，结果：{'成功' if success else '失败'}")
        return success

    async def run(self):
        while True:
            strategy, asset = await self.inbox.get()
            result = await self.execute(strategy, asset)
            await self.send("ReviewAgent", {
                "strategy": strategy,
                "asset": asset,
                "result": result,
                "exec_time": datetime.now().isoformat()
            })

# ---------- 复盘Agent ----------
class ReviewAgent(BaseAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []

    async def analyze(self, exec_record: Dict) -> float:
        self.records.append(exec_record)
        score = random.uniform(0.5, 1.0) if exec_record["result"] else random.uniform(0, 0.4)
        print(f"  [{self.name}] 复盘：策略 {exec_record['strategy'].action_type} 得分 {score:.2f}")
        return score

    async def run(self):
        while True:
            record = await self.inbox.get()
            score = await self.analyze(record)
            await self.send("StrategyAgent", {
                "feedback_score": score,
                "original_strategy_id": record["strategy"].id
            })

# ---------- 系统编排 ----------
async def main():
    queues = {
        "StrategyAgent": asyncio.Queue(),
        "ContentAgent": asyncio.Queue(),
        "ExecutionAgent": asyncio.Queue(),
        "ReviewAgent": asyncio.Queue(),
    }
    monitor_inbox = asyncio.Queue()

    agents = [
        MonitorAgent("MonitorAgent", monitor_inbox, {"StrategyAgent": queues["StrategyAgent"]}),
        StrategyAgent("StrategyAgent", queues["StrategyAgent"], {"ContentAgent": queues["ContentAgent"]}),
        ContentAgent("ContentAgent", queues["ContentAgent"], {"ExecutionAgent": queues["ExecutionAgent"]}),
        ExecutionAgent("ExecutionAgent", queues["ExecutionAgent"], {"ReviewAgent": queues["ReviewAgent"]}),
        ReviewAgent("ReviewAgent", queues["ReviewAgent"], {"StrategyAgent": queues["StrategyAgent"]}),
    ]

    tasks = [asyncio.create_task(agent.run()) for agent in agents]
    print("多Agent协同运营系统已启动...")
    await asyncio.sleep(30)
    for t in tasks:
        t.cancel()
    print("演示结束。")

if __name__ == "__main__":
    asyncio.run(main())
