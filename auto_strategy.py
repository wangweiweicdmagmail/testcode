"""
向后兼容入口 — 新代码请使用 auto_runner.AutoRunner。

架构：
  signals/    Alpha 信号（可插拔）
  portfolio/  组合风控
  execution/  OMS 执行
  auto_runner.py  薄编排层（Nautilus Strategy）
"""
from auto_runner import AutoRunner, AutoRunnerConfig

# 旧名称别名，main.py 无需改动
AutoStrategy = AutoRunner
AutoStrategyConfig = AutoRunnerConfig

__all__ = ["AutoRunner", "AutoRunnerConfig", "AutoStrategy", "AutoStrategyConfig"]
