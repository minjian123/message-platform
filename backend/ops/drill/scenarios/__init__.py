"""六个故障场景模块（任务 07-6-1）。

每个模块对外只有三样：`needs_tight_watermark`（是否收紧水位）、`plan(ctx)`（dry-run 渲染）、
`run(ctx)`（真注入 + 判据）。场景定义（标题 / 观测指标 / 判据锚点 / 预算）唯一来源是
`deploy/drill/scenarios.toml`，模块里不重复写。
"""
