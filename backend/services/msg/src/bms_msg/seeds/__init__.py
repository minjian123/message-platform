"""msg 默认数据种子包（产品自持 `[[seeds]]` 的入口实现；05-1）。

对外暴露 `seed_usage_forms`：平台种子编排按 `deploy/registry/msg.toml` 声明的
`module = "bms_msg.seeds.forms"` + `entry = "seed_usage_forms"` 加载，装载期校验入口可解析。
"""

from bms_msg.seeds.forms import seed_usage_forms

__all__ = ["seed_usage_forms"]
