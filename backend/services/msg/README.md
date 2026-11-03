# msg 服务工程（8B_00 设计阶段）

当前只提供服务包骨架：服务身份、产品服务清单、表归属空清单与应用工厂。
业务域（broker / rpc / console / admin）、路由、消费组、存储与迁移在 8B_01 起按
《8B_00 总体设计》逐步接入。

## 目录

```text
services/msg/
├── pyproject.toml
├── src/bms_msg/
│   ├── __init__.py        # 服务身份 / 工程根覆盖
│   ├── main.py            # ApplicationFactory（产品装配）
│   ├── asgi.py            # ASGI 入口
│   └── __main__.py        # python -m bms_msg
└── tests/
    └── test_service_identity.py
```

> 表归属清单与业务表随详细设计登记；当前 `services/table_registry.py` 为空。
