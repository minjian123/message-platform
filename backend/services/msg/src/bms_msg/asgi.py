"""ASGI 入口：模块级应用实例（调试 / 外部 ASGI 服务器挂载用）。

**正常启动走 `python -m bms_msg`**（基座 `bms_core.core.run.run_service` → hypercorn，读 `[server]`
的 host / port / TLS 与优雅停机超时；HTTP/2 在明文下为 h2c prior knowledge，配 TLS 后由 ALPN 协商 h2）。
本模块只提供 `app` 对象，供需要 `module:app` 形态的调试工具挂载——它**不启动服务器**，也不带信号摘流。
"""

from bms_msg.main import ApplicationFactory

app = ApplicationFactory().create(None)
