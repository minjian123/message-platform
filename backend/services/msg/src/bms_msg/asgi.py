"""ASGI 入口：模块级应用实例（`uvicorn bms_msg.asgi:app`，工作目录为 `backend/`）。"""

from bms_msg.main import ApplicationFactory

app = ApplicationFactory().create(None)
