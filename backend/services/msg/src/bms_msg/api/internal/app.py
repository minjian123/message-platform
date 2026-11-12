"""内部 ASGI 应用装配（02-8）：只挂 `/internal/replication/*`，监听独立端口。

**为什么单独应用**：内部面与业务面鉴权、路由、暴露面完全不同；独立应用可在 8001 上只挂内部
路由，业务 8000 完全看不到 `/internal/*`。

**共享 state**：内部应用按引用共享主应用的运行态实例（复制协调器 / 配置），不是第二个端口上的
主应用——两处读的是同一份进程内状态。
"""

from __future__ import annotations

from bms_core.core.exceptions import AuthError, BizError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from bms_msg.api.internal.replication import router

__all__ = ["create_internal_app"]


def create_internal_app(main_app: FastAPI) -> FastAPI:
    """构造内部 ASGI 应用（共享主应用 state 引用）。

    Args:
        main_app: 主应用实例（已装配复制协调器）。

    Returns:
        FastAPI: 内部应用（只含内部路由）。
    """
    app = FastAPI(
        title="msg internal replication",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    # 只共享内部端点真正需要的运行态；主应用继续持有权威引用。
    app.state.replication = getattr(main_app.state, "replication", None)
    app.state.settings = getattr(main_app.state, "settings", None)
    app.state.main_app = main_app
    app.include_router(router)

    @app.exception_handler(BizError)
    async def _biz_error_handler(request: Request, exc: BizError) -> JSONResponse:
        """把基座业务异常收敛成内部错误体（认证失败按 `auth` 归类）。

        Args:
            request: 当前请求。
            exc: 业务异常。

        Returns:
            JSONResponse: 内部错误体（不含产品路由口径）。
        """
        del request
        reason = "auth" if isinstance(exc, AuthError) else "unavailable"
        return JSONResponse(
            status_code=exc.http_status or 500,
            content={"detail": {"reason": reason, "code": exc.code, "message": str(exc)}},
        )

    return app
