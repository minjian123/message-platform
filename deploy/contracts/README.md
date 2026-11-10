# msg 公开契约

- `msg.json`：本服务公开契约（OpenAPI）快照，由**装配后的应用**导出，纳入跨仓装载校验与契约对账。
- 导出方式（工作目录 `backend/`，用仓内统一解释器）：

  ```bash
  cd msg/backend
  PYTHONPATH=".;services/msg/src" ../../bms/backend/.venv/Scripts/python.exe - <<'EOF'
  from pathlib import Path
  from bms_core.services.service_contract import render_contract_json
  from bms_msg.main import ApplicationFactory

  spec = ApplicationFactory().create(None).openapi()
  Path("../deploy/contracts/msg.json").write_text(
      render_contract_json(spec), encoding="utf-8", newline="\n"
  )
  EOF
  ```

- 现状：8B_01_1 已导出（仅基座探针 `/`、`/healthz`、`/readyz`）；**8B_01 起每加业务端点都要重导一次**，
  否则「声明的错误码 / 能力 ↔ 契约路径」对账会漏（跨仓 `check_modules` 会提示端点未声明 `x-error-codes`）。
- **无产品码可达的端点标 `x-error-codes: []`**（空数组 = 已确认无码，如 `/msg/connection` 这类排障自报端点、
  基座挂载的 `/` `/healthz` `/readyz`）：它与「键缺失」语义不同——键缺失计入跨仓未声明告警，空数组不计入。
  基座三端点由基座自带标注，产品侧只需给自己的无码端点标注。
- 首次导出须先有占位文件：产品清单一旦声明 `[[errcodes]]`，装载期就要求契约文件存在，而真实快照只能
  由装配后的应用产出（引导期死锁）。
