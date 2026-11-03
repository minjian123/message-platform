# msg 容器编排（8B_00 占位）

8B_01 起按详细设计补 `msg.yml`：服务进程 + jobs/worker（如需要）+ 网关 upstream。
当前只保留目录，避免提前固化部署形态。

口径：与 mdm / bms compose 同构，服务名 `msg`、容器内 8000、外部经网关 `/api/msg/v1/*`。
