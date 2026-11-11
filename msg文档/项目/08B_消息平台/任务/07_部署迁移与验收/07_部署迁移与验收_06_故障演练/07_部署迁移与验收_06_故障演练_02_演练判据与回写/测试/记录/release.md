# 发版前故障演练记录

- 任务：`07-6-2` / 模式 `release`
- 起止：`2026-11-11T05:50:44.198427+00:00` → `2026-11-11T05:56:17.895383+00:00`
- 环境：`test` / 目标 `local`
- 配置摘要：`3e2794c03684fdaca26991feeb8a6957fd694c7d4dd25b90a292f61476f56bb7`
- git 提交：`304b9a5a3bb299c374c3f5c49ad909208a587f5c`
- 复跑原因：07-6-2 最终取证复跑（判据拆分 + 记录增强）
- **汇总结论：降级通过**（退出码 2）

## 1. 场景结果

| 场景 | 结论 | 退出码 | 恢复耗时 | 记录 | sha256 |
| --- | --- | --- | --- | --- | --- |
| `process_crash` | 通过 | 0 | 13616 ms | `process_crash.json` | `8aa5d96fdf1f860862d57a44239cbf54ad4ec6a27bdc3615431d6175c24d34ac` |
| `disk_full` | 通过 | 0 | 4346 ms | `disk_full.json` | `1f1bd1fee4a0834c6928da816a53921354d9bf3df52e96742277f105a1d8d136` |
| `slow_disk` | 降级通过 | 2 | 1285 ms | `slow_disk.json` | `5f9416f2a35d9e9318d92b010d44e10809ad5be98799f9606a5fb548fa41a0dd` |
| `network_partition` | 通过 | 0 | 5237 ms | `network_partition.json` | `6dea08d88ef7b1eb701196b98ed03e2dddb572b31f234d39c34fa014ad425749` |
| `stuck_consumer` | 通过 | 0 | 0 ms | `stuck_consumer.json` | `7320e1ae28faf0dc850e189e32a0927b917d17c85d4c857fffbcd27fd40dc853` |
| `poison_message` | 降级通过 | 2 | 231534 ms | `poison_message.json` | `8adb2886e5e98c0880f1aeb25a4c21da7e704a911c5327c228823600473647d3` |

## 2. 验证结论

- 矩阵结论：降级通过（退出码 2）
- 需人工复核：是

未通过 / 降级原因：

- G11：slow_disk 最新记录为 降级通过，已归口：07-6 部署阶段（真盘 I/O 延迟注入，8B_08）
- G11：poison_message 最新记录为 降级通过，已归口：06-5 死信管理（生产同样触发，需 ACL 释放与 ORM 过期解耦后复验本场景）
- slow_disk：降级
- poison_message：降级
