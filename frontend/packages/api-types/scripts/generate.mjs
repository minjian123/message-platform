/**
 * 契约类型生成器薄壳（需求 01-9，与 bms / mdm 同款）：实现已下沉基座
 * `bms/frontend/scripts/api-types/generate.mjs`（工作区平级克隆），本文件只传本包目录；
 * 产品差异全部在 `api-types.config.json`。**前端测试只放前端**：生成器行为用例见
 * `frontend/modules/msg/tests/api-types-generation.spec.ts`（Kiwi 2463）。
 *
 * 用法（不变）：
 *   node scripts/generate.mjs           # 生成（覆盖写入）
 *   node scripts/generate.mjs --check   # 零漂移校验（不写盘）
 */

import { resolve } from 'node:path'

import { run } from '../../../../../bms/frontend/scripts/api-types/generate.mjs'

const packageDir = resolve(import.meta.dirname, '..')

process.exitCode = await run({ packageDir })
