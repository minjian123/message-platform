/**
 * Vitest 全量测试守卫入口（薄包装）：判定实现唯一在 engkit `toolchain/vitest_guard.mjs`。
 *
 * 各前端包在 `vitest.config.ts` 的 `test.globalSetup` 指向本文件；本文件只做定位与转发，
 * 不写任何判定逻辑（与 `scripts/tools/preflight/*` 的 Python 薄包装同口径）。
 */

import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))

/** engkit 定位：显式 `ENGKIT_HOME` 优先，其次工作区 / CI 的平级克隆。 */
const CANDIDATES = [
  process.env.ENGKIT_HOME ? resolve(process.env.ENGKIT_HOME, 'toolchain/vitest_guard.mjs') : '',
  resolve(HERE, '../../../engkit/toolchain/vitest_guard.mjs'),
].filter(Boolean)

let loaded
let lastError
for (const candidate of CANDIDATES) {
  try {
    loaded = await import(pathToFileURL(candidate).href)
    break
  } catch (error) {
    lastError = error
  }
}

if (!loaded) {
  throw new Error(
    `[vitest_guard] 未找到 engkit：请在工作区平级克隆 engkit，或设置 ENGKIT_HOME 指向其克隆目录（${lastError}）`,
  )
}

export default loaded.default
