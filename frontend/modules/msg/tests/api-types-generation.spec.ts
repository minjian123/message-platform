// kiwi_id: 2463
/**
 * 前端契约类型生成器与零漂移门禁（06-1-3）。
 *
 * **口径（2026-11-13 拍板）**：前端测试只放前端、后端测试只放后端——本用例原先写在
 * `backend/services/msg/tests/contract/`（用 pytest 跑 `node generate.mjs`），在无前端依赖的
 * `backend-test` job 里必然 `ERR_MODULE_NOT_FOUND`，故整体迁到前端 vitest（Kiwi 编号沿用 2463）。
 *
 * **单用例串行**：断言会临时改写 / 删除生成物，拆成多条会与其他 worker 的导入竞态，
 * 故全部断言收敛在一个用例内顺序执行，并用 `finally` 恢复现场。
 */

import { execFileSync } from 'node:child_process'
import { existsSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { describe, expect, it } from 'vitest'

/**
 * 自起点向上找到仓库根（以契约快照为锚点）。
 *
 * vitest 的 cwd 为本模块目录；不用 `import.meta.url` 是因为 vite 转译后它未必是 `file:` 协议，
 * `fileURLToPath` 会直接抛错。用锚点向上找，换目录结构或从子目录执行都仍然成立。
 *
 * @param start 起始目录。
 * @returns 仓库根绝对路径。
 */
function findRepoRoot(start: string): string {
  let current = start
  for (let depth = 0; depth < 8; depth += 1) {
    if (existsSync(resolve(current, 'deploy/contracts/msg.json'))) {
      return current
    }
    current = resolve(current, '..')
  }
  throw new Error(`未找到仓根（自 ${start} 向上）`)
}

const REPO = findRepoRoot(process.cwd())
const FRONTEND = resolve(REPO, 'frontend')
const SCRIPT = `${FRONTEND}/packages/api-types/scripts/generate.mjs`
const MSG_TS = `${FRONTEND}/packages/api-types/src/msg.ts`
const INDEX_TS = `${FRONTEND}/packages/api-types/src/index.ts`
const CONTRACT = resolve(REPO, 'deploy/contracts/msg.json')
const ENGKIT = resolve(REPO, 'engkit.toml')

/** 06-1-3 新增的四条管理面端点（生成物必须覆盖）。 */
const NEW_PATHS = [
  '/api/v1/msg/admin/partitions',
  '/api/v1/msg/admin/messages',
  '/api/v1/msg/admin/overview',
  '/api/v1/msg/admin/delayed/summary',
]

/** 生成器一次执行的结果。 */
interface RunResult {
  code: number
  stdout: string
  stderr: string
}

/**
 * 执行生成器脚本。
 *
 * @param args 附加参数（如 `--check`）。
 * @param cwd 工作目录（缺省为前端根）。
 * @returns 退出码与两条输出流。
 */
function run(args: string[] = [], cwd: string = FRONTEND): RunResult {
  try {
    const stdout = execFileSync('node', [SCRIPT, ...args], {
      cwd,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    return { code: 0, stdout, stderr: '' }
  } catch (error) {
    const failure = error as { status?: number | null; stdout?: string; stderr?: string }
    return { code: failure.status ?? -1, stdout: failure.stdout ?? '', stderr: failure.stderr ?? '' }
  }
}

describe('前端契约类型生成器（Kiwi 2463）', () => {
  // 子进程要跑 6 次 openapi-typescript 生成，CI 上单次可达数秒，缺省 30s 偏紧。
  it('生成 / 零漂移 / 漂移判定 / 缺失判定 / 门禁登记', { timeout: 120_000 }, () => {
    expect(existsSync(CONTRACT)).toBe(true)
    const originalMsg = readFileSync(MSG_TS)
    const originalIndex = readFileSync(INDEX_TS)
    try {
      // 生成物内容：四条新端点 + 头注 + 汇总导出
      const content = originalMsg.toString('utf8')
      expect(content).toContain('deploy/contracts/msg.json')
      expect(content).toContain('请勿手工修改')
      for (const path of NEW_PATHS) {
        expect(content).toContain(`"${path}"`)
      }
      expect(originalIndex.toString('utf8')).toContain("export type * as msg from './msg'")

      // 连续两次生成逐字节一致；生成物 LF、末尾单换行
      expect(run().code).toBe(0)
      const first = readFileSync(MSG_TS)
      expect(run().code).toBe(0)
      const second = readFileSync(MSG_TS)
      expect(second.equals(first)).toBe(true)
      expect(second.includes(Buffer.from('\r\n'))).toBe(false)
      const text = second.toString('utf8')
      expect(text.endsWith('\n')).toBe(true)
      expect(text.endsWith('\n\n')).toBe(false)

      // `--check` 无漂移通过，且从仓外工作目录调用仍能解析路径
      const clean = run(['--check'], REPO)
      expect(clean.stderr).toBe('')
      expect(clean.code).toBe(0)
      expect(clean.stdout).toContain('零漂移')
      expect(clean.stdout).toContain('info.version=')

      // 人为漂移：退出 2 并报首处差异行号
      writeFileSync(MSG_TS, text.replace('请勿手工修改', '被改坏了'), 'utf8')
      const drift = run(['--check'])
      expect(drift.code).toBe(1)
      expect(drift.stderr).toContain('漂移')
      expect(drift.stderr).toContain('行')
      writeFileSync(MSG_TS, first)

      // 生成物缺失：退出 2 且提示先跑 generate，不自动补写
      unlinkSync(INDEX_TS)
      const missing = run(['--check'])
      expect(missing.code).toBe(1)
      expect(missing.stderr).toContain('缺少生成物')
      expect(existsSync(INDEX_TS)).toBe(false)
    } finally {
      writeFileSync(MSG_TS, originalMsg)
      writeFileSync(INDEX_TS, originalIndex)
    }

    expect(run(['--check']).code).toBe(0)

    // 门禁步骤表：命令与 cwd 均为仓内相对写法
    const config = readFileSync(ENGKIT, 'utf8')
    expect(config).toContain('packages/api-types/scripts/generate.mjs')
    expect(config).toContain('"--check"')
    expect(config).toContain('cwd = "frontend"')
    expect(config).toContain('contract_snapshot')
  })
})
