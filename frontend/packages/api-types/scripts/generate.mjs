/**
 * msg 产品契约前端类型生成器：读 `deploy/contracts/msg.json`（唯一事实源），
 * 经 `openapi-typescript` 生成 `src/msg.ts` 与 `src/index.ts`。
 *
 * 与 bms / mdm 同款形态（生成物入库、`--check` 零漂移），差别只在事实源只此一份快照。
 *
 * 用法：
 *   node scripts/generate.mjs           # 生成（覆盖写入）
 *   node scripts/generate.mjs --check   # 零漂移校验（不写盘；不一致退出 2）
 */

import { existsSync } from 'node:fs'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import openapiTS, { astToString } from 'openapi-typescript'

const here = dirname(fileURLToPath(import.meta.url))
const pkgDir = resolve(here, '..')
const repoRoot = resolve(pkgDir, '../../..')
const contractPath = join(repoRoot, 'deploy', 'contracts', 'msg.json')
const srcDir = join(pkgDir, 'src')

/**
 * 生成 `msg.ts` 文件头（固定两行：来源 + 勿手改）。
 *
 * @returns 头部注释文本。
 */
function header() {
  return [
    '/**',
    ' * 产品契约生成类型：msg',
    ' *',
    ' * 本文件由 `frontend/packages/api-types/scripts/generate.mjs` 生成，请勿手工修改。',
    ' * 来源：deploy/contracts/msg.json（产品契约快照，唯一事实源）。',
    ' * 重新生成：pnpm run api-types:gen',
    ' */',
    '',
  ].join('\n')
}

/**
 * 生成 `index.ts`（按域名空间导出）。
 *
 * @returns 汇总文件内容。
 */
function indexContent() {
  return [
    '/**',
    ' * 产品契约生成类型汇总（由 scripts/generate.mjs 生成，勿手改）。',
    ' */',
    '',
    "export type * as msg from './msg'",
    '',
  ].join('\n')
}

/**
 * 定位首处不一致的行号与两侧片段（避免整份打印刷屏）。
 *
 * @param expected 期望文本。
 * @param actual 实际文本。
 * @returns 差异描述；文本一致时返回 null。
 */
function firstDiffLine(expected, actual) {
  const left = expected.split('\n')
  const right = actual.split('\n')
  const length = Math.max(left.length, right.length)
  for (let index = 0; index < length; index += 1) {
    if (left[index] !== right[index]) {
      return {
        line: index + 1,
        expected: left[index] ?? '<文件末尾>',
        actual: right[index] ?? '<文件末尾>',
      }
    }
  }
  return null
}

async function main() {
  const check = process.argv.includes('--check')
  if (!existsSync(contractPath)) {
    console.error(`[api-types] 契约快照不存在：${contractPath}`)
    process.exitCode = 2
    return
  }
  const schema = JSON.parse(await readFile(contractPath, 'utf8'))
  const expected = new Map([
    ['msg.ts', `${header()}${astToString(await openapiTS(schema))}`],
    ['index.ts', indexContent()],
  ])

  if (check) {
    for (const [name, content] of expected) {
      const path = join(srcDir, name)
      if (!existsSync(path)) {
        console.error(`[api-types] 缺少生成物：src/${name}（先跑 pnpm run api-types:gen）`)
        process.exitCode = 2
        return
      }
      const actual = await readFile(path, 'utf8')
      if (actual !== content) {
        const diff = firstDiffLine(content, actual)
        console.error(
          `[api-types] 与契约快照漂移：src/${name} 第 ${diff?.line ?? 1} 行` +
            `；期望 ${JSON.stringify(diff?.expected)}，实际 ${JSON.stringify(diff?.actual)}`,
        )
        process.exitCode = 2
        return
      }
    }
    console.log(`[api-types] 零漂移：msg 契约类型一致（info.version=${schema.info?.version ?? '未声明'}）`)
    return
  }

  await mkdir(srcDir, { recursive: true })
  for (const [name, content] of expected) {
    await writeFile(join(srcDir, name), content, 'utf8')
  }
  console.log('[api-types] 已生成 src/msg.ts + src/index.ts')
}

await main()
