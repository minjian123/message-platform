#!/usr/bin/env node
/**
 * 模块产物体积预算校验（模块产物**单独计量**，不并入宿主阈值；零依赖，CI 与本地同口径）。
 *
 * 「容器入口块」= 远端加载时**必下**的块闭包，口径与平台单一实现
 * `<bms>/frontend/scripts/module-metrics.mjs` **同源复用**（自 `dist/remoteEntry.js` 出发解析引用；
 * 种子块追静态 + 动态引用，其余只追静态引用）——跨仓引用平台脚本，不复制计量实现。
 *
 * 校验项：
 *   1. `dist/remoteEntry.js` 存在（远端容器入口契约，清单 `entry` 指向它）；
 *   2. 入口闭包 gzip 合计 ≤ `entryGzipKb`、单块 ≤ `largestGzipKb`；
 *   3. 入口闭包文件数 ≤ `maxEntryFiles`（防「分包退化、页面被并入入口」）；
 *   4. 入口闭包**文件名**不含 `pageChunkHints`（模块页面确实独立分包、未进容器入口）；
 *   5. 页面异步块数 ≥ `minAsyncChunks`（= 模块声明路由 + 插槽件异步块）。
 *
 * 用法：
 *     pnpm run build && pnpm run budget
 */
import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { join, relative } from 'node:path'
import { exit } from 'node:process'
import { fileURLToPath } from 'node:url'
import { gzipSync } from 'node:zlib'

import { REMOTE_ENTRY_FILE, collectEntryClosure } from '../../../../../bms/frontend/scripts/module-metrics.mjs'

const root = fileURLToPath(new URL('..', import.meta.url))
const budget = JSON.parse(readFileSync(join(root, 'budget.json'), 'utf8'))
const distDir = join(root, budget.distDir ?? 'dist')

const rel = (path) => relative(distDir, path).split('\\').join('/')

function walk(dir) {
  const files = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) files.push(...walk(path))
    else if (/\.(js|css)$/.test(entry.name)) files.push(path)
  }
  return files
}

let files
try {
  files = walk(distDir)
} catch {
  console.error(`[budget] 未找到构建产物目录：${distDir}（请先执行 pnpm run build）`)
  exit(1)
}

if (!existsSync(join(distDir, REMOTE_ENTRY_FILE))) {
  console.error(`[budget] 不通过：缺少远端容器入口 ${REMOTE_ENTRY_FILE}（远端形态产物契约）`)
  exit(1)
}

const entryNames = collectEntryClosure(distDir)
const entry = [...entryNames].map((name) => ({ path: name, gzip: gzipSync(readFileSync(join(distDir, name))).length }))
const rest = files
  .filter((path) => !entryNames.has(rel(path)))
  .map((path) => ({ path: rel(path), gzip: gzipSync(readFileSync(path)).length }))

const total = entry.reduce((sum, item) => sum + item.gzip, 0)
const largest = entry.reduce((max, item) => Math.max(max, item.gzip), 0)
const kb = (bytes) => (bytes / 1024).toFixed(1)

console.log(`[budget] 容器入口块 ${entry.length} 个（gzip，远端加载必下）：`)
for (const item of [...entry].sort((a, b) => b.gzip - a.gzip)) {
  console.log(`  ${kb(item.gzip).padStart(7)} KB  ${item.path}`)
}
const asyncJs = rest.filter((item) => item.path.endsWith('.js'))
console.log(`[budget] 入口外块 ${rest.length} 个（异步 / 独立预览，不计入入口预算；前 5）：`)
for (const item of [...rest].sort((a, b) => b.gzip - a.gzip).slice(0, 5)) {
  console.log(`  ${kb(item.gzip).padStart(7)} KB  ${item.path}`)
}

const hints = budget.pageChunkHints ?? []
const problems = []
if (total > budget.entryGzipKb * 1024) {
  problems.push(`容器入口合计 ${kb(total)} KB 超预算 ${budget.entryGzipKb} KB`)
}
if (largest > budget.largestGzipKb * 1024) {
  problems.push(`容器入口最大单块 ${kb(largest)} KB 超预算 ${budget.largestGzipKb} KB`)
}
if (entry.length > (budget.maxEntryFiles ?? Number.POSITIVE_INFINITY)) {
  problems.push(`容器入口块 ${entry.length} 个超上限 ${budget.maxEntryFiles}（分包可能退化）`)
}
for (const hint of hints) {
  const hit = entry.find((item) => item.path.includes(hint))
  if (hit !== undefined) {
    problems.push(`页面块进入容器入口：${hit.path}（命中 ${hint}，页面未独立分包）`)
  }
}
if (asyncJs.length < (budget.minAsyncChunks ?? 0)) {
  problems.push(`入口外异步块 ${asyncJs.length} 个少于 ${budget.minAsyncChunks}（页面独立分包失效）`)
}
for (const hint of hints) {
  if (!asyncJs.some((item) => item.path.includes(hint))) {
    problems.push(`未找到页面 / 插槽件异步块：${hint}（模块页面未独立分包）`)
  }
}

if (problems.length) {
  console.error(`[budget] 不通过：${problems.join('；')}`)
  console.error('[budget] 如需放宽预算，请在 budget.json 调整并说明原因（评审可见）。')
  exit(1)
}
console.log(
  `[budget] 通过：容器入口合计 ${kb(total)} KB（预算 ${budget.entryGzipKb} KB），` +
    `最大单块 ${kb(largest)} KB（预算 ${budget.largestGzipKb} KB），入口块 ${entry.length} 个（上限 ${budget.maxEntryFiles}），` +
    `入口外异步块 ${asyncJs.length} 个（下限 ${budget.minAsyncChunks}）`,
)
