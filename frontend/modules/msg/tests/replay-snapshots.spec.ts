// kiwi_id: 2468
/**
 * 实现侧渲染快照（06-6-2）：把「死信与重放」页的六个场景在组件测试环境中的真实 DOM 导出为静态 HTML，
 * 供无头浏览器截图与原型逐页对照（截图证据来源，不参与源码覆盖率口径）。
 */

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { beforeAll, describe, expect, it, vi } from 'vitest'

const { api, permissionRef, routeState } = vi.hoisted(() => ({
  api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() },
  permissionRef: { value: ['msg:query', 'msg:manage'] as readonly string[] | undefined },
  routeState: { value: { params: {} as Record<string, string>, query: {} as Record<string, string> } },
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => api }),
    hostPermissions: () => permissionRef.value,
  }
})

vi.mock('../src/constants', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/constants')>()
  return { ...actual, REPLAY_POLL_INTERVAL_MS: 600_000 }
})

vi.mock('vue-router', () => ({
  useRoute: () => routeState.value,
  useRouter: () => ({ push: vi.fn() }),
}))

import DlqPage from '../src/views/DlqPage.vue'

const OUT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../../msg文档/项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_06_重放任务/06_管理与运维_06_重放任务_02_重放任务页面与联动/测试/资源/01_测试记录_06-6-2/实现快照',
)

/** 组件库样式全文（内联进快照，保证证据自包含、可脱离工作区查看）。 */
const VENDOR_CSS = readFileSync(
  resolve(
    dirname(fileURLToPath(import.meta.url)),
    '../../../../../bms/bms文档/设计/原型设计/资源/vendor/element-plus.css',
  ),
  'utf8',
)

/** 死信行样例。 */
const DLQ = {
  id: '101',
  namespace: 'msg',
  topic: 'order.created',
  partition_no: 1,
  offset: 18_004,
  log_id: '01J8Y9Q7K3',
  message_id: 'msg-7f31a2',
  trace_id: '4bf92f1c7a2e',
  reason: 'processing_failed',
  attempts: 5,
  status: 'pending',
  failed_at: '2026-11-09T10:42:18Z',
  replay_task_id: null,
  disposition_reason: null,
  disposed_at: null,
  created_at: '2026-11-09T10:42:19Z',
}

/** 重放任务样例（运行中，带影响视图）。 */
const TASK = {
  task_id: '9021447701883021',
  namespace: 'msg',
  topic: 'invoice.issued',
  partitions: [0],
  offset_from: 8_900,
  offset_to: 8_920,
  time_from: null,
  time_to: null,
  key: null,
  rate_per_second: 100,
  status: 'running',
  total: 500,
  replayed: 310,
  log_id_first: '01J8YA2M4P',
  log_id_last: '01J8YB7Q1D',
  created_at: '2026-11-09T11:02:10Z',
  finished_at: null,
  error_msg: null,
  affected_groups: [
    { group: 'order-sync', baseline_lag: 1_200, current_lag: 1_840, delta: 640, captured_at: '2026-11-09T11:02:10Z' },
    { group: 'order-audit', baseline_lag: 0, current_lag: 512, delta: 512, captured_at: '2026-11-09T11:02:10Z' },
    { group: 'report-daily', baseline_lag: 3_200, current_lag: 3_200, delta: 0, captured_at: '2026-11-09T11:02:10Z' },
  ],
  affected_truncated: true,
}

/**
 * 把实现 DOM 包成可截图的静态页。
 *
 * @param title 场景名。
 * @param body 页面 HTML。
 * @returns 完整 HTML 文档。
 */
function page(title: string, body: string): string {
  return `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8" />
<title>实现渲染快照 · ${title}</title>
<style>${VENDOR_CSS}</style>
<style>
body{font-family:"Microsoft YaHei",Arial,sans-serif;margin:0;padding:24px;background:#f7f8fa;color:#303133}
h1{font-size:20px;margin:0 0 16px}
.frame{background:#fff;border:1px solid #dcdfe6;border-radius:6px;padding:18px}
/* 组件库 tabs 默认 display:flex（横向），静态页里补成纵向，贴近真实渲染 */
.el-tabs{flex-direction:column}
.el-tabs__content{overflow:visible}
</style></head><body>
<h1>实现渲染快照 · ${title}</h1>
<div class="frame">${body}</div>
</body></html>`
}

/** 等待多层异步结算。 */
async function settle(): Promise<void> {
  await flushPromises()
  await flushPromises()
  await flushPromises()
}

/**
 * 取 body 内元素。
 *
 * @param selector CSS 选择器。
 * @returns 元素（不存在返回 null）。
 */
function q(selector: string): HTMLElement | null {
  return document.body.querySelector<HTMLElement>(selector)
}

/**
 * 点击并结算。
 *
 * @param selector CSS 选择器。
 */
async function click(selector: string): Promise<void> {
  q(selector)?.click()
  await settle()
}

/**
 * 写值并派发 input。
 *
 * @param selector CSS 选择器（根元素）。
 * @param value 新值。
 */
async function type(selector: string, value: string): Promise<void> {
  const root = q(selector)
  const input = root?.tagName === 'INPUT' ? root : root?.querySelector('input')
  if (input === null || input === undefined) {
    return
  }
  input.value = value
  input.dispatchEvent(new Event('input'))
  await settle()
}

/**
 * 导出快照并卸载。
 *
 * @param name 文件名（不含后缀）。
 * @param html 页面 HTML。
 * @param wrapper 挂载结果。
 */
function emit(name: string, html: string, wrapper: VueWrapper): void {
  writeFileSync(resolve(OUT, `${name}.html`), page(name.replaceAll('_', ' · '), html), 'utf8')
  wrapper.unmount()
  document.body.innerHTML = ''
}

/**
 * 挂载死信与重放页。
 *
 * @returns 挂载结果。
 */
async function mounted(): Promise<VueWrapper> {
  const wrapper = mount(DlqPage as never, {
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  await settle()
  return wrapper
}

describe('重放任务实现渲染快照（Kiwi 2468）', () => {
  beforeAll(() => {
    mkdirSync(OUT, { recursive: true })
  })

  it('导出六个场景快照供逐页截图', async () => {
    permissionRef.value = ['msg:query', 'msg:manage']
    routeState.value = { params: {}, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/dlq') {
        return { code: 0, message: 'ok', data: { items: [DLQ], total: 1, page: 1, size: 20 } }
      }
      if (path === '/admin/dlq/101') {
        return { code: 0, message: 'ok', data: { ...DLQ, replay_task_id: TASK.task_id, detail: { error: 'NullPointerException' } } }
      }
      if (path === '/admin/replay') {
        return { code: 0, message: 'ok', data: { items: [TASK], total: 1, page: 1, size: 20 } }
      }
      if (path === '/admin/replay/9021447701883021') {
        return { code: 0, message: 'ok', data: TASK }
      }
      if (path === '/admin/messages') {
        return { code: 0, message: 'ok', data: { items: [], total: 12_480, page: 1, size: 1 } }
      }
      if (path === '/admin/groups') {
        return { code: 0, message: 'ok', data: { items: [{ group: 'order-sync' }, { group: 'order-audit' }], total: 2 } }
      }
      return { code: 0, message: 'ok', data: null }
    })
    api.post.mockResolvedValue({ code: 0, message: 'ok', data: TASK })

    // 1. 死信列表
    let wrapper = await mounted()
    emit('01_死信列表', document.body.innerHTML, wrapper)

    // 2. 重放任务列表（含高亮行）
    wrapper = await mounted()
    const tabs = wrapper.findAll('.el-tabs__item')
    await tabs[1].trigger('click')
    await settle()
    emit('02_重放任务列表', document.body.innerHTML, wrapper)

    // 3. 新建重放任务（表单）
    wrapper = await mounted()
    await wrapper.findAll('.el-tabs__item')[1].trigger('click')
    await settle()
    await click('[data-test="replay-create"]')
    await type('[data-test="replay-form-namespace"]', 'invoice')
    await type('[data-test="replay-form-topic"]', 'invoice.issued')
    await type('[data-test="replay-form-partitions"]', '0')
    emit('03_新建重放任务', document.body.innerHTML, wrapper)

    // 4. 创建确认
    await click('[data-test="replay-create-next"]')
    emit('04_创建确认', document.body.innerHTML, wrapper)

    // 5. 重放任务详情
    wrapper = await mounted()
    await wrapper.findAll('.el-tabs__item')[1].trigger('click')
    await settle()
    await click('[data-test="replay-detail"]')
    emit('05_重放任务详情', document.body.innerHTML, wrapper)

    // 6. 死信详情（关联重放任务）
    wrapper = await mounted()
    await click('[data-test="dlq-detail"]')
    emit('06_死信详情_关联任务', document.body.innerHTML, wrapper)

    expect(q('[data-test="page-dlq"]')).toBeNull()
  })
})
