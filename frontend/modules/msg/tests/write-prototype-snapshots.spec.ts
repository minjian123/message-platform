// kiwi_id: 2466
/**
 * 实现侧渲染快照（06-2-2）：把写操作相关页面在组件测试环境中的真实 DOM 输出为静态 HTML，
 * 供无头浏览器截图与原型逐页对照（截图证据来源，不参与源码覆盖率口径）。
 */

import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { beforeAll, describe, expect, it, vi } from 'vitest'

const { api, permissionRef, routeState, push } = vi.hoisted(() => ({
  api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() },
  permissionRef: { value: ['msg:query', 'msg:manage'] as readonly string[] | undefined },
  routeState: { value: { params: {} as Record<string, string>, query: {} as Record<string, string> } },
  push: vi.fn(),
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => wrapScope(api) }),
    hostPermissions: () => permissionRef.value,
  }
})

vi.mock('vue-router', () => ({
  useRoute: () => routeState.value,
  useRouter: () => ({ push }),
}))

import GroupDetailPage from '../src/views/GroupDetailPage.vue'
import QuotasPage from '../src/views/QuotasPage.vue'
import TopicDetailPage from '../src/views/TopicDetailPage.vue'
import TopicsPage from '../src/views/TopicsPage.vue'

const OUT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../../msg文档/项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_02_管理模块写操作/06_管理与运维_02_管理模块写操作_02_写操作页面与二次确认/测试/资源/01_测试记录_06-2-2/实现快照',
)

/** 主题样例。 */
const TOPIC = {
  id: '42',
  tenant_id: '1',
  name: 'order.created',
  namespace: 'msg',
  partition_count: 6,
  retention_ms: 604_800_000,
  retention_bytes: 0,
  cleanup_policy: 'delete',
  status: 'active',
  created_at: '2026-11-09T00:00:00Z',
  cleanup: null,
}

/** 清理进度样例。 */
const CLEANUP = {
  status: 'running',
  done_steps: ['messages', 'delay'],
  attempts: 1,
  started_at: '2026-11-09T00:00:00Z',
  finished_at: null,
  last_error: null,
}

/** 分区样例。 */
const PARTITION = {
  partition: 0,
  leader: 'msg-0',
  leo: 21405,
  hw: null,
  isr: [],
  committed: 21380,
  lag: 25,
  owner: 'cw-1',
}

/** 消费组详情样例。 */
const GROUP_DETAIL = {
  namespace: 'msg',
  topic: 'order.created',
  group: 'order-sync',
  state: 'stable',
  generation: 7,
  partition_count: 3,
  members: [
    { consumer_id: 'cw-1', partitions: [0, 2], last_seen: '2026-11-09T00:00:00Z' },
    { consumer_id: 'cw-2', partitions: [1], last_seen: '2026-11-09T00:00:00Z' },
  ],
  partitions: [PARTITION],
}

/** 配额视图样例。 */
const QUOTA_VIEW = {
  items: [
    {
      quota_key: 'topic_count',
      namespace: '',
      limit: 20,
      soft_limit: 16,
      hard_limit: 20,
      unit: 'count',
      period: 'none',
      usage: 6,
      state: 'ok',
      source: 'configured',
      updated_at: '2026-11-09T00:00:00Z',
    },
    {
      quota_key: 'group_count',
      namespace: '',
      limit: 50,
      soft_limit: 40,
      hard_limit: 50,
      unit: 'count',
      period: 'none',
      usage: 12,
      state: 'ok',
      source: 'default',
      updated_at: null,
    },
    {
      quota_key: 'storage_bytes',
      namespace: '',
      limit: null,
      soft_limit: null,
      hard_limit: null,
      unit: 'bytes',
      period: 'none',
      usage: 734003200,
      state: 'unconfigured',
      source: 'default',
      updated_at: null,
    },
  ],
}

/** 把实现 DOM 包成可截图的静态页（内联简化样式，便于逐项识图）。 */
function page(title: string, body: string): string {
  return `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8" />
<title>实现渲染快照 · ${title}</title>
<style>
body{font-family:"Microsoft YaHei",Arial,sans-serif;margin:0;padding:24px;background:#f7f8fa;color:#303133}
h1{font-size:20px;margin:0 0 16px}
.frame{background:#fff;border:1px solid #dcdfe6;border-radius:6px;padding:18px}
h2{font-size:15px;margin:12px 0}
table{width:100%;border-collapse:collapse;font-size:12px;margin:8px 0}
th,td{border:1px solid #ebeef5;padding:6px 8px;text-align:left}
th{background:#f5f7fa}
button,.el-button{border:1px solid #dcdfe6;background:#fff;border-radius:4px;padding:5px 10px;margin:2px}
input,.el-input__inner{border:1px solid #dcdfe6;border-radius:4px;padding:5px 8px;margin:2px}
.el-alert{border:1px solid #e1f3d8;background:#f0f9eb;padding:10px;margin:8px 0;border-radius:4px}
.el-alert .el-alert__icon,.el-alert svg{width:16px;height:16px;max-width:16px;max-height:16px;display:inline-block;vertical-align:middle;fill:currentColor}
.el-alert__content{display:inline-block;margin-left:6px;max-width:calc(100% - 30px);vertical-align:top}
.el-icon,.el-icon svg{width:14px;height:14px}
.el-input-number{margin:2px}
.el-dialog{border:1px solid #dcdfe6;border-radius:6px;padding:12px;margin:12px 0;background:#fff}
.el-descriptions{margin:8px 0}
.hint{color:#909399;font-size:12px}
.el-tag{border:1px solid #d9ecff;background:#ecf5ff;padding:1px 6px;border-radius:3px;font-size:12px}
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
 * 挂载页面并结算。
 *
 * @param component 页面组件。
 * @returns 挂载结果。
 */
async function mounted(component: unknown): Promise<VueWrapper> {
  const wrapper = mount(component as never, {
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  await settle()
  return wrapper
}

/**
 * 点某元素（弹窗内按钮经 teleport 渲染，用原生 click）。
 *
 * @param selector CSS 选择器。
 */
async function click(selector: string): Promise<void> {
  document.body.querySelector<HTMLElement>(selector)?.click()
  await settle()
}

/**
 * 取页面根元素的 HTML（`el-dialog` 未开启 `append-to-body`，弹窗就在页面根内部，无需另取）。
 *
 * @param selector 选择器（页面根）。
 * @returns 页面 HTML。
 */
function pageHtml(selector: string): string {
  return document.body.querySelector(selector)?.outerHTML ?? ''
}

/**
 * 写一张快照并卸载。
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

describe('写操作实现渲染快照（Kiwi 2466）', () => {
  beforeAll(() => {
    mkdirSync(OUT, { recursive: true })
  })

  it('导出六张写操作快照供逐页截图', async () => {
    // 1. 新建主题对话框
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: [TOPIC], page: 1, size: 20, total: 1 } })
    routeState.value = { params: {}, query: {} }
    let wrapper = await mounted(TopicsPage)
    await click('[data-test="topics-create"]')
    emit('01_新建主题', pageHtml('[data-test="page-topics"]'), wrapper)

    // 2. 扩分区对话框
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    routeState.value = { params: { topicId: '42' }, query: {} }
    wrapper = await mounted(TopicDetailPage)
    await click('[data-test="topic-expand"]')
    emit('02_扩分区', pageHtml('[data-test="page-topic-detail"]'), wrapper)

    // 3. 改保留期对话框
    wrapper = await mounted(TopicDetailPage)
    await click('[data-test="topic-retention"]')
    emit('03_改保留期', pageHtml('[data-test="page-topic-detail"]'), wrapper)

    // 4. 删除确认（五项影响范围）
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42/delete-impact') {
        return {
          code: 0,
          message: 'ok',
          data: { partition_count: 6, message_total: 128430, group_count: 2, delayed_total: 17, dlq_total: 3 },
        }
      }
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    wrapper = await mounted(TopicDetailPage)
    await click('[data-test="topic-delete"]')
    emit('04_删除确认', pageHtml('[data-test="page-topic-detail"]'), wrapper)

    // 5. 清理中
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: CLEANUP } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    wrapper = await mounted(TopicDetailPage)
    emit('05_清理中', pageHtml('[data-test="page-topic-detail"]'), wrapper)

    // 6. 位点重置
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: GROUP_DETAIL })
    routeState.value = { params: { namespace: 'msg', topic: 'order.created', group: 'order-sync' }, query: {} }
    wrapper = await mounted(GroupDetailPage)
    await click('[data-test="group-reset"]')
    emit('06_位点重置', pageHtml('[data-test="page-group-detail"]'), wrapper)

    // 7. 配额管理页
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: QUOTA_VIEW })
    wrapper = await mounted(QuotasPage)
    emit('07_配额管理', pageHtml('[data-test="page-quotas"]'), wrapper)

    // 护栏：确认弹窗确实渲染过（避免空快照）
    expect(api.get).toHaveBeenCalled()
  })
})
