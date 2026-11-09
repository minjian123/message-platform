// kiwi_id: 2464
/**
 * 实现侧渲染快照：把各只读页面在组件测试环境中的真实 DOM 输出为静态 HTML，
 * 供无头浏览器截图，与原型逐页对照（截图证据来源，不参与源码覆盖率口径）。
 */

import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import ElementPlus from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeAll, describe, expect, it, vi } from 'vitest'

const { scopeGet, permissionRef, routeState, push } = vi.hoisted(() => ({
  scopeGet: vi.fn(),
  permissionRef: { value: undefined as readonly string[] | undefined },
  routeState: { value: { params: {} as Record<string, string>, query: {} as Record<string, string> } },
  push: vi.fn(),
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => ({ get: scopeGet }) }),
    hostPermissions: () => permissionRef.value,
  }
})

vi.mock('vue-router', () => ({
  useRoute: () => routeState.value,
  useRouter: () => ({ push }),
}))

import OverviewCard from '../src/components/OverviewCard.vue'
import DelayedPage from '../src/views/DelayedPage.vue'
import DlqPage from '../src/views/DlqPage.vue'
import GroupDetailPage from '../src/views/GroupDetailPage.vue'
import GroupsPage from '../src/views/GroupsPage.vue'
import MessagesPage from '../src/views/MessagesPage.vue'
import OverviewPage from '../src/views/OverviewPage.vue'
import PartitionsPage from '../src/views/PartitionsPage.vue'
import TopicDetailPage from '../src/views/TopicDetailPage.vue'
import TopicsPage from '../src/views/TopicsPage.vue'

const OUT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../../msg文档/项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_01_管理模块只读视图/06_管理与运维_01_管理模块只读视图_02_前端管理模块骨架与只读页面/测试/资源/01_测试记录_06-1-2/实现快照',
)

const DATA: Record<string, unknown> = {
  '/admin/overview': {
    consumer_lag: 3412,
    disk_used_ratio: 0.63,
    dlq_total: 17,
    messages_in_per_second: 1280,
    snapshot_available: true,
  },
  '/admin/topics': {
    items: [
      {
        id: '42',
        name: 'order.created',
        namespace: 'bms',
        partition_count: 3,
        retention_ms: 86400000,
        status: 'active',
        cleanup_policy: 'delete',
        created_at: '2026-11-09T00:00:00Z',
        retention_bytes: 0,
        tenant_id: '1',
      },
    ],
    page: 1,
    size: 20,
    total: 1,
  },
  '/admin/topics/42': {
    id: '42',
    name: 'order.created',
    namespace: 'bms',
    partition_count: 3,
    retention_ms: 86400000,
    status: 'active',
    cleanup_policy: 'delete',
    created_at: '2026-11-09T00:00:00Z',
    retention_bytes: 0,
    tenant_id: '1',
  },
  '/admin/groups': {
    items: [
      {
        group: 'order-sync',
        namespace: 'bms',
        topic: 'order.created',
        member_count: 2,
        assigned_partitions: 3,
        generation: 7,
        state: 'stable',
      },
    ],
    page: 1,
    size: 20,
    total: 1,
  },
  '/admin/partitions': {
    items: [
      { partition: 0, leader: 'msg-0', leo: 18004, hw: null, isr: [], committed: 17990, lag: 14, owner: 'c-1' },
      { partition: 1, leader: 'msg-0', leo: 17900, hw: null, isr: [], committed: null, lag: null, owner: null },
    ],
    namespace: 'bms',
    topic: 'order.created',
    partition_count: 3,
    group: null,
  },
  '/admin/messages': {
    items: [
      {
        log_id: '01J8Y9ABCDE',
        message_id: 'msg-1',
        partition: 1,
        offset: 18004,
        published_at: '2026-11-09T10:00:00Z',
        headers: { trace_id: '4bf92f' },
        payload_len: 512,
        payload_sha256: '9f2c1d…e8a4',
      },
    ],
    namespace: 'bms',
    topic: 'order.created',
    page: 1,
    size: 20,
    total: 1,
  },
  '/admin/delayed': {
    items: [
      {
        log_id: '01J8Y7FGHIJ',
        message_id: 'msg-2',
        namespace: 'bms',
        topic: 'order.created',
        partition: 0,
        level: '5m',
        deliver_at: '2026-11-09T10:05:00Z',
        remaining_ms: 280000,
      },
    ],
    page: 1,
    size: 20,
    total: 1,
  },
  '/admin/delayed/summary': {
    hint: 'near_quota',
    levels: [
      { level: '1s', count: 3, min_remaining_ms: 200 },
      { level: '5m', count: 12, min_remaining_ms: 280000 },
      { level: '60m', count: 0, min_remaining_ms: 0 },
    ],
    namespace: null,
    quota_used_ratio: 0.86,
    stale_count: 2,
    topic: null,
    total: 17,
  },
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
.el-card,.bms-metric-card{border:1px solid #ebeef5;border-radius:4px;padding:12px;display:inline-block;min-width:150px;margin:4px}
.el-pagination{margin-top:10px;font-size:12px;color:#909399}
.hint{color:#909399;font-size:12px}
</style></head><body>
<h1>实现渲染快照 · ${title}</h1>
<div class="frame">${body}</div>
</body></html>`
}

/** 等待多层异步取数结算。 */
async function settle(): Promise<void> {
  await flushPromises()
  await flushPromises()
  await flushPromises()
}

interface SnapshotCase {
  name: string
  component: unknown
  params?: Record<string, string>
  query?: Record<string, string>
  messageQuery?: boolean
}

const CASES: SnapshotCase[] = [
  { name: '02_平台概览', component: OverviewPage },
  { name: '03_主题管理', component: TopicsPage },
  { name: '03b_主题详情', component: TopicDetailPage, params: { topicId: '42' } },
  { name: '04_消费组与积压', component: GroupsPage },
  {
    name: '04b_消费组详情',
    component: GroupDetailPage,
    params: { namespace: 'bms', topic: 'order.created', group: 'order-sync' },
  },
  {
    name: '04c_分区查询',
    component: PartitionsPage,
    query: { namespace: 'bms', topic: 'order.created' },
  },
  { name: '05_消息查询', component: MessagesPage, messageQuery: true },
  { name: '06_死信与重放', component: DlqPage },
  { name: '08_延迟消息', component: DelayedPage },
  { name: '09_工作台卡片', component: OverviewCard },
]

describe('实现渲染快照（Kiwi 2464）', () => {
  beforeAll(() => {
    mkdirSync(OUT, { recursive: true })
  })

  it('导出十个视图的真实 DOM 快照供逐页截图', async () => {
    for (const item of CASES) {
      scopeGet.mockReset()
      permissionRef.value = undefined
      routeState.value = { params: item.params ?? {}, query: item.query ?? {} }
      scopeGet.mockImplementation(async (path: string) => {
        const key =
          path in DATA
            ? path
            : Object.keys(DATA).find((candidate) => candidate === path || path.startsWith(`${candidate}/`))
        return { code: 0, message: 'ok', data: key === undefined ? null : DATA[key] }
      })
      const wrapper = mount(item.component as never, { global: { plugins: [ElementPlus] } })
      if (item.messageQuery === true) {
        await wrapper.find('[data-test="messages-namespace"]').setValue('bms')
        await wrapper.find('[data-test="messages-topic"]').setValue('order.created')
        await wrapper.find('[data-test="messages-query"]').trigger('click')
      }
      await settle()
      const html = page(item.name.replaceAll('_', ' · '), wrapper.html())
      writeFileSync(resolve(OUT, `${item.name}.html`), html, 'utf8')
      expect(html.length).toBeGreaterThan(100)
      wrapper.unmount()
    }
  })
})
