// kiwi_id: 2464
/** 六类只读页面与共用骨架：列表 / 详情 / 筛选 / 深链 / 四态 / 无正文 / 占位。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

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

vi.mock('../src/constants', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/constants')>()
  return { ...actual, GRAFANA_OVERVIEW_URL: 'https://grafana.example/d/msg-overview' }
})

import { ElPagination } from 'element-plus'
import ReadOnlyList from '../src/components/ReadOnlyList.vue'
import DelayedPage from '../src/views/DelayedPage.vue'
import DlqPage from '../src/views/DlqPage.vue'
import GroupDetailPage from '../src/views/GroupDetailPage.vue'
import GroupsPage from '../src/views/GroupsPage.vue'
import MessagesPage from '../src/views/MessagesPage.vue'
import OverviewPage from '../src/views/OverviewPage.vue'
import PartitionsPage from '../src/views/PartitionsPage.vue'
import TopicDetailPage from '../src/views/TopicDetailPage.vue'
import TopicsPage from '../src/views/TopicsPage.vue'

const TOPIC = {
  id: '42',
  name: 'order.created',
  namespace: 'bms',
  partition_count: 2,
  retention_ms: 86400000,
  status: 'active',
  cleanup_policy: 'delete',
  created_at: '2026-11-09T00:00:00Z',
  retention_bytes: 0,
  tenant_id: '1',
}
const PARTITION = {
  partition: 0,
  leader: 'msg-0',
  leo: 100,
  hw: null,
  isr: [],
  committed: null,
  lag: null,
  owner: null,
}
const GROUP = {
  group: 'order-sync',
  namespace: 'bms',
  topic: 'order.created',
  member_count: 1,
  assigned_partitions: 2,
  generation: 3,
  state: 'stable',
}
const MESSAGE = {
  log_id: 'log-1',
  message_id: 'msg-1',
  partition: 0,
  offset: 18,
  published_at: '2026-11-09T00:00:00Z',
  headers: { trace_id: '4bf92f' },
  payload_len: 128,
  payload_sha256: 'abcdef',
}
const DELAYED = {
  log_id: 'log-2',
  message_id: 'msg-2',
  namespace: 'bms',
  topic: 'order.created',
  partition: 0,
  level: '1s',
  deliver_at: '2026-11-09T00:01:00Z',
  remaining_ms: 900,
}
const SUMMARY = {
  hint: 'stale',
  levels: [{ level: '1s', count: 1, min_remaining_ms: 900 }],
  namespace: null,
  quota_used_ratio: 0.8,
  stale_count: 1,
  topic: null,
  total: 1,
}

/** 默认响应表（按端点路径）。 */
const DATA: Record<string, unknown> = {
  '/admin/overview': {
    consumer_lag: 1,
    disk_used_ratio: 0.5,
    dlq_total: 0,
    messages_in_per_second: 2,
    snapshot_available: true,
  },
  '/admin/topics': { items: [TOPIC], page: 1, size: 20, total: 1 },
  '/admin/topics/42': TOPIC,
  '/admin/groups': { items: [GROUP], page: 1, size: 20, total: 1 },
  '/admin/partitions': { items: [PARTITION], namespace: 'bms', topic: 'order.created', partition_count: 2, group: null },
  '/admin/messages': { items: [MESSAGE], namespace: 'bms', topic: 'order.created', page: 1, size: 20, total: 1 },
  '/admin/delayed': { items: [DELAYED], page: 1, size: 20, total: 1 },
  '/admin/delayed/summary': SUMMARY,
}

/** 装配默认取数响应（可按路径覆盖）。 */
function respond(overrides: Record<string, unknown> = {}, code = 0): void {
  scopeGet.mockImplementation(async (path: string) => {
    const key =
      path in DATA
        ? path
        : Object.keys(DATA).find((candidate) => candidate === path || path.startsWith(`${candidate}/`))
    const data = path in overrides ? overrides[path] : key === undefined ? null : DATA[key]
    return { code, message: 'ok', data }
  })
}

/** 挂载页面（注入 Element Plus）。 */
function mountView(component: unknown) {
  return mount(component as never, { global: { plugins: [ElementPlus] } })
}

/** 等待页面内多层异步取数结算。 */
async function settle(): Promise<void> {
  await flushPromises()
  await flushPromises()
  await flushPromises()
}

describe('只读列表页（Kiwi 2464）', () => {
  beforeEach(() => {
    scopeGet.mockReset()
    push.mockReset()
    permissionRef.value = undefined
    routeState.value = { params: {}, query: {} }
    respond()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('主题列表：渲染行、筛选 / 重置、详情深链按主键跳转', async () => {
    const wrapper = mountView(TopicsPage)
    await settle()
    expect(wrapper.find('[data-test="page-topics"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('order.created')
    await wrapper.find('[data-test="topics-namespace"]').setValue('bms')
    await wrapper.find('[data-test="topics-keyword"]').setValue('order')
    await wrapper.find('[data-test="topics-query"]').trigger('click')
    await settle()
    expect(scopeGet).toHaveBeenCalledWith('/admin/topics', expect.objectContaining({ namespace: 'bms' }))
    await wrapper.find('[data-test="topics-detail"]').trigger('click')
    expect(push).toHaveBeenCalledWith('/msg/topics/42')
    await wrapper.find('[data-test="topics-reset"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-test="topics-namespace"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('主题详情：元数据、分区 LEO、HW / ISR 可空占位、返回列表；加载失败按码映射', async () => {
    routeState.value = { params: { topicId: '42' }, query: {} }
    const wrapper = mountView(TopicDetailPage)
    await settle()
    expect(wrapper.find('[data-test="topic-detail-meta"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('msg-0')
    expect(wrapper.text()).toContain('—')
    await wrapper.find('[data-test="readonly-back"]').trigger('click')
    expect(push).toHaveBeenCalledWith('/msg/topics')
    wrapper.unmount()

    respond({ '/admin/topics/42': null })
    const empty = mountView(TopicDetailPage)
    await settle()
    expect(empty.find('[data-test="readonly-choose"]').exists()).toBe(true)
    empty.unmount()

    scopeGet.mockRejectedValue({ code: 350001 })
    const failed = mountView(TopicDetailPage)
    await settle()
    expect(failed.find('[data-test="topic-detail-error"]').exists()).toBe(true)
    expect(failed.text()).toContain('主题不存在或未启用')
    failed.unmount()

    respond()
    routeState.value = { params: {}, query: {} }
    const noParams = mountView(TopicDetailPage)
    await settle()
    expect(noParams.find('[data-test="page-topic-detail"]').exists()).toBe(true)
    noParams.unmount()
  })

  it('消费组列表与详情：成员 / 分配 / 分区积压 / 返回', async () => {
    const list = mountView(GroupsPage)
    await settle()
    expect(list.text()).toContain('order-sync')
    await list.find('[data-test="groups-namespace"]').setValue('bms')
    await list.find('[data-test="groups-topic"]').setValue('order.created')
    await list.find('[data-test="groups-query"]').trigger('click')
    await settle()
    expect(scopeGet).toHaveBeenCalledWith('/admin/groups', expect.objectContaining({ namespace: 'bms', topic: 'order.created' }))
    await list.find('[data-test="groups-reset"]').trigger('click')
    await settle()
    await list.find('[data-test="groups-detail"]').trigger('click')
    expect(push).toHaveBeenCalledWith('/msg/groups/bms/order.created/order-sync')
    list.unmount()

    routeState.value = { params: { namespace: 'bms', topic: 'order.created', group: 'order-sync' }, query: {} }
    scopeGet.mockImplementation(async (path: string) => ({
      code: 0,
      message: 'ok',
      data: path.startsWith('/admin/groups/')
        ? {
            ...GROUP,
            members: [{ consumer_id: 'c-1', joined_at: '', last_seen: '', partitions: [0, 1] }],
            partitions: [{ partition: 0, leo: 100, committed: 90, lag: 10, owner: 'c-1' }],
          }
        : DATA[path],
    }))
    const detail = mountView(GroupDetailPage)
    await settle()
    expect(detail.find('[data-test="group-members-table"]').exists()).toBe(true)
    expect(detail.text()).toContain('0, 1')
    expect(detail.find('[data-test="group-partitions-table"]').exists()).toBe(true)
    await detail.find('[data-test="readonly-back"]').trigger('click')
    expect(push).toHaveBeenCalledWith('/msg/groups')
    detail.unmount()
  })

  it('消费组详情：路径参数缺省与详情为空时降级为空态', async () => {
    routeState.value = { params: {}, query: {} }
    scopeGet.mockResolvedValue({ code: 0, message: 'ok', data: null })
    const wrapper = mountView(GroupDetailPage)
    await settle()
    expect(wrapper.find('[data-test="page-group-detail"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="readonly-empty"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('分区查询：未选主题为待选条件态、选择后取数、重置回待选；深链带入参数直接加载', async () => {
    const wrapper = mountView(PartitionsPage)
    await settle()
    expect(wrapper.find('[data-test="readonly-choose"]').exists()).toBe(true)
    expect(scopeGet).not.toHaveBeenCalled()
    await wrapper.find('[data-test="partitions-namespace"]').setValue('bms')
    await wrapper.find('[data-test="partitions-query"]').trigger('click')
    await settle()
    expect(scopeGet).not.toHaveBeenCalled()
    expect(wrapper.find('[data-test="readonly-choose"]').exists()).toBe(true)
    await wrapper.find('[data-test="partitions-topic"]').setValue('order.created')
    await wrapper.find('[data-test="partitions-query"]').trigger('click')
    await settle()
    expect(scopeGet).toHaveBeenCalledWith('/admin/partitions', expect.objectContaining({ namespace: 'bms' }))
    await wrapper.find('[data-test="partitions-reset"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-test="readonly-choose"]').exists()).toBe(true)
    wrapper.unmount()

    routeState.value = { params: {}, query: { namespace: 'bms', topic: 'order.created' } }
    const deep = mountView(PartitionsPage)
    await settle()
    expect(deep.text()).toContain('100')
    deep.unmount()
  })

  it('消息查询：组合条件、只展示头与摘要、消息头详情无正文入口', async () => {
    const wrapper = mountView(MessagesPage)
    await settle()
    expect(wrapper.find('[data-test="readonly-choose"]').exists()).toBe(true)
    await wrapper.find('[data-test="messages-query"]').trigger('click')
    await settle()
    expect(scopeGet).not.toHaveBeenCalled()
    await wrapper.find('[data-test="messages-namespace"]').setValue('bms')
    await wrapper.find('[data-test="messages-topic"]').setValue('order.created')
    await wrapper.find('[data-test="messages-partition"]').setValue('0')
    await wrapper.find('[data-test="messages-offset-from"]').setValue('10')
    await wrapper.find('[data-test="messages-offset-to"]').setValue('20')
    await wrapper.find('[data-test="messages-key"]').setValue('order-1')
    await wrapper.find('[data-test="messages-time-from"]').setValue('2026-11-09T00:00:00Z')
    await wrapper.find('[data-test="messages-time-to"]').setValue('2026-11-10T00:00:00Z')
    await wrapper.find('[data-test="messages-log-id"]').setValue('log-1')
    await wrapper.find('[data-test="messages-query"]').trigger('click')
    await settle()
    expect(scopeGet).toHaveBeenCalledWith('/admin/messages', expect.objectContaining({ namespace: 'bms', log_id: 'log-1' }))
    expect(wrapper.text()).toContain('abcdef')
    expect(wrapper.find('[data-test="messages-no-body"]').exists()).toBe(true)
    await wrapper.find('[data-test="messages-detail"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-test="messages-header"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('trace_id=4bf92f')
    await wrapper.find('[data-test="messages-reset"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-test="readonly-choose"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('消息头自定义头缺省：详情仍可渲染且无正文入口', async () => {
    const row = { ...MESSAGE, headers: undefined }
    scopeGet.mockImplementation(async (path: string) => ({
      code: 0,
      message: 'ok',
      data:
        path === '/admin/messages'
          ? { items: [row], namespace: 'bms', topic: 'order.created', page: 1, size: 20, total: 1 }
          : DATA[path],
    }))
    const wrapper = mountView(MessagesPage)
    await settle()
    await wrapper.find('[data-test="messages-namespace"]').setValue('bms')
    await wrapper.find('[data-test="messages-topic"]').setValue('order.created')
    await wrapper.find('[data-test="messages-query"]').trigger('click')
    await settle()
    await wrapper.find('[data-test="messages-detail"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-test="messages-header"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="messages-no-body"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('延迟消息：待投列表、九档分布、hint 三态、汇总失败只降级汇总区', async () => {
    const wrapper = mountView(DelayedPage)
    await settle()
    expect(wrapper.text()).toContain('存在已到期未转投消息')
    expect(wrapper.find('[data-test="delayed-levels-table"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="delayed-table"]').exists()).toBe(true)
    await wrapper.find('[data-test="delayed-namespace"]').setValue('bms')
    await wrapper.find('[data-test="delayed-query"]').trigger('click')
    await settle()
    expect(scopeGet).toHaveBeenCalledWith('/admin/delayed/summary', { namespace: 'bms' })
    await wrapper.find('[data-test="delayed-reset"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-test="page-delayed"]').exists()).toBe(true)
    wrapper.unmount()

    scopeGet.mockImplementation(async (path: string) =>
      path === '/admin/delayed/summary' ? Promise.reject({ code: 350009 }) : { code: 0, message: 'ok', data: DATA[path] },
    )
    const failed = mountView(DelayedPage)
    await settle()
    expect(failed.find('[data-test="delayed-summary-error"]').exists()).toBe(true)
    expect(failed.find('[data-test="delayed-table"]').exists()).toBe(true)
    failed.unmount()
  })

  it('死信与重放：两张只读表占位、后续任务说明；无权限时不渲染数据区', async () => {
    const wrapper = mountView(DlqPage)
    expect(wrapper.find('[data-test="dlq-dead-table"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="dlq-replay-table"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="dlq-later"]').exists()).toBe(true)
    expect(scopeGet).not.toHaveBeenCalled()
    wrapper.unmount()

    permissionRef.value = ['msg:manage']
    const denied = mountView(DlqPage)
    expect(denied.findAll('[data-test="readonly-denied"]')).toHaveLength(2)
    denied.unmount()
  })

  it('概览页：Grafana 地址已配置时渲染跳转入口', async () => {
    const wrapper = mountView(OverviewPage)
    await settle()
    expect(wrapper.find('[data-test="overview-grafana"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('四态：空态 / 错误态 / 加载骨架 / 无权限 / 分页事件 / 刷新与返回', async () => {
    const states = ['empty', 'error', 'denied', 'choose', 'loading'] as const
    for (const state of states) {
      const wrapper = mount(ReadOnlyList, {
        props: { title: '标题', state, textKey: state === 'error' ? 'error.350001' : 'state.empty', back: true },
        global: { plugins: [ElementPlus] },
      })
      expect(wrapper.attributes('data-state')).toBe(state)
      if (state === 'error') {
        expect(wrapper.text()).toContain('主题不存在或未启用')
      }
      if (state === 'loading') {
        expect(wrapper.find('[data-test="readonly-loading"]').exists()).toBe(true)
      }
      await wrapper.find('[data-test="readonly-refresh"]').trigger('click')
      await wrapper.find('[data-test="readonly-back"]').trigger('click')
      expect(wrapper.emitted('refresh')).toBeTruthy()
      expect(wrapper.emitted('back')).toBeTruthy()
      wrapper.unmount()
    }

    const ready = mount(ReadOnlyList, {
      props: { title: '标题', state: 'ready', page: 1, pageSize: 20, total: 40 },
      slots: { default: '<div class="row">内容</div>', filters: '<div class="filter">筛选</div>' },
      global: { plugins: [ElementPlus] },
    })
    expect(ready.find('[data-test="readonly-content"]').exists()).toBe(true)
    const pager = ready.findComponent(ElPagination)
    pager.vm.$emit('current-change', 2)
    expect(ready.emitted('update:page')?.[0]).toEqual([2])
    ready.unmount()

    const defaultText = mount(ReadOnlyList, {
      props: { title: '标题', state: 'empty' },
      global: { plugins: [ElementPlus] },
    })
    expect(defaultText.text()).toContain('暂无数据')
    defaultText.unmount()

    const noSkeleton = mount(ReadOnlyList, {
      props: { title: '标题', state: 'loading', skeleton: false },
      global: { plugins: [ElementPlus] },
    })
    expect(noSkeleton.find('[data-test="readonly-loading"]').exists()).toBe(false)
    noSkeleton.unmount()

    const noPagination = mount(ReadOnlyList, {
      props: { title: '标题', state: 'ready', paginated: false },
      global: { plugins: [ElementPlus] },
    })
    expect(noPagination.find('[data-test="readonly-pagination"]').exists()).toBe(false)
    noPagination.unmount()
  })
})
