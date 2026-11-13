// kiwi_id: 2494
/** 06-2-3 管理台前端缺陷收口：时间口径 / 轮询保旧值 / 批量筛选 / 写操作反馈 / 连接列表 / 英文包。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { api, permissionRef, routeState, push } = vi.hoisted(() => ({
  api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() },
  permissionRef: { value: undefined as readonly string[] | undefined },
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

import OverviewMetrics from '../src/components/OverviewMetrics.vue'
import AclTopicSelect from '../src/components/AclTopicSelect.vue'
import MsgConfirmDialog from '../src/components/MsgConfirmDialog.vue'
import ReplayTaskPanel from '../src/components/ReplayTaskPanel.vue'
import ConnectionsPage from '../src/views/ConnectionsPage.vue'
import DelayedPage from '../src/views/DelayedPage.vue'
import DlqPage from '../src/views/DlqPage.vue'
import MessagesPage from '../src/views/MessagesPage.vue'
import QuotasPage from '../src/views/QuotasPage.vue'
import TopicDetailPage from '../src/views/TopicDetailPage.vue'
import TopicsPage from '../src/views/TopicsPage.vue'
import { applyHostContext, hostLocale, normalizeLocale } from '../src/runtime'
import { message } from '../src/i18n/messages'
import { durationText, isValidLocal, toLocalText, toUtcParam } from '../src/services/time-format'

const TOPIC = {
  id: '42',
  name: 'order.created',
  namespace: 'msg',
  partition_count: 2,
  retention_ms: 86_400_000,
  retention_bytes: 0,
  status: 'active',
  cleanup_policy: 'delete',
  created_at: '2026-11-09T00:00:00Z',
  cleanup: null,
  tenant_id: '1',
}

const CONNECTION = {
  connection_id: 'abc123def456',
  tenant_id: '7',
  remote_prefix: '10.1.*.*',
  http_version: 'HTTP/2',
  scheme: 'http',
  opened_at: '2026-11-13T10:00:00Z',
  last_seen_at: '2026-11-13T10:01:00Z',
  last_heartbeat_at: null,
  streams: 1,
  requests: 12,
  state: 'active',
}

let mounted: VueWrapper | undefined

/** 取 body 内元素（弹窗经 teleport 渲染到 body）。 */
function q(selector: string): HTMLElement | null {
  return document.body.querySelector<HTMLElement>(selector)
}

/** 写值并触发 input 事件（el-input 的 v-model 监听原生 input）。 */
async function input(selector: string, value: string): Promise<void> {
  const element = q(selector) as HTMLInputElement | null
  if (element === null) {
    throw new Error(`元素不存在：${selector}`)
  }
  element.value = value
  element.dispatchEvent(new Event('input'))
  await flushPromises()
}

/** 点击 body 内元素。 */
async function click(selector: string): Promise<void> {
  const element = q(selector)
  if (element === null) {
    throw new Error(`元素不存在：${selector}`)
  }
  element.click()
  await flushPromises()
}

/** 挂载页面（Element Plus + 默认附到 body）。 */
function mountPage(component: unknown): VueWrapper {
  mounted = mount(component as never, {
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  return mounted
}

beforeEach(() => {
  permissionRef.value = undefined
  routeState.value = { params: {}, query: {} }
  api.get.mockReset()
  api.post.mockReset()
  api.patch.mockReset()
  api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: [], page: 1, size: 20, total: 0 } })
})

afterEach(() => {
  mounted?.unmount()
  mounted = undefined
  applyHostContext({} as never)
})

describe('时间口径（纯函数）', () => {
  it('本地时间 → UTC 带 Z；展示转回本地', () => {
    const utc = toUtcParam('2026-11-13T20:00:00')
    expect(utc).not.toBeNull()
    expect(utc?.endsWith('Z')).toBe(true)
    expect(toLocalText(utc)).toBe('2026-11-13 20:00:00')
  })

  it('空值与非法值：空返回 null，非法返回 null，展示占位符', () => {
    expect(toUtcParam('')).toBeNull()
    expect(toUtcParam(null)).toBeNull()
    expect(toUtcParam(undefined)).toBeNull()
    expect(toUtcParam('not-a-time')).toBeNull()
    expect(toLocalText(null)).toBe('—')
    expect(toLocalText('not-a-time')).toBe('—')
    expect(toLocalText('')).toBe('—')
    expect(toLocalText(undefined)).toBe('—')
  })

  it('空格分隔的本地时间同样可转 UTC', () => {
    expect(toLocalText(toUtcParam('2026-11-13 08:30:00'))).toBe('2026-11-13 08:30:00')
  })

  it('时长按最大可读单位展示；非法值占位', () => {
    expect(durationText(2_592_000_000)).toBe('30 天')
    expect(durationText(43_200_000)).toBe('12 小时')
    expect(durationText(2_700_000)).toBe('45 分钟')
    expect(durationText(30_000)).toBe('30 秒')
    expect(durationText(null)).toBe('—')
    expect(durationText(-1)).toBe('—')
    expect(durationText(Number.NaN)).toBe('—')
  })

  it('本地时间合法性：空串放行、非法拒绝', () => {
    expect(isValidLocal('')).toBe(true)
    expect(isValidLocal(undefined)).toBe(true)
    expect(isValidLocal('2026-11-13T20:00:00')).toBe(true)
    expect(isValidLocal('xxx')).toBe(false)
  })
})

describe('语言解析与英文包', () => {
  it('归一化：en-US → en，其它 → zh-cn', () => {
    expect(normalizeLocale('en-US')).toBe('en')
    expect(normalizeLocale('EN')).toBe('en')
    expect(normalizeLocale('zh-CN')).toBe('zh-cn')
    expect(normalizeLocale(undefined)).toBe('zh-cn')
  })

  it('宿主语言优先：i18n.defaultLocale 为英文时文案生效；locales() 兜底', () => {
    applyHostContext({ i18n: { defaultLocale: 'en-US' } } as never)
    expect(hostLocale()).toBe('en')
    expect(message('msg.admin.common.query')).toBe('Query')
    expect(message('msg.admin.common.query', 'zh-cn')).toBe('查询')

    applyHostContext({ i18n: { locales: () => ['en'] } } as never)
    expect(hostLocale()).toBe('en')

    applyHostContext({ i18n: { locale: 'en' } } as never)
    expect(hostLocale()).toBe('en')

    applyHostContext({ i18n: { locales: () => [] } } as never)
    expect(hostLocale()).toBe('zh-cn')

    applyHostContext({ i18n: {} } as never)
    expect(hostLocale()).toBe('zh-cn')

    applyHostContext({ i18n: 'zh' } as never)
    expect(hostLocale()).toBe('zh-cn')

    applyHostContext({ user: { locale: 'en-GB' } } as never)
    expect(hostLocale()).toBe('en')

    applyHostContext({ user: {} } as never)
    expect(hostLocale()).toBe('zh-cn')

    applyHostContext({} as never)
    expect(hostLocale()).toBe('zh-cn')
  })
})

describe('连接列表页（06-1-4 端点）', () => {
  it('渲染连接行、状态文案与脱敏地址', async () => {
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: [CONNECTION], page: 1, size: 20, total: 1 } })
    const wrapper = mountPage(ConnectionsPage)
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith('/admin/connections', { page: 1, size: 20 })
    expect(wrapper.text()).toContain('10.1.*.*')
    expect(wrapper.text()).toContain('活跃')
    expect(wrapper.find('[data-test="connections-single-instance"]').exists()).toBe(true)
  })

  it('未知状态回落原值；空页显示空态', async () => {
    api.get.mockResolvedValue({
      code: 0,
      message: 'ok',
      data: { items: [{ ...CONNECTION, state: 'weird' }], page: 1, size: 20, total: 1 },
    })
    const wrapper = mountPage(ConnectionsPage)
    await flushPromises()
    expect(wrapper.text()).toContain('weird')

    mounted?.unmount()
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: [], page: 1, size: 20, total: 0 } })
    const empty = mountPage(ConnectionsPage)
    await flushPromises()
    expect(empty.find('.el-empty').exists() || empty.text().includes('暂无数据')).toBe(true)
  })

  it('无 msg:query：不取数，展示无权限态', async () => {
    permissionRef.value = []
    const wrapper = mountPage(ConnectionsPage)
    await flushPromises()
    expect(api.get).not.toHaveBeenCalled()
    expect(wrapper.text()).toContain('无「msg:query」查询权限')
  })
})

describe('概览轮询保留旧值', () => {
  const OVERVIEW = {
    consumer_lag: 10,
    disk_used_ratio: 0.5,
    dlq_total: 2,
    messages_in_per_second: 1280,
    snapshot_available: true,
  }

  it('轮询刷新（loading 且非首次）仍渲染上次数值；失败保留数值并提示', async () => {
    const wrapper = mount(OverviewMetrics, {
      props: { overview: OVERVIEW, state: 'loading', firstLoad: false },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    expect(wrapper.text()).toContain('1,280')
    expect(wrapper.text()).not.toContain('加载中…')
    wrapper.unmount()

    const failed = mount(OverviewMetrics, {
      props: { overview: OVERVIEW, state: 'error', textKey: 'error.350001', firstLoad: false },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    expect(failed.find('[data-test="overview-error"]').exists()).toBe(true)
    expect(failed.text()).toContain('1,280')
    failed.unmount()
  })

  it('首次加载占位与上次更新文案', async () => {
    const wrapper = mount(OverviewMetrics, {
      props: { overview: null, state: 'loading', firstLoad: true, lastUpdated: new Date(2026, 10, 13, 12, 0, 0) },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    expect(wrapper.text()).toContain('加载中…')
    expect(wrapper.find('[data-test="overview-updated"]').exists()).toBe(true)
    wrapper.unmount()
  })
})

describe('新建主题：保留期默认与校验', () => {
  it('默认 30 天，提交体为 2592000000 毫秒', async () => {
    const wrapper = mountPage(TopicsPage)
    await flushPromises()
    await click('[data-test="topics-create"]')
    const numbers = wrapper.findAllComponents({ name: 'ElInputNumber' })
    expect(numbers[1]?.props('modelValue')).toBe(30)
    await input('[data-test="create-name"]', 'order.paid')
    api.post.mockResolvedValue({ code: 0, message: 'ok', data: TOPIC })
    await click('[data-test="create-submit"]')
    const [, body] = api.post.mock.calls[0] as [string, Record<string, unknown>]
    expect(body['retention_ms']).toBe(2_592_000_000)
  })

  it('非法主题名 / 空命名空间就地拦截，不发请求', async () => {
    mountPage(TopicsPage)
    await flushPromises()
    await click('[data-test="topics-create"]')
    await input('[data-test="create-name"]', 'Bad Name')
    await click('[data-test="create-submit"]')
    expect(q('[data-test="create-validation"]')?.textContent).toContain('小写字母')
    expect(api.post).not.toHaveBeenCalled()
  })
})

describe('主题详情：保留期无变更与清理进度字段', () => {
  it('两项未变更：保存按钮禁用且提示未做修改', async () => {
    routeState.value = { params: { topicId: '42' }, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      return { code: 0, message: 'ok', data: { items: [], total: 0 } }
    })
    const wrapper = mountPage(TopicDetailPage)
    await flushPromises()
    await wrapper.find('[data-test="topic-retention"]').trigger('click')
    await flushPromises()
    expect(wrapper.find('[data-test="retention-submit"]').attributes('disabled')).toBeDefined()
    expect(wrapper.find('[data-test="retention-no-change"]').exists()).toBe(true)
  })

  it('清理中：展示已尝试轮次 / 开始时间 / 最后错误', async () => {
    routeState.value = { params: { topicId: '42' }, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return {
          code: 0,
          message: 'ok',
          data: {
            ...TOPIC,
            status: 'deleting',
            cleanup: {
              status: 'running',
              done_steps: ['messages'],
              attempts: 3,
              started_at: '2026-11-13T10:00:00Z',
              finished_at: null,
              last_error: 'disk busy',
            },
          },
        }
      }
      return { code: 0, message: 'ok', data: { items: [], total: 0 } }
    })
    const wrapper = mountPage(TopicDetailPage)
    await flushPromises()
    expect(wrapper.find('[data-test="topic-cleanup-metrics"]').text()).toContain('3')
    expect(wrapper.text()).toContain('disk busy')
    expect(wrapper.text()).toContain('开始时间')
  })
})

describe('消息查询与延迟筛选', () => {
  it('深链带入 log_id 与查询参数，一次请求完成', async () => {
    routeState.value = { params: { logId: 'log-9' }, query: { namespace: 'msg', topic: 'order.created' } }
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: [], page: 1, size: 20, total: 0 } })
    const wrapper = mountPage(MessagesPage)
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith(
      '/admin/messages',
      expect.objectContaining({ namespace: 'msg', topic: 'order.created', log_id: 'log-9' }),
    )
    expect(wrapper.find('[data-test="messages-log-id"]').element).toHaveProperty('value', 'log-9')
  })

  it('延迟页：主题 / 档位筛选一次提交，档位占比按总数计算', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/delayed/summary') {
        return {
          code: 0,
          message: 'ok',
          data: {
            total: 4,
            stale_count: 0,
            quota_used_ratio: 0.1,
            hint: 'normal',
            levels: [{ level: '1s', count: 1, min_remaining_ms: 900 }],
            namespace: null,
            topic: null,
          },
        }
      }
      return { code: 0, message: 'ok', data: { items: [], page: 1, size: 20, total: 0 } }
    })
    const wrapper = mountPage(DelayedPage)
    await flushPromises()
    expect(wrapper.text()).toContain('25.0%')

    const inputs = wrapper.findAllComponents({ name: 'ElInput' })
    inputs.find((item) => item.props('placeholder') === '主题过滤')?.vm.$emit('update:modelValue', 'order.created')
    wrapper.findAllComponents({ name: 'ElSelect' })[0]?.vm.$emit('update:modelValue', '1s')
    await flushPromises()
    await wrapper.find('[data-test="delayed-query"]').trigger('click')
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith('/admin/delayed', expect.objectContaining({ topic: 'order.created', level: '1s' }))
  })
})

describe('配额页：非法值与只读态', () => {
  const QUOTA_VIEW = {
    items: [
      {
        quota_key: 'topic_count',
        namespace: '',
        limit: 10,
        soft_limit: 8,
        hard_limit: 10,
        unit: 'count',
        period: 'none',
        usage: 3,
        state: 'ok',
        source: 'configured',
        updated_at: '2026-11-09T00:00:00Z',
      },
    ],
  }

  it('负数 / 非整数：就地告警且保存禁用', async () => {
    permissionRef.value = ['msg:query', 'msg:manage']
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: QUOTA_VIEW })
    const wrapper = mountPage(QuotasPage)
    await flushPromises()
    wrapper.findAllComponents({ name: 'ElInput' })[0]?.vm.$emit('update:modelValue', '-1')
    await flushPromises()
    expect(wrapper.find('[data-test="quotas-invalid-warning"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="quotas-save"]').attributes('disabled')).toBeDefined()
  })

  it('无 msg:manage：输入只读并提示', async () => {
    permissionRef.value = ['msg:query']
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: QUOTA_VIEW })
    const wrapper = mountPage(QuotasPage)
    await flushPromises()
    expect(wrapper.find('[data-test="quotas-readonly"]').exists()).toBe(true)
    expect(wrapper.findAllComponents({ name: 'ElInput' })[0]?.props('disabled')).toBe(true)
    expect(wrapper.find('[data-test="quotas-save"]').exists()).toBe(false)
  })
})

describe('ACL 主题候选：同名主题跨命名空间可区分', () => {
  it('候选键为「命名空间/主题名」', async () => {
    api.get.mockResolvedValue({
      code: 0,
      message: 'ok',
      data: {
        items: [
          { ...TOPIC, namespace: 'msg', name: 'same' },
          { ...TOPIC, namespace: 'other', name: 'same' },
        ],
        page: 1,
        size: 20,
        total: 2,
      },
    })
    const wrapper = mount(AclTopicSelect, {
      props: { modelValue: '', namespace: '' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    await flushPromises()
    const values = wrapper.findAllComponents({ name: 'ElOption' }).map((item) => item.props('value'))
    expect(values).toEqual(['msg/same', 'other/same'])
    wrapper.unmount()
  })
})

describe('补充脚本分支', () => {
  it('新建主题：命名空间 / 分区 / 保留期三条校验各自拦截', async () => {
    const wrapper = mountPage(TopicsPage)
    await flushPromises()
    await click('[data-test="topics-create"]')
    await input('[data-test="create-name"]', 'order.paid')
    wrapper.findAllComponents({ name: 'ElSelect' })[0]?.vm.$emit('update:modelValue', '')
    await click('[data-test="create-submit"]')
    expect(q('[data-test="create-validation"]')?.textContent).toContain('请选择命名空间')

    wrapper.findAllComponents({ name: 'ElSelect' })[0]?.vm.$emit('update:modelValue', 'msg')
    wrapper.findAllComponents({ name: 'ElInputNumber' })[0]?.vm.$emit('update:modelValue', 0)
    await click('[data-test="create-submit"]')
    expect(q('[data-test="create-validation"]')?.textContent).toContain('分区数须在')

    wrapper.findAllComponents({ name: 'ElInputNumber' })[0]?.vm.$emit('update:modelValue', 1)
    wrapper.findAllComponents({ name: 'ElInputNumber' })[1]?.vm.$emit('update:modelValue', 0)
    await click('[data-test="create-submit"]')
    expect(q('[data-test="create-validation"]')?.textContent).toContain('保留期不得少于')
    expect(api.post).not.toHaveBeenCalled()
  })

  it('主题列表：状态枚举本地化', async () => {
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: [TOPIC], page: 1, size: 20, total: 1 } })
    const wrapper = mountPage(TopicsPage)
    await flushPromises()
    expect(wrapper.text()).toContain('正常')
  })

  it('消息查询：时间格式非法就地提示且不发请求', async () => {
    const wrapper = mountPage(MessagesPage)
    await flushPromises()
    wrapper.findAllComponents({ name: 'ElDatePicker' })[0]?.vm.$emit('update:modelValue', 'bad-time')
    await flushPromises()
    await wrapper.find('[data-test="messages-query"]').trigger('click')
    await flushPromises()
    expect(wrapper.find('[data-test="messages-time-error"]').exists()).toBe(true)
    expect(api.get).not.toHaveBeenCalled()
  })

  it('死信：时间格式非法就地提示且不发请求', async () => {
    const wrapper = mountPage(DlqPage)
    await flushPromises()
    wrapper.findAllComponents({ name: 'ElDatePicker' })[0]?.vm.$emit('update:modelValue', 'bad-time')
    await flushPromises()
    await wrapper.find('[data-test="dlq-query"]').trigger('click')
    await flushPromises()
    expect(wrapper.find('[data-test="dlq-time-error"]').exists()).toBe(true)
  })

  it('死信：时间留空按「不带区间」查询', async () => {
    const wrapper = mountPage(DlqPage)
    await flushPromises()
    await wrapper.find('[data-test="dlq-query"]').trigger('click')
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith('/admin/dlq', expect.not.objectContaining({ failed_from: expect.anything() }))
  })

  it('延迟页：待投总数为 0 时占比记 0.0%', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/delayed/summary') {
        return {
          code: 0,
          message: 'ok',
          data: {
            total: 0,
            stale_count: 0,
            quota_used_ratio: 0,
            hint: 'normal',
            levels: [{ level: '1s', count: 1, min_remaining_ms: 900 }],
            namespace: null,
            topic: null,
          },
        }
      }
      return { code: 0, message: 'ok', data: { items: [], page: 1, size: 20, total: 0 } }
    })
    const wrapper = mountPage(DelayedPage)
    await flushPromises()
    expect(wrapper.text()).toContain('0.0%')
  })

  it('主题详情：清理轮询连续失败 3 次即暂停并提示', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    routeState.value = { params: { topicId: '42' }, query: {} }
    let calls = 0
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        calls += 1
        if (calls === 1) {
          return {
            code: 0,
            message: 'ok',
            data: {
              ...TOPIC,
              status: 'deleting',
              cleanup: { status: 'running', done_steps: [], attempts: 1, started_at: null, finished_at: null, last_error: null },
            },
          }
        }
        return Promise.reject({ code: 10000 })
      }
      return { code: 0, message: 'ok', data: { items: [], total: 0 } }
    })
    const wrapper = mountPage(TopicDetailPage)
    await flushPromises()
    for (let index = 0; index < 3; index += 1) {
      await vi.advanceTimersByTimeAsync(3_000)
      await flushPromises()
    }
    expect(wrapper.find('[data-test="topic-detail-error"]').text()).toContain('进度刷新已暂停')
    vi.useRealTimers()
  })

  it('连接列表：接口返回 null 时回落空页', async () => {
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: null })
    const { fetchConnections } = await import('../src/services/admin-service')
    const page = await fetchConnections()
    expect(page).toEqual({ items: [], page: 1, size: 20, total: 0 })
  })

  it('重放表单：时间非法返回通用时间错误', async () => {
    const { validateReplayForm } = await import('../src/services/replay-format')
    expect(
      validateReplayForm({
        namespace: 'msg',
        topic: 't',
        offsetFrom: undefined,
        offsetTo: undefined,
        timeFrom: 'bad',
        timeTo: '',
        key: '',
        partitions: '',
        rate: 1,
      }),
    ).toBe('msg.admin.common.invalidTime')
  })

  it('确认弹窗：error 文案渲染在弹窗内部', async () => {
    const wrapper = mount(MsgConfirmDialog, {
      props: { modelValue: true, title: '删除', error: '删除失败' },
      global: { plugins: [ElementPlus] },
      attachTo: document.body,
    })
    await flushPromises()
    expect(q('[data-test="confirm-error"]')?.textContent).toContain('删除失败')
    wrapper.unmount()
  })

  it('重放面板：切走页签即关闭详情抽屉（停止轮询）', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/replay/t1') {
        return {
          code: 0,
          message: 'ok',
          data: {
            task_id: 't1',
            namespace: 'msg',
            topic: 't',
            status: 'completed',
            replayed: 1,
            total: 1,
            rate_per_second: 1,
            partitions: null,
            offset_from: null,
            offset_to: null,
            time_from: null,
            time_to: null,
            key: null,
            created_at: '2026-11-13T00:00:00Z',
            finished_at: '2026-11-13T00:01:00Z',
            log_id_first: null,
            log_id_last: null,
            error_msg: null,
            affected_groups: [],
            affected_truncated: false,
            impact_snapshot: null,
            impact_groups: null,
          },
        }
      }
      return { code: 0, message: 'ok', data: { items: [], page: 1, size: 20, total: 0 } }
    })
    const wrapper = mount(ReplayTaskPanel, {
      props: { active: true, focusTaskId: 't1' },
      global: { plugins: [ElementPlus] },
      attachTo: document.body,
    })
    await flushPromises()
    expect(q('[data-test="replay-detail-drawer"]')).not.toBeNull()
    await wrapper.setProps({ active: false })
    await flushPromises()
    expect(q('[data-test="replay-detail-progress"]')).toBeNull()
    wrapper.unmount()
  })
})
