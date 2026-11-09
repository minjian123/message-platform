// kiwi_id: 2468
/**
 * 重放任务页签（06-6-2）：创建两段式 / 列表筛选 / 详情进度与影响 / 暂停恢复取消 / 权限 / 轮询。
 *
 * 轮询间隔在本文件内改为 5ms（真实 5s 见 `constants.ts`），用真实定时器等待，避免 fake timers 与
 * Element Plus 过渡动画互相干扰。
 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { api, permissionRef } = vi.hoisted(() => ({
  api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() },
  permissionRef: { value: ['msg:query', 'msg:manage'] as readonly string[] | undefined },
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

vi.mock('../src/constants', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/constants')>()
  return { ...actual, REPLAY_POLL_INTERVAL_MS: 5 }
})

import ReplayTaskDetailDrawer from '../src/components/ReplayTaskDetailDrawer.vue'
import ReplayTaskPanel from '../src/components/ReplayTaskPanel.vue'
import {
  describeReplayScope,
  parsePartitionText,
  toReplayBody,
  validateReplayForm,
  type ReplayForm,
} from '../src/services/replay-format'

/** 运行中的任务（含影响视图）。 */
const RUNNING = {
  task_id: '9001',
  namespace: 'msg',
  topic: 'order.created',
  partitions: [0, 1],
  offset_from: 100,
  offset_to: 200,
  time_from: null,
  time_to: null,
  key: null,
  rate_per_second: 500,
  status: 'running',
  total: 400,
  replayed: 120,
  log_id_first: 'log-a',
  log_id_last: 'log-b',
  created_at: '2026-11-09T10:00:00Z',
  finished_at: null,
  error_msg: null,
  affected_groups: [{ group: 'order-sync', baseline_lag: 10, current_lag: 30, delta: 20, captured_at: '2026-11-09T10:05:00Z' }],
  affected_truncated: false,
}

/** 已完成任务（终态、无操作按钮）。 */
const COMPLETED = {
  ...RUNNING,
  status: 'completed',
  replayed: 400,
  finished_at: '2026-11-09T10:10:00Z',
  affected_truncated: true,
  affected_groups: [],
}

let mounted: VueWrapper[] = []

/**
 * 挂载组件并登记以便统一卸载。
 *
 * @param component 组件。
 * @param props 属性。
 * @returns 挂载结果。
 */
function mountWith(component: unknown, props: Record<string, unknown> = {}): VueWrapper {
  const wrapper = mount(component as never, {
    props,
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  mounted.push(wrapper)
  return wrapper
}

/**
 * 取 body 内元素（弹窗 / 抽屉 teleport 到 body）。
 *
 * @param selector CSS 选择器。
 * @returns 元素（不存在返回 null）。
 */
function q(selector: string): HTMLElement | null {
  return document.body.querySelector<HTMLElement>(selector)
}

/**
 * 点击元素并结算。
 *
 * @param selector CSS 选择器。
 */
async function click(selector: string): Promise<void> {
  const element = q(selector)
  if (element === null) {
    throw new Error(`元素不存在：${selector}`)
  }
  element.click()
  await flushPromises()
  await flushPromises()
}

/**
 * 给输入框写值。
 *
 * @param selector CSS 选择器（根元素）。
 * @param value 新值。
 */
async function type(selector: string, value: string): Promise<void> {
  const root = q(selector)
  if (root === null) {
    throw new Error(`元素不存在：${selector}`)
  }
  const input = root.tagName === 'INPUT' ? root : root.querySelector('input')
  if (input === null) {
    throw new Error(`输入框不存在：${selector}`)
  }
  input.value = value
  input.dispatchEvent(new Event('input'))
  await flushPromises()
}

/** 等待轮询触发（间隔在本文件内改为 5ms）。 */
async function waitPoll(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 20))
  await flushPromises()
}

/**
 * 按端点路径装配默认响应（可用 `overrides` 覆盖单个路径）。
 *
 * @param overrides 覆盖表。
 */
function respond(overrides: Record<string, unknown> = {}): void {
  api.get.mockImplementation(async (path: string) => {
    if (path in overrides) {
      const value = overrides[path]
      return value instanceof Error ? Promise.reject(value) : { code: 0, message: 'ok', data: value }
    }
    if (path === '/admin/replay') {
      return { code: 0, message: 'ok', data: { items: [RUNNING], total: 1, page: 1, size: 20 } }
    }
    if (path === '/admin/replay/9001') {
      return { code: 0, message: 'ok', data: RUNNING }
    }
    if (path === '/admin/messages') {
      return { code: 0, message: 'ok', data: { items: [], total: 12480, page: 1, size: 1 } }
    }
    if (path === '/admin/groups') {
      return { code: 0, message: 'ok', data: { items: [{ group: 'order-sync' }, { group: 'order-audit' }], total: 2 } }
    }
    return { code: 0, message: 'ok', data: null }
  })
}

beforeEach(() => {
  permissionRef.value = ['msg:query', 'msg:manage']
  api.get.mockReset()
  api.post.mockReset()
  respond()
  api.post.mockResolvedValue({ code: 0, message: 'ok', data: RUNNING })
})

afterEach(() => {
  for (const wrapper of mounted) {
    wrapper.unmount()
  }
  mounted = []
  document.body.innerHTML = ''
})

describe('重放条件格式化（纯函数）', () => {
  it('分区文本：空 = 全部分区；去重升序；非数字视为非法', () => {
    expect(parsePartitionText('   ')).toEqual({ ok: true, value: null })
    expect(parsePartitionText('2, 0，1 2')).toEqual({ ok: true, value: [0, 1, 2] })
    expect(parsePartitionText('0,a')).toEqual({ ok: false, value: null })
  })

  it('条件摘要：全部分区 / 分区列表、offset 单边、时间区间、key', () => {
    expect(
      describeReplayScope({ partitions: null, offset_from: null, offset_to: null, time_from: null, time_to: null, key: null }),
    ).toBe('全部分区')
    expect(
      describeReplayScope({
        partitions: [0, 1],
        offset_from: 100,
        offset_to: null,
        time_from: '2026-11-09T00:00:00',
        time_to: null,
        key: 'k1',
      }),
    ).toBe('分区 0,1 · offset 100~* · 2026-11-09 00:00:00 ~ * · key=k1')
    expect(
      describeReplayScope({ partitions: [], offset_from: null, offset_to: 5, time_from: null, time_to: null, key: null }),
    ).toBe('分区  · offset *~5')
  })

  it('表单校验：必填 / 分区非法 / offset 颠倒 / 时间颠倒 / 通过', () => {
    const base: ReplayForm = {
      namespace: 'msg',
      topic: 'order.created',
      offsetFrom: undefined,
      offsetTo: undefined,
      timeFrom: '',
      timeTo: '',
      key: '',
      partitions: '',
      rate: 100,
    }
    expect(validateReplayForm({ ...base, namespace: ' ' })).toBe('msg.admin.replay.validation')
    expect(validateReplayForm({ ...base, partitions: 'x' })).toBe('msg.admin.replay.invalidPartitions')
    expect(validateReplayForm({ ...base, offsetFrom: 200, offsetTo: 200 })).toBe('msg.admin.replay.invalidRange')
    expect(validateReplayForm({ ...base, timeFrom: '2026-11-09T10:00:00', timeTo: '2026-11-09T09:00:00' })).toBe(
      'msg.admin.replay.invalidRange',
    )
    expect(validateReplayForm(base)).toBe('')
  })

  it('表单 → 请求体：空值归一为 null、文本去空格、分区解析', () => {
    expect(
      toReplayBody({
        namespace: ' msg ',
        topic: ' order.created ',
        offsetFrom: 10,
        offsetTo: undefined,
        timeFrom: '',
        timeTo: '2026-11-09T10:00:00',
        key: ' k1 ',
        partitions: '1,0',
        rate: 200,
      }),
    ).toEqual({
      namespace: 'msg',
      topic: 'order.created',
      partitions: [0, 1],
      offset_from: 10,
      offset_to: null,
      time_from: null,
      time_to: '2026-11-09T10:00:00',
      key: 'k1',
      rate_per_second: 200,
    })
    expect(
      toReplayBody({
        namespace: 'msg',
        topic: 't',
        offsetFrom: undefined,
        offsetTo: undefined,
        timeFrom: '2026-11-01T00:00:00',
        timeTo: '',
        key: '',
        partitions: '',
        rate: 100,
      }),
    ).toMatchObject({ time_from: '2026-11-01T00:00:00', time_to: null, key: null, partitions: null })
  })
})

describe('重放任务页签', () => {
  it('列表渲染七列并在筛选后带参取数、重置清空', async () => {
    const wrapper = mountWith(ReplayTaskPanel)
    await flushPromises()

    expect(wrapper.text()).toContain('9001')
    expect(wrapper.text()).toContain('msg / order.created')
    expect(wrapper.text()).toContain('分区 0,1 · offset 100~200')
    expect(wrapper.text()).toContain('120 / 400')
    expect(wrapper.text()).toContain('运行中')

    await type('[data-test="replay-namespace"]', 'msg')
    await click('[data-test="replay-query"]')
    expect(api.get).toHaveBeenCalledWith('/admin/replay', { namespace: 'msg', page: 1, size: 20 })

    await click('[data-test="replay-reset"]')
    expect(api.get).toHaveBeenLastCalledWith('/admin/replay', { page: 1, size: 20 })
  })

  it('创建两段式：必填校验 → 预估与目标组 → 确认载荷含 confirm', async () => {
    const wrapper = mountWith(ReplayTaskPanel)
    await flushPromises()
    await click('[data-test="replay-create"]')

    await click('[data-test="replay-create-next"]')
    expect(q('[data-test="replay-create-error"]')?.textContent).toContain('命名空间与主题必填')

    await type('[data-test="replay-form-namespace"]', 'msg')
    await type('[data-test="replay-form-topic"]', 'order.created')
    await type('[data-test="replay-form-partitions"]', '1')
    await click('[data-test="replay-create-next"]')

    expect(api.get).toHaveBeenCalledWith('/admin/messages', {
      namespace: 'msg',
      topic: 'order.created',
      partition: 1,
      page: 1,
      size: 1,
    })
    expect(q('[data-test="replay-create-dialog"]')).not.toBeNull()
    expect(document.body.textContent).toContain('12480')
    expect(q('[data-test="replay-target-groups"]')?.textContent).toContain('order-sync')

    api.post.mockResolvedValueOnce({ code: 0, message: 'ok', data: RUNNING })
    await click('[data-test="confirm-ok"]')
    expect(api.post).toHaveBeenCalledWith('/admin/replay', {
      namespace: 'msg',
      topic: 'order.created',
      partitions: [1],
      offset_from: null,
      offset_to: null,
      time_from: null,
      time_to: null,
      key: null,
      rate_per_second: 100,
      confirm: true,
    })
    expect(wrapper.emitted('created')?.[0]).toEqual(['9001'])
    wrapper.unmount()
    mounted = mounted.filter((item) => item !== wrapper)
  })

  it('多分区不预估、无订阅组与取组失败各有降级文案', async () => {
    const wrapper = mountWith(ReplayTaskPanel)
    await flushPromises()
    await click('[data-test="replay-create"]')
    await type('[data-test="replay-form-namespace"]', 'msg')
    await type('[data-test="replay-form-topic"]', 'order.created')
    await type('[data-test="replay-form-partitions"]', '0,1,2')
    await click('[data-test="replay-create-next"]')
    expect(api.get).not.toHaveBeenCalledWith('/admin/messages', expect.anything())
    expect(document.body.textContent).toContain('多分区以创建时计数为准')
    expect(api.get).toHaveBeenCalledWith('/admin/groups', { namespace: 'msg', topic: 'order.created', page: 1, size: 100 })

    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/groups') {
        return Promise.reject(new Error('boom'))
      }
      return { code: 0, message: 'ok', data: { items: [RUNNING], total: 1, page: 1, size: 20 } }
    })
    await click('[data-test="confirm-cancel"]')
    await type('[data-test="replay-form-partitions"]', '')
    await click('[data-test="replay-create-next"]')
    expect(q('[data-test="replay-target-groups-failed"]')?.textContent).toContain('目标组暂不可用')

    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/groups') {
        return { code: 0, message: 'ok', data: { items: [], total: 0 } }
      }
      return { code: 0, message: 'ok', data: { items: [], total: 12480, page: 1, size: 1 } }
    })
    await click('[data-test="confirm-cancel"]')
    await click('[data-test="replay-create-next"]')
    expect(q('[data-test="replay-target-groups-empty"]')?.textContent).toContain('无订阅组')

    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/groups') {
        return { code: 0, message: 'ok', data: { items: [], total: 0 } }
      }
      return Promise.reject(new Error('messages down'))
    })
    await click('[data-test="confirm-cancel"]')
    await click('[data-test="replay-create-next"]')
    expect(document.body.textContent).toContain('预计总量暂不可用')
    wrapper.unmount()
    mounted = mounted.filter((item) => item !== wrapper)
  })

  it('创建失败展示错误码文案并保留表单', async () => {
    mountWith(ReplayTaskPanel)
    await flushPromises()
    await click('[data-test="replay-create"]')
    await type('[data-test="replay-form-namespace"]', 'msg')
    await type('[data-test="replay-form-topic"]', 'order.created')
    await click('[data-test="replay-create-next"]')
    api.post.mockRejectedValueOnce({ code: 350010 })
    await click('[data-test="confirm-ok"]')
    expect(q('[data-test="replay-confirm-error"]')?.textContent).toContain('命名空间未登记或未启用')
  })

  it('无 msg:manage 时不渲染新建入口', async () => {
    permissionRef.value = ['msg:query']
    mountWith(ReplayTaskPanel)
    await flushPromises()
    expect(q('[data-test="replay-create"]')).toBeNull()
  })
})

describe('重放任务详情', () => {
  it('进度、条件、首末 log_id 与影响表；终态给只读提示并带截断提示', async () => {
    api.get.mockImplementation(async (path: string) => ({
      code: 0,
      message: 'ok',
      data: path === '/admin/replay/9001' ? COMPLETED : { items: [], total: 0, page: 1, size: 20 },
    }))
    const wrapper = mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()

    expect(q('[data-test="replay-detail-progress-text"]')?.textContent).toContain('400 / 400')
    expect(q('[data-test="replay-detail-status"]')?.textContent).toContain('已完成')
    expect(q('[data-test="replay-detail-truncated"]')?.textContent).toContain('仅展示前 100 个组')
    expect(q('[data-test="replay-detail-terminal"]')?.textContent).toContain('终态任务只读')
    expect(q('[data-test="replay-pause"]')).toBeNull()
    expect(q('[data-test="replay-cancel"]')).toBeNull()
    wrapper.unmount()
    mounted = mounted.filter((item) => item !== wrapper)
  })

  it('运行中展示影响表与变化值，并可暂停 / 取消', async () => {
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()

    expect(document.body.textContent).toContain('order-sync')
    expect(document.body.textContent).toContain('+20')

    await click('[data-test="replay-pause"]')
    expect(document.body.textContent).toContain('暂停在批边界生效')
    await click('[data-test="confirm-ok"]')
    expect(api.post).toHaveBeenCalledWith('/admin/replay/9001/pause', {})

    api.post.mockResolvedValueOnce({ code: 0, message: 'ok', data: { ...RUNNING, status: 'cancelled' } })
    await click('[data-test="replay-cancel"]')
    expect(document.body.textContent).toContain('已写入的记录不回滚')
    await click('[data-test="confirm-ok"]')
    expect(api.post).toHaveBeenCalledWith('/admin/replay/9001/cancel', {})
  })

  it('已暂停任务只给恢复入口', async () => {
    api.get.mockImplementation(async (path: string) => ({
      code: 0,
      message: 'ok',
      data: path === '/admin/replay/9001' ? { ...RUNNING, status: 'paused' } : null,
    }))
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    expect(q('[data-test="replay-pause"]')).toBeNull()
    await click('[data-test="replay-resume"]')
    await click('[data-test="confirm-ok"]')
    expect(api.post).toHaveBeenCalledWith('/admin/replay/9001/resume', {})
  })

  it('状态操作失败展示 350017 文案', async () => {
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    api.post.mockRejectedValueOnce({ code: 350017 })
    await click('[data-test="replay-cancel"]')
    await click('[data-test="confirm-ok"]')
    expect(q('[data-test="replay-action-error"]')?.textContent).toContain('状态不允许该操作')
  })

  it('详情取数失败给错误态与重试；失败原因透传', async () => {
    api.get.mockRejectedValueOnce({ code: 350016 })
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    expect(q('[data-test="replay-detail-error"]')?.textContent).toContain('详情加载失败')

    respond({ '/admin/replay/9001': { ...RUNNING, error_msg: '写入失败' } })
    await click('[data-test="replay-detail-retry"]')
    expect(q('[data-test="replay-detail-error-msg"]')?.textContent).toContain('写入失败')
  })

  it('非终态 5ms 后再次取数，转终态即停', async () => {
    let calls = 0
    api.get.mockImplementation(async (path: string) => {
      if (path !== '/admin/replay/9001') {
        return { code: 0, message: 'ok', data: null }
      }
      calls += 1
      return { code: 0, message: 'ok', data: calls === 1 ? RUNNING : COMPLETED }
    })
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    expect(calls).toBeGreaterThanOrEqual(1)

    await waitPoll()
    expect(calls).toBe(2)
    await waitPoll()
    expect(calls).toBe(2)
  })

  it('详情 data 为空视为失败并停止轮询', async () => {
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: null })
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    expect(q('[data-test="replay-detail-error"]')).not.toBeNull()
  })

  it('无 msg:manage 时不渲染任何状态按钮', async () => {
    permissionRef.value = ['msg:query']
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    expect(q('[data-test="replay-pause"]')).toBeNull()
    expect(q('[data-test="replay-resume"]')).toBeNull()
    expect(q('[data-test="replay-cancel"]')).toBeNull()
  })

  it('任务 ID 为空时不发请求', async () => {
    mountWith(ReplayTaskDetailDrawer, { taskId: null })
    await flushPromises()
    expect(api.get).not.toHaveBeenCalled()
  })
})
