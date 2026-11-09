// kiwi_id: 2467
/** 死信与重放页（06-5-2）：列表 / 操作列 / 忽略原因必填 / 无权限隐藏 / 详情抽屉。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { api, permissionRef, routeState, push } = vi.hoisted(() => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
  },
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

import DlqPage from '../src/views/DlqPage.vue'

/** 死信行样例（pending）。 */
const PENDING = {
  id: '101',
  namespace: 'msg',
  topic: 'audit.operation',
  partition_no: 0,
  offset: 10,
  log_id: 'log-10',
  message_id: 'msg-10',
  trace_id: 'trace-1',
  reason: 'processing_failed',
  attempts: 5,
  status: 'pending',
  failed_at: '2026-11-09T10:00:00Z',
  replay_task_id: null,
  disposition_reason: null,
  disposed_at: null,
  created_at: '2026-11-09T10:00:01Z',
}

/** 死信行样例（archived）。 */
const ARCHIVED = { ...PENDING, id: '102', log_id: 'log-11', status: 'archived' }

/** 重放任务行样例（死信联动 / 深链用）。 */
const TASK = {
  task_id: '9001',
  namespace: 'msg',
  topic: 'audit.operation',
  partitions: [0],
  offset_from: 10,
  offset_to: 11,
  time_from: null,
  time_to: null,
  key: null,
  rate_per_second: 100,
  status: 'running',
  total: 1,
  replayed: 0,
  log_id_first: null,
  log_id_last: null,
  created_at: '2026-11-09T10:00:00Z',
  finished_at: null,
  error_msg: null,
  affected_groups: [],
  affected_truncated: false,
}

let mounted: VueWrapper | undefined

/**
 * 取 body 内某选择器的元素（弹窗内容经 teleport 渲染到 body）。
 *
 * @param selector CSS 选择器。
 * @returns 元素（不存在返回 null）。
 */
function q(selector: string): HTMLElement | null {
  return document.body.querySelector<HTMLElement>(selector)
}

/**
 * 点击元素。
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
}

/**
 * 写值并触发 input 事件。
 *
 * @param selector CSS 选择器。
 * @param value 新值。
 */
async function input(selector: string, value: string): Promise<void> {
  const element = q(selector) as HTMLTextAreaElement | null
  if (element === null) {
    throw new Error(`元素不存在：${selector}`)
  }
  element.value = value
  element.dispatchEvent(new Event('input'))
  await flushPromises()
}

beforeEach(() => {
  permissionRef.value = ['msg:query', 'msg:manage']
  api.get.mockReset()
  api.post.mockReset()
  api.get.mockImplementation((path: string) => {
    if (path === '/admin/dlq') {
      return Promise.resolve({ code: 0, message: 'ok', data: { items: [PENDING, ARCHIVED], total: 2, page: 1, size: 20 } })
    }
    if (path === '/admin/dlq/101') {
      return Promise.resolve({ code: 0, message: 'ok', data: { ...PENDING, detail: { error: 'boom' } } })
    }
    if (path === '/admin/replay') {
      return Promise.resolve({ code: 0, message: 'ok', data: { items: [TASK], total: 1, page: 1, size: 20 } })
    }
    if (path === '/admin/replay/9001') {
      return Promise.resolve({ code: 0, message: 'ok', data: TASK })
    }
    return Promise.resolve({ code: 0, message: 'ok', data: null })
  })
  api.post.mockResolvedValue({ code: 0, message: 'ok', data: { id: '101', status: 'ignored', replay_task_id: null } })
  routeState.value = { params: {}, query: {} }
})

afterEach(() => {
  mounted?.unmount()
  mounted = undefined
  document.body.innerHTML = ''
})

describe('DlqPage', () => {
  it('渲染死信列表，并按状态显示处置按钮', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    expect(document.body.textContent).toContain('log-10')
    expect(q('[data-test="dlq-replay"]')).not.toBeNull()
    expect(q('[data-test="dlq-ignore"]')).not.toBeNull()
    expect(q('[data-test="dlq-archive"]')).not.toBeNull()
  })

  it('忽略原因必填：空原因不发请求，填写后提交', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await click('[data-test="dlq-ignore"]')
    await click('[data-test="confirm-ok"]')
    expect(q('[data-test="dlq-dispose-error"]')).not.toBeNull()
    expect(api.post).not.toHaveBeenCalled()

    await input('textarea[data-test="dlq-disposition-reason"]', '上游已作废')
    await click('[data-test="confirm-ok"]')
    expect(api.post).toHaveBeenCalledWith('/admin/dlq/101/ignore', { confirm: true, reason: '上游已作废' })
  })

  it('无 msg:manage 时不渲染操作列', async () => {
    permissionRef.value = ['msg:query']
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    expect(q('[data-test="dlq-replay"]')).toBeNull()
    expect(q('[data-test="dlq-ignore"]')).toBeNull()
    expect(q('[data-test="dlq-archive"]')).toBeNull()
  })

  it('点击 log_id 打开详情抽屉并展示失败上下文', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await click('[data-test="dlq-detail"]')
    expect(api.get).toHaveBeenCalledWith('/admin/dlq/101', {})
    expect(document.body.textContent).toContain('boom')
  })

  it('页签结构：死信与重放任务两个页签同页', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    expect(q('[data-test="dlq-tabs"]')).not.toBeNull()
    expect(document.body.textContent).toContain('重放任务')
    expect(api.get).toHaveBeenCalledWith('/admin/replay', { page: 1, size: 20 })
  })

  it('死信重放成功后切到重放页签并高亮新任务', async () => {
    api.post.mockResolvedValueOnce({ code: 0, message: 'ok', data: { id: '101', status: 'replaying', replay_task_id: '9001' } })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await click('[data-test="dlq-replay"]')
    await click('[data-test="confirm-ok"]')
    await flushPromises()

    expect(api.post).toHaveBeenCalledWith('/admin/dlq/101/replay', { confirm: true, rate_per_second: 100 })
    expect(document.body.querySelector('.replay-row--active')).not.toBeNull()
    expect(api.get.mock.calls.filter(([path]) => path === '/admin/replay').length).toBeGreaterThan(1)
  })

  it('死信详情展示关联任务状态与进度，并可跳到任务详情', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/dlq') {
        return { code: 0, message: 'ok', data: { items: [PENDING], total: 1, page: 1, size: 20 } }
      }
      if (path === '/admin/dlq/101') {
        return { code: 0, message: 'ok', data: { ...PENDING, replay_task_id: '9001', detail: {} } }
      }
      if (path === '/admin/replay/9001') {
        return { code: 0, message: 'ok', data: TASK }
      }
      return { code: 0, message: 'ok', data: { items: [TASK], total: 1, page: 1, size: 20 } }
    })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await click('[data-test="dlq-detail"]')
    await flushPromises()
    expect(q('[data-test="dlq-related-task"]')?.textContent).toContain('9001')
    expect(q('[data-test="dlq-related-task"]')?.textContent).toContain('运行中')

    await click('[data-test="dlq-related-open"]')
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith('/admin/replay/9001', {})
  })

  it('未关联任务的死信详情给出「未关联」提示', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()
    await click('[data-test="dlq-detail"]')
    await flushPromises()
    expect(q('[data-test="dlq-related-none"]')?.textContent).toContain('未关联重放任务')
  })

  it('?task= 深链接直接打开重放任务详情', async () => {
    routeState.value = { params: {}, query: { task: '9001' } }
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()
    await flushPromises()

    expect(api.get).toHaveBeenCalledWith('/admin/replay/9001', {})
  })

  it('筛选控件改动后查询带参、重置清空参数', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    const selects = mounted.findAllComponents({ name: 'ElSelect' })
    selects[0].vm.$emit('update:modelValue', 'pending')
    selects[1].vm.$emit('update:modelValue', 'schema_invalid')
    const numbers = mounted.findAllComponents({ name: 'ElInputNumber' })
    numbers[0].vm.$emit('update:modelValue', 2)
    const pickers = mounted.findAllComponents({ name: 'ElDatePicker' })
    pickers[0].vm.$emit('update:modelValue', '2026-11-01T00:00:00')
    pickers[1].vm.$emit('update:modelValue', '2026-11-09T00:00:00')
    const inputs = mounted.findAllComponents({ name: 'ElInput' })
    const byPlaceholder = (placeholder: string) => inputs.find((item) => item.props('placeholder') === placeholder)!
    byPlaceholder('主题').vm.$emit('update:modelValue', 'order.created')
    byPlaceholder('log_id / message_id').vm.$emit('update:modelValue', 'log-10')
    await flushPromises()

    await click('[data-test="dlq-query"]')
    expect(api.get).toHaveBeenCalledWith('/admin/dlq', {
      status: 'pending',
      reason: 'schema_invalid',
      topic: 'order.created',
      partition: 2,
      failed_from: '2026-11-01T00:00:00',
      failed_to: '2026-11-09T00:00:00',
      keyword: 'log-10',
      page: 1,
      size: 20,
    })

    await click('[data-test="dlq-reset"]')
    expect(api.get).toHaveBeenLastCalledWith('/admin/dlq', { page: 1, size: 20 })
  })

  it('页签可切换；归档走确认弹窗并提交', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await mounted.findAll('.el-tabs__item')[1].trigger('click')
    await flushPromises()

    await click('[data-test="dlq-archive"]')
    await click('[data-test="confirm-ok"]')
    expect(api.post).toHaveBeenCalledWith('/admin/dlq/101/archive', { confirm: true })
  })

  it('详情取数失败给出失败文案', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/dlq/101') {
        return Promise.reject({ code: 350029 })
      }
      if (path === '/admin/dlq') {
        return { code: 0, message: 'ok', data: { items: [PENDING], total: 1, page: 1, size: 20 } }
      }
      return { code: 0, message: 'ok', data: { items: [TASK], total: 1, page: 1, size: 20 } }
    })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await click('[data-test="dlq-detail"]')
    await flushPromises()
    expect(document.body.textContent).toContain('详情加载失败')
  })

  it('处置失败展示错误码文案；抽屉与确认弹窗关闭回调可用', async () => {
    api.post.mockRejectedValueOnce({ code: 350030 })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await click('[data-test="dlq-detail"]')
    await flushPromises()
    const drawer = mounted.findComponent({ name: 'ElDrawer' })
    drawer.vm.$emit('update:modelValue', false)
    await flushPromises()

    await click('[data-test="dlq-ignore"]')
    const rate = mounted.findAllComponents({ name: 'ElInputNumber' })
    rate[rate.length - 1].vm.$emit('update:modelValue', 300)
    await mounted.find('textarea[data-test="dlq-disposition-reason"]').setValue('无需重放')
    await click('[data-test="confirm-ok"]')
    expect(document.body.textContent).toContain('状态不允许该操作')

    const dialogs = mounted.findAllComponents({ name: 'ElDialog' })
    dialogs[dialogs.length - 1].vm.$emit('update:modelValue', false)
    await flushPromises()
  })

  it('死信重放可调速率并带参提交', async () => {
    api.post.mockResolvedValueOnce({ code: 0, message: 'ok', data: { id: '101', status: 'replaying', replay_task_id: '9001' } })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await click('[data-test="dlq-replay"]')
    const numbers = mounted.findAllComponents({ name: 'ElInputNumber' })
    numbers[numbers.length - 1].vm.$emit('update:modelValue', 300)
    await flushPromises()
    await click('[data-test="confirm-ok"]')
    expect(api.post).toHaveBeenCalledWith('/admin/dlq/101/replay', { confirm: true, rate_per_second: 300 })
  })

  it('在重放页签内新建任务后父页高亮该任务', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/replay') {
        return { code: 0, message: 'ok', data: { items: [TASK], total: 1, page: 1, size: 20 } }
      }
      if (path === '/admin/dlq') {
        return { code: 0, message: 'ok', data: { items: [PENDING], total: 1, page: 1, size: 20 } }
      }
      return { code: 0, message: 'ok', data: { items: [], total: 0 } }
    })
    api.post.mockResolvedValueOnce({ code: 0, message: 'ok', data: TASK })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    await mounted.findAll('.el-tabs__item')[1].trigger('click')
    await click('[data-test="replay-create"]')
    const inputs = mounted.findAllComponents({ name: 'ElInput' })
    const byPlaceholder = (placeholder: string) => inputs.find((item) => item.props('placeholder') === placeholder)!
    byPlaceholder('msg').vm.$emit('update:modelValue', 'msg')
    byPlaceholder('order.created').vm.$emit('update:modelValue', 'order.created')
    await flushPromises()
    await click('[data-test="replay-create-next"]')
    await click('[data-test="confirm-ok"]')
    await flushPromises()

    expect(api.post).toHaveBeenCalledWith('/admin/replay', expect.objectContaining({ confirm: true }))
    expect(document.body.querySelector('.replay-row--active')).not.toBeNull()
  })

  it('死信详情抽屉可经自身关闭', async () => {
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()
    await click('[data-test="dlq-detail"]')
    await flushPromises()

    const drawers = mounted.findAllComponents({ name: 'ElDrawer' })
    drawers[drawers.length - 1].vm.$emit('update:modelValue', false)
    await flushPromises()
  })

  it('已忽略 / 已解决行仍可归档；关联任务取数失败时降级显示', async () => {
    const ignored = { ...PENDING, id: '103', log_id: 'log-12', status: 'ignored' }
    const resolved = { ...PENDING, id: '104', log_id: 'log-13', status: 'resolved' }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/dlq') {
        return { code: 0, message: 'ok', data: { items: [ignored, resolved], total: 2, page: 1, size: 20 } }
      }
      if (path === '/admin/dlq/103') {
        return { code: 0, message: 'ok', data: { ...ignored, replay_task_id: '9001', detail: {} } }
      }
      if (path === '/admin/replay/9001') {
        return Promise.reject({ code: 350016 })
      }
      return { code: 0, message: 'ok', data: { items: [TASK], total: 1, page: 1, size: 20 } }
    })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    expect(mounted.findAll('[data-test="dlq-archive"]')).toHaveLength(2)
    await click('[data-test="dlq-detail"]')
    await flushPromises()
    expect(q('[data-test="dlq-related-task"]')).toBeNull()
  })

  it('权限集合未装载时按宿主口径渲染操作列', async () => {
    permissionRef.value = undefined
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()
    expect(q('[data-test="dlq-archive"]')).not.toBeNull()
  })

  it('关联任务取数中显示加载态；未知状态行退回通用状态文案', async () => {
    let release: (value: unknown) => void = () => undefined
    const pending = new Promise((resolve) => {
      release = resolve
    })
    const odd = { ...PENDING, id: '105', log_id: 'log-14', status: 'unknown' }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/dlq') {
        return { code: 0, message: 'ok', data: { items: [odd], total: 1, page: 1, size: 20 } }
      }
      if (path === '/admin/dlq/105') {
        return { code: 0, message: 'ok', data: { ...odd, replay_task_id: '9001', detail: {} } }
      }
      if (path === '/admin/replay/9001') {
        return pending
      }
      return { code: 0, message: 'ok', data: { items: [TASK], total: 1, page: 1, size: 20 } }
    })
    mounted = mount(DlqPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
    await flushPromises()

    const detailButton = q('[data-test="dlq-detail"]')
    detailButton?.click()
    await new Promise((resolve) => setTimeout(resolve, 30))
    await flushPromises()
    expect(q('[data-test="dlq-related-loading"]')).not.toBeNull()

    release({ code: 0, message: 'ok', data: TASK })
    await flushPromises()
    expect(q('[data-test="dlq-related-task"]')).not.toBeNull()
    expect(document.body.textContent).toContain('状态')
  })
})
