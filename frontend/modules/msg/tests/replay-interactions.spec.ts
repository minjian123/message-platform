// kiwi_id: 2468
/**
 * 重放任务页签 / 弹窗 / 抽屉的交互补充（06-6-2）：筛选控件、行内跳转、表单其余控件、终态与降级着色。
 *
 * 与 `replay-page.spec.ts` 同一被测面，拆开只为让「取数与业务断言」与「控件交互」各自可读。
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
  return { ...actual, REPLAY_POLL_INTERVAL_MS: 600_000 }
})

import ReplayTaskDetailDrawer from '../src/components/ReplayTaskDetailDrawer.vue'
import ReplayTaskPanel from '../src/components/ReplayTaskPanel.vue'

/** 运行中的任务。 */
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
 * 取 body 内元素。
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

beforeEach(() => {
  permissionRef.value = ['msg:query', 'msg:manage']
  api.get.mockReset()
  api.post.mockReset()
  api.get.mockImplementation(async (path: string) => {
    if (path === '/admin/replay') {
      return { code: 0, message: 'ok', data: { items: [RUNNING], total: 1, page: 1, size: 20 } }
    }
    if (path === '/admin/replay/9001') {
      return { code: 0, message: 'ok', data: RUNNING }
    }
    return { code: 0, message: 'ok', data: null }
  })
})

afterEach(() => {
  for (const wrapper of mounted) {
    wrapper.unmount()
  }
  mounted = []
  document.body.innerHTML = ''
})

describe('重放任务页签交互', () => {
  it('筛选控件改动后带参查询', async () => {
    const wrapper = mountWith(ReplayTaskPanel)
    await flushPromises()

    wrapper.findAllComponents({ name: 'ElSelect' })[0].vm.$emit('update:modelValue', 'paused')
    const inputs = wrapper.findAllComponents({ name: 'ElInput' })
    const byPlaceholder = (placeholder: string) => inputs.find((item) => item.props('placeholder') === placeholder)!
    byPlaceholder('命名空间').vm.$emit('update:modelValue', 'bms')
    byPlaceholder('主题').vm.$emit('update:modelValue', 'invoice.issued')
    const pickers = wrapper.findAllComponents({ name: 'ElDatePicker' })
    pickers[0].vm.$emit('update:modelValue', '2026-11-01T00:00:00')
    pickers[1].vm.$emit('update:modelValue', '2026-11-09T00:00:00')
    await flushPromises()

    await click('[data-test="replay-query"]')
    expect(api.get).toHaveBeenCalledWith('/admin/replay', {
      status: 'paused',
      namespace: 'bms',
      topic: 'invoice.issued',
      created_from: '2026-11-01T00:00:00',
      created_to: '2026-11-09T00:00:00',
      page: 1,
      size: 20,
    })
  })

  it('点任务行打开详情抽屉；详情变更回调刷新列表；关闭后复位', async () => {
    const wrapper = mountWith(ReplayTaskPanel)
    await flushPromises()

    await click('[data-test="replay-detail"]')
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith('/admin/replay/9001', {})
    expect(q('[data-test="replay-detail-progress-text"]')?.textContent).toContain('120 / 400')

    wrapper.findComponent(ReplayTaskDetailDrawer).vm.$emit('changed')
    await flushPromises()
    expect(api.get.mock.calls.filter(([path]) => path === '/admin/replay').length).toBeGreaterThan(1)

    await click('[data-test="replay-detail-close"]')
    await flushPromises()
    expect(q('[data-test="replay-detail-progress-text"]')).toBeNull()
  })

  it('创建弹窗其余控件可改、弹窗可关闭', async () => {
    const wrapper = mountWith(ReplayTaskPanel)
    await flushPromises()
    await click('[data-test="replay-create"]')

    const numbers = wrapper.findAllComponents({ name: 'ElInputNumber' })
    numbers[0].vm.$emit('update:modelValue', 100)
    numbers[1].vm.$emit('update:modelValue', 200)
    numbers[2].vm.$emit('update:modelValue', 500)
    const pickers = wrapper.findAllComponents({ name: 'ElDatePicker' })
    pickers[pickers.length - 2].vm.$emit('update:modelValue', '2026-11-01T00:00:00')
    pickers[pickers.length - 1].vm.$emit('update:modelValue', '2026-11-09T00:00:00')
    const inputs = wrapper.findAllComponents({ name: 'ElInput' })
    inputs.find((item) => item.props('placeholder') === '精确匹配，留空不过滤')!.vm.$emit('update:modelValue', 'k1')
    await flushPromises()

    await type('[data-test="replay-form-namespace"]', 'msg')
    await type('[data-test="replay-form-topic"]', 'order.created')
    await click('[data-test="replay-create-next"]')
    expect(document.body.textContent).toContain('offset 100~200')
    expect(document.body.textContent).toContain('2026-11-01 00:00:00')
    expect(document.body.textContent).toContain('key=k1')

    wrapper.findAllComponents({ name: 'ElDialog' })[0].vm.$emit('update:modelValue', false)
    await new Promise((resolve) => setTimeout(resolve, 80))
    await flushPromises()
    const overlay = q('[data-test="replay-create-dialog"]')?.closest('.el-overlay') as HTMLElement | null
    expect(overlay?.style.display).toBe('none')
  })

  it('创建弹窗取消按钮复位并关闭', async () => {
    mountWith(ReplayTaskPanel)
    await flushPromises()
    await click('[data-test="replay-create"]')
    await type('[data-test="replay-form-namespace"]', 'msg')
    await click('[data-test="replay-create-cancel"]')
    await new Promise((resolve) => setTimeout(resolve, 80))
    await flushPromises()
    expect(api.post).not.toHaveBeenCalled()
  })
})

describe('重放任务详情交互', () => {
  it('总数为 0 时进度记 0；失败任务给 exception 状态并展示失败原因', async () => {
    api.get.mockImplementation(async (path: string) => ({
      code: 0,
      message: 'ok',
      data:
        path === '/admin/replay/9001'
          ? { ...RUNNING, status: 'failed', total: 0, replayed: 0, error_msg: '写入失败' }
          : { items: [], total: 0, page: 1, size: 20 },
    }))
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()

    expect(q('[data-test="replay-detail-progress-text"]')?.textContent).toContain('0 / 0')
    expect(q('[data-test="replay-detail-status"]')?.textContent).toContain('已失败')
    expect(q('[data-test="replay-detail-error-msg"]')?.textContent).toContain('写入失败')
  })

  it('变化值为负时照实展示；操作确认弹窗可取消关闭', async () => {
    api.get.mockImplementation(async (path: string) => ({
      code: 0,
      message: 'ok',
      data:
        path === '/admin/replay/9001'
          ? {
              ...RUNNING,
              affected_groups: [
                { group: 'g', baseline_lag: 30, current_lag: 10, delta: -20, captured_at: '2026-11-09T10:05:00Z' },
              ],
            }
          : { items: [], total: 0, page: 1, size: 20 },
    }))
    const wrapper = mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    expect(document.body.textContent).toContain('-20')

    const drawer = wrapper.findComponent({ name: 'ElDrawer' })
    drawer.vm.$emit('update:modelValue', true)
    await flushPromises()

    await click('[data-test="replay-pause"]')
    expect(document.body.textContent).toContain('暂停在批边界生效')
    await click('[data-test="confirm-cancel"]')
    await flushPromises()
    expect(api.post).not.toHaveBeenCalled()
  })

  it('影响视图为 null 时按空表渲染', async () => {
    api.get.mockImplementation(async (path: string) => ({
      code: 0,
      message: 'ok',
      data:
        path === '/admin/replay/9001'
          ? { ...RUNNING, affected_groups: null, affected_truncated: false }
          : { items: [], total: 0, page: 1, size: 20 },
    }))
    mountWith(ReplayTaskDetailDrawer, { taskId: '9001' })
    await flushPromises()
    expect(document.body.textContent).toContain('该主题无订阅组')
  })

  it('权限集合未装载时按宿主口径放行（可渲染新建入口）', async () => {
    permissionRef.value = undefined
    mountWith(ReplayTaskPanel)
    await flushPromises()
    expect(q('[data-test="replay-create"]')).not.toBeNull()
  })

  it('刷新令牌与目标任务 ID 变化时重新取数并打开详情', async () => {
    const wrapper = mountWith(ReplayTaskPanel)
    await flushPromises()
    const before = api.get.mock.calls.filter(([path]) => path === '/admin/replay').length

    await wrapper.setProps({ refreshToken: 1 })
    await flushPromises()
    expect(api.get.mock.calls.filter(([path]) => path === '/admin/replay').length).toBeGreaterThan(before)

    await wrapper.setProps({ focusTaskId: '9001' })
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith('/admin/replay/9001', {})
  })
})
