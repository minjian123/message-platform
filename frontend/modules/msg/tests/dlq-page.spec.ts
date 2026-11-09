// kiwi_id: 2467
/** 死信与重放页（06-5-2）：列表 / 操作列 / 忽略原因必填 / 无权限隐藏 / 详情抽屉。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { api, permissionRef } = vi.hoisted(() => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
  },
  permissionRef: { value: ['msg:query', 'msg:manage'] as readonly string[] | undefined },
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => api }),
    hostPermissions: () => permissionRef.value,
  }
})

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
    return Promise.resolve({ code: 0, message: 'ok', data: null })
  })
  api.post.mockResolvedValue({ code: 0, message: 'ok', data: { id: '101', status: 'ignored', replay_task_id: null } })
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
})
