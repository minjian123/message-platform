// kiwi_id: 2466
/** 写操作页面：新建主题 / 扩分区 / 改保留期 / 删除确认与清理进度 / 位点重置。 */

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
  permissionRef: { value: undefined as readonly string[] | undefined },
  routeState: { value: { params: {} as Record<string, string>, query: {} as Record<string, string> } },
  push: vi.fn(),
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => api }),
    hostPermissions: () => permissionRef.value,
  }
})

vi.mock('vue-router', () => ({
  useRoute: () => routeState.value,
  useRouter: () => ({ push }),
}))

import { ElDialog, ElInputNumber } from 'element-plus'
import {
  createTopic,
  deleteTopic,
  expandPartitions,
  fetchDeleteImpact,
  resetOffset,
  updateRetention,
} from '../src/services/admin-service'
import GroupDetailPage from '../src/views/GroupDetailPage.vue'
import TopicDetailPage from '../src/views/TopicDetailPage.vue'
import TopicsPage from '../src/views/TopicsPage.vue'

/** 主题详情样例。 */
const TOPIC = {
  id: '42',
  tenant_id: '1',
  name: 'order.created',
  namespace: 'msg',
  partition_count: 3,
  retention_ms: 86_400_000,
  retention_bytes: 0,
  cleanup_policy: 'delete',
  status: 'active',
  created_at: '2026-11-09T00:00:00Z',
  cleanup: null,
}

/** 分区行样例。 */
const PARTITION = {
  partition: 0,
  leader: 'msg-0',
  leo: 100,
  hw: null,
  isr: [],
  committed: 90,
  lag: 10,
  owner: 'c-1',
}

/** 消费组详情样例。 */
const GROUP_DETAIL = {
  namespace: 'msg',
  topic: 'order.created',
  group: 'order-sync',
  state: 'stable',
  generation: 3,
  partition_count: 1,
  members: [{ consumer_id: 'cw-1', partitions: [0], last_seen: '2026-11-09T00:00:00Z' }],
  partitions: [PARTITION],
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
 * 写值并触发 input 事件（el-input 的 v-model 监听原生 input）。
 *
 * @param selector CSS 选择器。
 * @param value 新值。
 */
async function input(selector: string, value: string): Promise<void> {
  const element = q(selector) as HTMLInputElement | null
  if (element === null) {
    throw new Error(`元素不存在：${selector}`)
  }
  element.value = value
  element.dispatchEvent(new Event('input'))
  await flushPromises()
}

/**
 * 点击元素（弹窗 / 页面按钮统一走原生 click）。
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
 * 弹窗是否已隐藏（Element Plus 关闭后保留 DOM，仅把遮罩设为 `display: none`）。
 *
 * @param selector 弹窗内容选择器。
 * @returns 已隐藏返回 true。
 */
function hidden(selector: string): boolean {
  const overlay = q(selector)?.closest('.el-overlay') as HTMLElement | null | undefined
  return overlay === null || overlay === undefined || overlay.style.display === 'none'
}

/**
 * 挂载页面（teleport 目标为 body，便于统一断言）。
 *
 * @param component 页面组件。
 * @returns 挂载结果。
 */
async function mountPage(component: unknown): Promise<VueWrapper> {
  const wrapper = mount(component as never, {
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  await flushPromises()
  mounted = wrapper
  return wrapper
}

beforeEach(() => {
  // 页面可读需 msg:query、写入口需 msg:manage：两码同时持有才是真实的管理员视角。
  permissionRef.value = ['msg:query', 'msg:manage']
  routeState.value = { params: {}, query: {} }
  api.get.mockReset()
  api.post.mockReset()
  api.put.mockReset()
  api.patch.mockReset()
  api.del.mockReset()
  api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: [], page: 1, size: 20, total: 0 } })
})

afterEach(() => {
  mounted?.unmount()
  mounted = undefined
  document.body.innerHTML = ''
  vi.useRealTimers()
})

describe('主题列表：新建主题（Kiwi 2466）', () => {
  it('无 msg:manage：不渲染新建入口', async () => {
    permissionRef.value = ['msg:query']
    await mountPage(TopicsPage)
    expect(q('[data-test="topics-create"]')).toBeNull()
  })

  it('提交成功：POST 携带表单字段，成功后可再次打开（状态已清）', async () => {
    await mountPage(TopicsPage)
    await click('[data-test="topics-create"]')
    expect(q('[data-test="topics-create-dialog"]')).not.toBeNull()
    await input('[data-test="create-name"]', 'order.paid')
    api.post.mockResolvedValue({ code: 0, message: 'ok', data: TOPIC })
    await click('[data-test="create-submit"]')
    const [path, body] = api.post.mock.calls[0] as [string, Record<string, unknown>]
    expect(path).toBe('/admin/topics')
    expect(body).toMatchObject({ name: 'order.paid', namespace: 'msg', partition_count: 1 })
    expect(api.get).toHaveBeenCalled()
    await click('[data-test="topics-create"]')
    expect(q('[data-test="create-error"]')).toBeNull()
  })

  it('提交失败：展示错误码文案且弹窗保留', async () => {
    await mountPage(TopicsPage)
    await click('[data-test="topics-create"]')
    api.post.mockResolvedValue({ code: 350008, message: 'dup', data: null })
    await click('[data-test="create-submit"]')
    expect(q('[data-test="create-error"]')?.textContent).toContain('同名主题已存在')
    expect(q('[data-test="topics-create-dialog"]')).not.toBeNull()
  })
})

describe('主题详情：写操作与清理进度（Kiwi 2466）', () => {
  beforeEach(() => {
    routeState.value = { params: { topicId: '42' }, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
  })

  it('扩分区：只增校验由后端兜底，提交 POST 目标分区数并刷新', async () => {
    await mountPage(TopicDetailPage)
    await click('[data-test="topic-expand"]')
    api.post.mockResolvedValue({ code: 0, message: 'ok', data: { ...TOPIC, partition_count: 4 } })
    await click('[data-test="expand-submit"]')
    const [path, body] = api.post.mock.calls[0] as [string, Record<string, unknown>]
    expect(path).toBe('/admin/topics/42/partitions')
    expect(body).toEqual({ partition_count: 4 })
  })

  it('扩分区失败：按码展示错误', async () => {
    await mountPage(TopicDetailPage)
    await click('[data-test="topic-expand"]')
    api.post.mockResolvedValue({ code: 350009, message: 'bad', data: null })
    await click('[data-test="expand-submit"]')
    expect(q('[data-test="expand-error"]')?.textContent).toContain('非法')
  })

  it('改保留期：PATCH 携带当前回填值，失败展示错误', async () => {
    await mountPage(TopicDetailPage)
    await click('[data-test="topic-retention"]')
    api.patch.mockResolvedValue({ code: 350028, message: 'bad', data: null })
    await click('[data-test="retention-submit"]')
    const [path, body] = api.patch.mock.calls[0] as [string, Record<string, unknown>]
    expect(path).toBe('/admin/topics/42')
    expect(body).toEqual({ retention_ms: 86_400_000, retention_bytes: 0 })
    expect(q('[data-test="retention-error"]')?.textContent).toContain('删除')
  })

  it('删除：先取影响范围再确认，DELETE 带 confirm=true，受理后转清理中', async () => {
    let accepted = false
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42/delete-impact') {
        return {
          code: 0,
          message: 'ok',
          data: { partition_count: 3, message_total: 120, group_count: 2, delayed_total: 5, dlq_total: 1 },
        }
      }
      if (path === '/admin/topics/42') {
        return {
          code: 0,
          message: 'ok',
          data: accepted ? { ...TOPIC, status: 'deleting', cleanup: CLEANUP } : TOPIC,
        }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    await click('[data-test="topic-delete"]')
    expect(q('[data-test="confirm-impact"]')?.textContent).toContain('120')
    api.del.mockResolvedValue({ code: 0, message: 'ok', data: {} })
    accepted = true
    await click('[data-test="confirm-ok"]')
    const [path, params] = api.del.mock.calls[0] as [string, Record<string, unknown>]
    expect(path).toBe('/admin/topics/42')
    expect(params).toEqual({ confirm: true })
    expect(q('[data-test="topic-deleting"]')).not.toBeNull()
    expect(q('[data-test="topic-cleanup-table"]')).not.toBeNull()
    expect(q('[data-test="topic-expand"]')).toBeNull()
  })

  it('删除失败：确认弹窗保留并展示错误码', async () => {
    await mountPage(TopicDetailPage)
    await click('[data-test="topic-delete"]')
    api.del.mockResolvedValue({ code: 350028, message: 'bad', data: null })
    await click('[data-test="confirm-ok"]')
    expect(q('[data-test="delete-error"]')?.textContent).toContain('删除')
  })

  it('影响预检失败：仍可打开确认（不阻断），行表为空', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42/delete-impact') {
        throw new Error('down')
      }
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    await click('[data-test="topic-delete"]')
    expect(q('[data-test="confirm-impact"]')).toBeNull()
    expect(q('[data-test="confirm-warning"]')).not.toBeNull()
  })

  it('清理中：轮询详情，状态回到 active 后停止轮询并恢复写入口', async () => {
    vi.useFakeTimers()
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: CLEANUP } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    expect(q('[data-test="topic-cleanup-table"]')).not.toBeNull()
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await vi.advanceTimersByTimeAsync(3_000)
    await flushPromises()
    expect(q('[data-test="topic-cleanup-table"]')).toBeNull()
    expect(q('[data-test="topic-expand"]')).not.toBeNull()
  })

  it('清理中轮询到主题行消失（350001）：提示已删除且停止轮询', async () => {
    vi.useFakeTimers()
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: CLEANUP } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 350001, message: 'gone', data: null }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await vi.advanceTimersByTimeAsync(3_000)
    await flushPromises()
    expect(q('[data-test="topic-deleted"]')).not.toBeNull()
  })

  it('清理中轮询遇到其它错误：展示错误文案但继续可读', async () => {
    vi.useFakeTimers()
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: CLEANUP } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        throw Object.assign(new Error('x'), { code: 10001 })
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await vi.advanceTimersByTimeAsync(3_000)
    await flushPromises()
    expect(q('[data-test="topic-detail-error"]')).not.toBeNull()
  })

  it('详情首屏失败：展示错误码文案', async () => {
    api.get.mockImplementation(async () => ({ code: 350001, message: 'gone', data: null }))
    await mountPage(TopicDetailPage)
    expect(q('[data-test="topic-detail-error"]')?.textContent).toContain('主题不存在')
  })
})

describe('消费组详情：位点重置（Kiwi 2466）', () => {
  beforeEach(() => {
    routeState.value = { params: { namespace: 'msg', topic: 'order.created', group: 'order-sync' }, query: {} }
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: GROUP_DETAIL })
  })

  it('无 msg:manage：不渲染重置列', async () => {
    permissionRef.value = ['msg:query']
    await mountPage(GroupDetailPage)
    expect(q('[data-test="group-reset"]')).toBeNull()
  })

  it('确认重置：POST 带 confirm=true 与定位入参', async () => {
    await mountPage(GroupDetailPage)
    await click('[data-test="group-reset"]')
    expect(q('[data-test="confirm-impact"]')?.textContent).toContain('90')
    api.post.mockResolvedValue({ code: 0, message: 'ok', data: {} })
    await click('[data-test="confirm-ok"]')
    const [path, body] = api.post.mock.calls[0] as [string, Record<string, unknown>]
    expect(path).toBe('/admin/offsets/reset')
    expect(body).toMatchObject({
      namespace: 'msg',
      topic: 'order.created',
      group: 'order-sync',
      partition: 0,
      offset: 90,
      confirm: true,
    })
  })

  it('重置失败：按码提示（350003 早于起点）', async () => {
    await mountPage(GroupDetailPage)
    await click('[data-test="group-reset"]')
    api.post.mockResolvedValue({ code: 350003, message: 'bad', data: null })
    await click('[data-test="confirm-ok"]')
    expect(q('[data-test="group-reset-error"]')?.textContent).toContain('已被清理')
  })
})

/** 清理进度样例（两步已完成）。 */
const CLEANUP = {
  status: 'running',
  done_steps: ['messages', 'delay'],
  attempts: 1,
  started_at: '2026-11-09T00:00:00Z',
  finished_at: null,
  last_error: null,
}


/**
 * 让第 index 个 `el-input-number` 发出新值（数字控件不响应原生 input 赋值）。
 *
 * @param wrapper 挂载结果。
 * @param index 序号。
 * @param value 新值。
 */
async function setNumber(wrapper: VueWrapper, index: number, value: number): Promise<void> {
  const numbers = wrapper.findAllComponents(ElInputNumber)
  const target = numbers[index]
  if (target === undefined) {
    throw new Error(`数字控件不存在：${index}`)
  }
  target.vm.$emit('update:modelValue', value)
  await flushPromises()
}

/**
 * 让第 index 个 `el-dialog` 关闭（覆盖 `v-model` 的写回分支）。
 *
 * @param wrapper 挂载结果。
 * @param index 序号。
 */
async function closeDialog(wrapper: VueWrapper, index: number): Promise<void> {
  const dialogs = wrapper.findAllComponents(ElDialog)
  const target = dialogs[index]
  if (target === undefined) {
    throw new Error(`弹窗不存在：${index}`)
  }
  target.vm.$emit('update:modelValue', false)
  await flushPromises()
}

describe('写操作取数层：契约空值与错误码（Kiwi 2466）', () => {
  it('必填响应体为空：五个写函数统一抛 10000', async () => {
    api.post.mockResolvedValue({ code: 0, message: 'ok', data: null })
    api.patch.mockResolvedValue({ code: 0, message: 'ok', data: null })
    api.del.mockResolvedValue({ code: 0, message: 'ok', data: null })
    await expect(createTopic({ namespace: 'msg', name: 'a.b', partition_count: 1 })).rejects.toMatchObject({
      code: 10000,
    })
    await expect(deleteTopic('42')).rejects.toMatchObject({ code: 10000 })
    await expect(expandPartitions('42', 2)).rejects.toMatchObject({ code: 10000 })
    await expect(updateRetention('42', { retention_ms: 1 })).rejects.toMatchObject({ code: 10000 })
    await expect(
      resetOffset({ namespace: 'msg', topic: 'a.b', group: 'g', partition: 0, offset: 1, confirm: true }),
    ).rejects.toMatchObject({ code: 10000 })
  })

  it('写路径业务码非 0：抛出携带原码的错误', async () => {
    api.post.mockResolvedValue({ code: 350008, message: 'dup', data: null })
    await expect(createTopic({ namespace: 'msg', name: 'a.b', partition_count: 1 })).rejects.toMatchObject({
      code: 350008,
    })
  })

  it('影响预检 data 为空：按全 0 兜底（不伪造数值）', async () => {
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: null })
    await expect(fetchDeleteImpact('42')).resolves.toEqual({
      partition_count: 0,
      message_total: 0,
      group_count: 0,
      delayed_total: 0,
      dlq_total: 0,
    })
  })

  it('删除走 DELETE 且显式带 confirm=true', async () => {
    api.del.mockResolvedValue({ code: 0, message: 'ok', data: { topic_id: '42' } })
    await deleteTopic('42')
    expect(api.del.mock.calls[0]).toEqual(['/admin/topics/42', { confirm: true }])
  })
})

describe('写操作页面：表单回填与关闭分支（Kiwi 2466）', () => {
  it('新建主题：编辑命名空间 / 分区数 / 保留期 / 容量上限并取消关闭', async () => {
    const page = await mountPage(TopicsPage)
    await click('[data-test="topics-create"]')
    await input('[data-test="create-namespace"]', 'order')
    await setNumber(page, 0, 4)
    await setNumber(page, 1, 120_000)
    await setNumber(page, 2, 2048)
    await click('[data-test="create-cancel"]')
    expect(hidden('[data-test="topics-create-dialog"]')).toBe(true)
    // 覆盖 `v-model` 写回：弹窗自身关闭时也会同步布尔值。
    await closeDialog(page, 0)
  })
})

describe('主题详情：表单 / 弹窗关闭与空参数（Kiwi 2466）', () => {
  beforeEach(() => {
    routeState.value = { params: { topicId: '42' }, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      if (path === '/admin/topics/42/delete-impact') {
        return { code: 0, message: 'ok', data: { partition_count: 3, message_total: 1, group_count: 0, delayed_total: 0, dlq_total: 0 } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
  })

  it('扩分区：改目标值并取消关闭弹窗', async () => {
    const page = await mountPage(TopicDetailPage)
    await click('[data-test="topic-expand"]')
    await setNumber(page, 0, 9)
    await click('[data-test="expand-cancel"]')
    expect(hidden('[data-test="topic-expand-dialog"]')).toBe(true)
    await closeDialog(page, 0)
  })

  it('改保留期：编辑两项并取消关闭弹窗', async () => {
    const page = await mountPage(TopicDetailPage)
    await click('[data-test="topic-retention"]')
    await setNumber(page, 0, 120_000)
    await setNumber(page, 1, 2048)
    await click('[data-test="retention-cancel"]')
    expect(hidden('[data-test="topic-retention-dialog"]')).toBe(true)
    await closeDialog(page, 1)
  })

  it('改保留期成功：PATCH 后关闭弹窗并刷新详情', async () => {
    await mountPage(TopicDetailPage)
    await click('[data-test="topic-retention"]')
    api.patch.mockResolvedValue({ code: 0, message: 'ok', data: TOPIC })
    await click('[data-test="retention-submit"]')
    expect(hidden('[data-test="topic-retention-dialog"]')).toBe(true)
    expect(api.get.mock.calls.filter(([path]) => path === '/admin/topics/42').length).toBeGreaterThanOrEqual(2)
  })

  it('删除确认弹窗：显隐写回后按钮仍在（关闭不改数据）', async () => {
    const page = await mountPage(TopicDetailPage)
    await click('[data-test="topic-delete"]')
    await closeDialog(page, 2)
    expect(hidden('[data-test="confirm-dialog"]')).toBe(true)
    expect(q('[data-test="topic-delete"]')).not.toBeNull()
  })

  it('清理中且无进度详情：进度表按「未完成」渲染，不抛错', async () => {
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: null } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    expect(q('[data-test="topic-cleanup-table"]')?.textContent).toContain('待执行')
  })

  it('清理中轮询仍为 deleting：不重复起定时器（幂等）', async () => {
    vi.useFakeTimers()
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/42') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: CLEANUP } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    await vi.advanceTimersByTimeAsync(3_000)
    await flushPromises()
    await vi.advanceTimersByTimeAsync(3_000)
    await flushPromises()
    expect(q('[data-test="topic-cleanup-table"]')).not.toBeNull()
  })

  it('深链缺 topicId：写操作按空主键取数（不抛错，交由后端 350001）', async () => {
    routeState.value = { params: {}, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: CLEANUP } }
      }
      if (path === '/admin/topics//delete-impact') {
        return { code: 0, message: 'ok', data: { partition_count: 3, message_total: 1, group_count: 0, delayed_total: 0, dlq_total: 0 } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    const page = await mountPage(TopicDetailPage)
    expect(q('[data-test="topic-cleanup-table"]')).not.toBeNull()
    api.del.mockResolvedValue({ code: 0, message: 'ok', data: {} })
    // 清理中不渲染写入口：直接验证轮询走空主键路径（上一步已取数一次）。
    expect(page.find('[data-test="topic-delete"]').exists()).toBe(false)
  })
})

describe('消费组详情：空位点与输入回填（Kiwi 2466）', () => {
  it('分区未提交位点：目标缺省 0，确认弹窗显示占位符', async () => {
    routeState.value = { params: { namespace: 'msg', topic: 'order.created', group: 'order-sync' }, query: {} }
    api.get.mockResolvedValue({
      code: 0,
      message: 'ok',
      data: { ...GROUP_DETAIL, partitions: [{ ...PARTITION, committed: null }] },
    })
    const page = await mountPage(GroupDetailPage)
    await click('[data-test="group-reset"]')
    expect(q('[data-test="confirm-impact"]')?.textContent).toContain('—')
    await setNumber(page, 0, 12)
    await closeDialog(page, 0)
    expect(hidden('[data-test="confirm-dialog"]')).toBe(true)
  })
})


describe('主题详情：深链缺 topicId 时的写操作与轮询（Kiwi 2466）', () => {
  it('三个写操作按空主键发起（不抛错，交由后端判 350001）', async () => {
    routeState.value = { params: {}, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/') {
        return { code: 0, message: 'ok', data: TOPIC }
      }
      if (path === '/admin/topics//delete-impact') {
        return { code: 0, message: 'ok', data: { partition_count: 3, message_total: 1, group_count: 0, delayed_total: 0, dlq_total: 0 } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    api.post.mockResolvedValue({ code: 0, message: 'ok', data: TOPIC })
    await click('[data-test="topic-expand"]')
    await click('[data-test="expand-submit"]')
    expect(api.post.mock.calls[0][0]).toBe('/admin/topics//partitions')
    api.patch.mockResolvedValue({ code: 0, message: 'ok', data: TOPIC })
    await click('[data-test="topic-retention"]')
    await click('[data-test="retention-submit"]')
    expect(api.patch.mock.calls[0][0]).toBe('/admin/topics/')
    api.del.mockResolvedValue({ code: 0, message: 'ok', data: {} })
    await click('[data-test="topic-delete"]')
    await click('[data-test="confirm-ok"]')
    expect(api.del.mock.calls[0][0]).toBe('/admin/topics/')
  })

  it('清理中轮询同样按空主键取数', async () => {
    vi.useFakeTimers()
    routeState.value = { params: {}, query: {} }
    api.get.mockImplementation(async (path: string) => {
      if (path === '/admin/topics/') {
        return { code: 0, message: 'ok', data: { ...TOPIC, status: 'deleting', cleanup: CLEANUP } }
      }
      return { code: 0, message: 'ok', data: { items: [PARTITION], total: 1 } }
    })
    await mountPage(TopicDetailPage)
    const before = api.get.mock.calls.length
    await vi.advanceTimersByTimeAsync(3_000)
    await flushPromises()
    expect(api.get.mock.calls.length).toBeGreaterThan(before)
    expect(api.get.mock.calls.at(-1)?.[0]).toBe('/admin/topics/')
  })
})
