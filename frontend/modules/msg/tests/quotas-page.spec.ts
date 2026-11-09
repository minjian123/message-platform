// kiwi_id: 2466
/** 配额管理页：六维可编辑 / 软限校验 / 整批保存 / 失败降级。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { api, permissionRef } = vi.hoisted(() => ({
  api: { get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), del: vi.fn() },
  permissionRef: { value: undefined as readonly string[] | undefined },
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => api }),
    hostPermissions: () => permissionRef.value,
  }
})

import QuotasPage from '../src/views/QuotasPage.vue'

/** 配额视图样例（两维：一维已配置、一维不限）。 */
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
    {
      quota_key: 'storage_bytes',
      namespace: '',
      limit: null,
      soft_limit: null,
      hard_limit: null,
      unit: 'bytes',
      period: 'none',
      usage: null,
      state: 'unconfigured',
      source: 'default',
      updated_at: null,
    },
  ],
}

let wrapper: VueWrapper | undefined

/**
 * 挂载页面。
 *
 * @returns 挂载结果。
 */
async function mountPage(): Promise<VueWrapper> {
  const mounted = mount(QuotasPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
  await flushPromises()
  wrapper = mounted
  return mounted
}

/**
 * 改写表格内某列第 index 个输入框（经原生 DOM 取数：`el-input` 的 `data-test` 落在容器上）。
 *
 * @param wrapper 挂载结果。
 * @param column 列键（`quota-hard` / `quota-soft`）。
 * @param index 同列序号。
 * @param value 新值。
 */
async function editCell(wrapper: VueWrapper, column: string, index: number, value: string): Promise<void> {
  const nodes = wrapper.element.querySelectorAll<HTMLElement>(`[data-test="${column}"]`)
  const node = nodes[index]
  // `data-test` 可能落在 `el-input` 容器上，也可能被组件透传到内部 `input`，两种都接受。
  const element = node instanceof HTMLInputElement ? node : node?.querySelector('input')
  if (element === undefined || element === null) {
    throw new Error(`输入框不存在：${column}[${index}]`)
  }
  element.value = value
  element.dispatchEvent(new Event('input'))
  await flushPromises()
}

beforeEach(() => {
  permissionRef.value = ['msg:query', 'msg:manage']
  api.get.mockReset()
  api.put.mockReset()
  api.get.mockResolvedValue({ code: 0, message: 'ok', data: QUOTA_VIEW })
})

afterEach(() => {
  wrapper?.unmount()
  wrapper = undefined
  document.body.innerHTML = ''
})

describe('配额管理页（Kiwi 2466）', () => {
  it('首屏按租户级取数并渲染六维行的可编辑硬限 / 软限', async () => {
    const page = await mountPage()
    expect(api.get).toHaveBeenCalledWith('/admin/quotas', undefined)
    expect(page.findAll('[data-test="quota-hard"]')).toHaveLength(2)
    expect(page.findAll('[data-test="quota-soft"]')).toHaveLength(2)
    expect(page.find('[data-test="quotas-table"]').text()).toContain('主题数')
  })

  it('切换作用域：带 namespace 重新取数并清空上一轮保存态', async () => {
    const page = await mountPage()
    await page.findComponent({ name: 'ElSelect' }).vm.$emit('update:modelValue', 'msg')
    await page.findComponent({ name: 'ElSelect' }).vm.$emit('change', 'msg')
    await flushPromises()
    expect(api.get).toHaveBeenLastCalledWith('/admin/quotas', { namespace: 'msg' })
  })

  it('软限大于硬限：提示且禁用保存（不发请求）', async () => {
    const page = await mountPage()
    await editCell(page, 'quota-soft', 0, '99')
    expect(page.find('[data-test="quotas-soft-warning"]').exists()).toBe(true)
    const save = page.find('[data-test="quotas-save"]')
    expect(save.attributes('disabled')).toBeDefined()
    await save.trigger('click')
    expect(api.put).not.toHaveBeenCalled()
  })

  it('保存：整批提交（空软限转 null、硬限 0 表示不限），成功后重新取数', async () => {
    const page = await mountPage()
    await editCell(page, 'quota-hard', 0, '20')
    api.put.mockResolvedValue({ code: 0, message: 'ok', data: QUOTA_VIEW })
    await page.find('[data-test="quotas-save"]').trigger('click')
    await flushPromises()
    const [path, body] = api.put.mock.calls[0] as [string, { items: Record<string, unknown>[] }]
    expect(path).toBe('/admin/quotas')
    expect(body.items[0]).toEqual({ quota_key: 'topic_count', namespace: '', limit: 20, soft_limit: 8 })
    expect(body.items[1]).toMatchObject({ quota_key: 'storage_bytes', limit: 0, soft_limit: null })
    expect(api.get).toHaveBeenCalledTimes(2)
  })

  it('保存失败：按错误码展示文案', async () => {
    const page = await mountPage()
    api.put.mockResolvedValue({ code: 10001, message: 'bad', data: null })
    await page.find('[data-test="quotas-save"]').trigger('click')
    await flushPromises()
    expect(page.find('[data-test="quotas-save-error"]').text()).toContain('参数非法')
  })

  it('取数失败：展示降级提示且表格为空', async () => {
    api.get.mockRejectedValue(new Error('down'))
    const page = await mountPage()
    expect(page.find('[data-test="quotas-error"]').exists()).toBe(true)
    expect(page.findAll('[data-test="quota-hard"]')).toHaveLength(0)
  })

  it('保存响应 data 为空：按空清单兜底并重新取数', async () => {
    const page = await mountPage()
    api.put.mockResolvedValue({ code: 0, message: 'ok', data: null })
    api.get.mockResolvedValue({ code: 0, message: 'ok', data: { items: null } })
    await page.find('[data-test="quotas-save"]').trigger('click')
    await flushPromises()
    expect(api.get).toHaveBeenCalledTimes(2)
    expect(page.findAll('[data-test="quota-hard"]')).toHaveLength(0)
  })

  it('硬限留空：按 0（不限）提交', async () => {
    const page = await mountPage()
    await editCell(page, 'quota-hard', 0, '')
    api.put.mockResolvedValue({ code: 0, message: 'ok', data: QUOTA_VIEW })
    await page.find('[data-test="quotas-save"]').trigger('click')
    await flushPromises()
    const [, body] = api.put.mock.calls[0] as [string, { items: Record<string, unknown>[] }]
    expect(body.items[0]).toMatchObject({ quota_key: 'topic_count', limit: 0 })
  })

  it('无 msg:manage：不渲染保存按钮（仍可查看）', async () => {
    permissionRef.value = ['msg:query']
    const page = await mountPage()
    expect(page.find('[data-test="quotas-save"]').exists()).toBe(false)
    expect(page.findAll('[data-test="quota-hard"]')).toHaveLength(2)
  })
})
