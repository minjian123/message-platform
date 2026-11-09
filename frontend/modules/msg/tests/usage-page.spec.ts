// kiwi_id: 2471
/** 租户用量页：四卡 + 口径说明 / 加载 / 失败重试 / 无权限 / 手动刷新。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet, permissionRef } = vi.hoisted(() => ({
  scopeGet: vi.fn(),
  permissionRef: { value: ['msg:query'] as readonly string[] | undefined },
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => wrapScope({ get: scopeGet }) }),
    hostPermissions: () => permissionRef.value,
  }
})

import UsagePage from '../src/views/UsagePage.vue'
import { message } from '../src/i18n/messages'

/** 契约样例数据。 */
const VIEW = {
  columns: [
    { key: 'topic_count', label: '主题数' },
    { key: 'partition_count', label: '分区数' },
    { key: 'storage_bytes', label: '消息存储' },
    { key: 'delivered_today', label: '今日投递量' },
  ],
  items: [{ topic_count: 24, partition_count: 96, storage_bytes: 3650722201, delivered_today: 182430 }],
  total: 1,
}

/**
 * 挂载页面。
 *
 * @returns 挂载结果。
 */
function mountPage() {
  return mount(UsagePage, { global: { plugins: [ElementPlus] } })
}

describe('UsagePage（Kiwi 2471）', () => {
  beforeEach(() => {
    scopeGet.mockReset()
    permissionRef.value = ['msg:query']
  })

  it('就绪：四卡 + 口径说明六条 + 副标题', async () => {
    scopeGet.mockResolvedValue(VIEW)
    const wrapper = mountPage()
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledWith('/admin/tenant-usage', {})
    const text = wrapper.text()
    expect(text).toContain(message('msg.admin.usage.title'))
    expect(text).toContain(message('msg.admin.usage.subtitle'))
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(4)
    expect(text).toContain(message('msg.usage.caption.topicCount'))
    expect(text).toContain(message('msg.usage.note.topicCount'))
    expect(text).toContain(message('msg.usage.note.storage'))
    expect(text).toContain(message('msg.usage.note.deliveredToday'))
    expect(text).toContain(message('msg.usage.note.source'))
    expect(text).toContain(message('msg.usage.note.grafana'))
    expect(wrapper.find('[data-test="usage-blocked"]').exists()).toBe(false)
  })

  it('首次加载：四块骨架，不显示旧值与空卡', async () => {
    scopeGet.mockReturnValue(new Promise(() => {}))
    const wrapper = mountPage()
    await flushPromises()
    expect(wrapper.findAll('[data-test="usage-skeleton"]')).toHaveLength(4)
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(0)
  })

  it('请求失败：整页降级为错误条 + 重试，不渲染四卡', async () => {
    scopeGet.mockRejectedValueOnce({ code: 350001 })
    const wrapper = mountPage()
    await flushPromises()
    expect(wrapper.find('[data-test="usage-blocked"]').exists()).toBe(true)
    expect(wrapper.text()).toContain(message('error.350001'))
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(0)

    scopeGet.mockResolvedValue(VIEW)
    await wrapper.find('[data-test="usage-retry"]').trigger('click')
    await flushPromises()
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(4)
  })

  it('无查询权限：提示条替代四卡，且不发请求', async () => {
    permissionRef.value = ['msg:manage']
    const wrapper = mountPage()
    await flushPromises()
    expect(scopeGet).not.toHaveBeenCalled()
    expect(wrapper.find('[data-test="usage-blocked"]').exists()).toBe(true)
    expect(wrapper.text()).toContain(message('state.denied'))
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(0)
  })

  it('手动刷新：再次取数并覆盖展示值', async () => {
    scopeGet.mockResolvedValueOnce(VIEW)
    const wrapper = mountPage()
    await flushPromises()
    scopeGet.mockResolvedValueOnce({ ...VIEW, items: [{ ...VIEW.items[0], topic_count: 30 }] })
    await wrapper.find('[data-test="usage-refresh"]').trigger('click')
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledTimes(2)
    expect(wrapper.find('[data-test="usage-card-topic_count"]').text()).toContain('30')
  })
})
