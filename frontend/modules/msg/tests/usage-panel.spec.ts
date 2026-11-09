// kiwi_id: 2471
/** 消息用量页签：非租户不渲染 / 2×2 四卡 / 租户对照提示 / 跳转 / 无权限与失败降级。 */

import { provideModuleSlotContext } from '@bms/ui_ep'
import ElementPlus from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { defineComponent, h, reactive } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { loadMock, permissionRef, push } = vi.hoisted(() => ({
  loadMock: vi.fn(),
  permissionRef: { value: ['msg:query'] as readonly string[] | undefined },
  push: vi.fn(),
}))

vi.mock('../src/services/usage-service', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/services/usage-service')>()
  return { ...actual, loadUsageCards: loadMock }
})

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return { ...actual, hostPermissions: () => permissionRef.value }
})

vi.mock('vue-router', () => ({
  useRoute: () => ({ path: '/tenant/records/1', params: {}, query: {} }),
  useRouter: () => ({ push, replace: vi.fn() }),
}))

import UsagePanel from '../src/components/UsagePanel.vue'
import { message } from '../src/i18n/messages'

/** 四列卡片行（loader 返回值）。 */
const PAGE = {
  items: [
    { key: 'topic_count', label: '主题数', value: 1280 },
    { key: 'partition_count', label: '分区数', value: 15360 },
    { key: 'storage_bytes', label: '消息存储', value: 3650722201 },
    { key: 'delivered_today', label: '今日投递量', value: 182430 },
  ],
  total: 4,
}

/**
 * 以宿主槽上下文挂载页签（上下文经 getter 传入，便于用例中途改变注入值）。
 *
 * @param getContext 槽上下文读取函数。
 */
function mountPanel(getContext: () => Record<string, unknown>) {
  const Host = defineComponent({
    setup() {
      provideModuleSlotContext(getContext)
      return () => h(UsagePanel)
    },
  })
  return mount(Host, { global: { plugins: [ElementPlus] } })
}

describe('UsagePanel（Kiwi 2471）', () => {
  beforeEach(() => {
    loadMock.mockReset()
    push.mockReset()
    permissionRef.value = ['msg:query']
  })

  it('租户表单：2×2 四卡 + 当前登录租户提示 + 记录页租户对照 + 跳转链接', async () => {
    loadMock.mockResolvedValue(PAGE)
    const wrapper = mountPanel(() => ({ formKey: 'tenant', recordId: '2026011500001' }))
    await flushPromises()
    expect(loadMock).toHaveBeenCalledTimes(1)
    const cards = wrapper.find('[data-test="usage-cards"]')
    expect(cards.attributes('data-layout')).toBe('2')
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(4)
    const hint = wrapper.find('[data-test="usage-panel-hint"]').text()
    expect(hint).toContain(message('msg.usage.tenantHint'))
    expect(hint).toContain(message('msg.usage.tenantTarget'))
    expect(hint).toContain('2026011500001')
    expect(wrapper.text()).toContain('1,280')
    expect(wrapper.text()).toContain('GiB')
    expect(wrapper.find('[data-test="usage-panel-open"]').exists()).toBe(true)
  })

  it('非租户表单：不请求、不渲染', async () => {
    const wrapper = mountPanel(() => ({ formKey: 'user', recordId: '7' }))
    await flushPromises()
    expect(loadMock).not.toHaveBeenCalled()
    expect(wrapper.find('[data-test="usage-panel"]').exists()).toBe(false)
  })

  it('记录页租户标识缺失：只显示「按当前登录租户统计」', async () => {
    loadMock.mockResolvedValue(PAGE)
    const wrapper = mountPanel(() => ({ formKey: 'tenant', recordId: '' }))
    await flushPromises()
    const hint = wrapper.find('[data-test="usage-panel-hint"]').text()
    expect(hint).toContain(message('msg.usage.tenantHint'))
    expect(hint).not.toContain(message('msg.usage.tenantTarget'))
  })

  it('宿主未注入记录页标识：同样只显示提示前半句', async () => {
    loadMock.mockResolvedValue(PAGE)
    const wrapper = mountPanel(() => ({ formKey: 'tenant' }))
    await flushPromises()
    const hint = wrapper.find('[data-test="usage-panel-hint"]').text()
    expect(hint).toContain(message('msg.usage.tenantHint'))
    expect(hint).not.toContain(message('msg.usage.tenantTarget'))
  })

  it('无查询权限：降级提示替代四卡与跳转，且不发请求', async () => {
    permissionRef.value = ['msg:manage']
    const wrapper = mountPanel(() => ({ formKey: 'tenant', recordId: '7' }))
    await flushPromises()
    expect(loadMock).not.toHaveBeenCalled()
    expect(wrapper.find('[data-test="usage-panel-degraded"]').exists()).toBe(true)
    expect(wrapper.text()).toContain(message('msg.usage.unavailable'))
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(0)
    expect(wrapper.find('[data-test="usage-panel-open"]').exists()).toBe(false)
  })

  it('请求失败：降级提示，不阻塞宿主页', async () => {
    loadMock.mockRejectedValue({ code: 350001 })
    const wrapper = mountPanel(() => ({ formKey: 'tenant', recordId: '7' }))
    await flushPromises()
    expect(wrapper.find('[data-test="usage-panel-degraded"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="usage-panel-open"]').exists()).toBe(false)
  })

  it('宿主后注入表单标识：由不渲染切到渲染并发起取数', async () => {
    loadMock.mockResolvedValue(PAGE)
    const context = reactive<Record<string, unknown>>({ formKey: '', recordId: '7' })
    const wrapper = mountPanel(() => context)
    await flushPromises()
    expect(loadMock).not.toHaveBeenCalled()

    context.formKey = 'tenant'
    await flushPromises()
    expect(loadMock).toHaveBeenCalledTimes(1)
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(4)
  })

  it('点击「打开用量页」跳独立页', async () => {
    loadMock.mockResolvedValue(PAGE)
    const wrapper = mountPanel(() => ({ formKey: 'tenant', recordId: '7' }))
    await flushPromises()
    await wrapper.find('[data-test="usage-panel-open"]').trigger('click')
    expect(push).toHaveBeenCalledWith('/msg/usage')
  })
})
