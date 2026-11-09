// kiwi_id: 2454
/** 消息配额只读页签：六维渲染 / 状态 / 失败降级 / 非租户表单不渲染 / 无提交器。 */

import { provideModuleSlotContext } from '@bms/ui_ep'
import ElementPlus from 'element-plus'
import { mount } from '@vue/test-utils'
import { defineComponent, h } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import QuotaPanel from '../src/components/QuotaPanel.vue'
import { fetchQuotas } from '../src/services/quota-service'

vi.mock('../src/services/quota-service', () => ({ fetchQuotas: vi.fn() }))

const fetchMock = vi.mocked(fetchQuotas)

/**
 * 以宿主槽上下文挂载插件件。
 *
 * @param context 槽上下文。
 */
function mountPanel(context: Record<string, unknown>) {
  const Host = defineComponent({
    setup() {
      provideModuleSlotContext(() => context)
      return () => h(QuotaPanel)
    },
  })
  return mount(Host, { global: { plugins: [ElementPlus] } })
}

const ITEMS = [
  { quota_key: 'topic_count', namespace: '', limit: 10, soft_limit: 8, hard_limit: 10, unit: 'count', period: 'none', usage: 10, state: 'hard_exceeded', source: 'configured', updated_at: '2026-11-08T10:00:00' },
  { quota_key: 'group_count', namespace: '', limit: 100, soft_limit: 80, hard_limit: 100, unit: 'count', period: 'none', usage: 1, state: 'ok', source: 'configured', updated_at: null },
  { quota_key: 'produce_rate', namespace: '', limit: 1000, soft_limit: 800, hard_limit: 1000, unit: 'qps', period: 'none', usage: null, state: 'unavailable', source: 'default', updated_at: null },
  { quota_key: 'storage_bytes', namespace: '', limit: 1048576, soft_limit: 838860, hard_limit: 1048576, unit: 'bytes', period: 'none', usage: 2048, state: 'soft_exceeded', source: 'default', updated_at: null },
  { quota_key: 'unacked_count', namespace: '', limit: null, soft_limit: null, hard_limit: null, unit: 'count', period: 'none', usage: 0, state: 'unconfigured', source: 'default', updated_at: null },
  { quota_key: 'connection_count', namespace: '', limit: 5, soft_limit: 4, hard_limit: 5, unit: 'count', period: 'none', usage: 1, state: 'ok', source: 'configured', updated_at: null },
]

describe('QuotaPanel（Kiwi 2454）', () => {
  beforeEach(() => {
    fetchMock.mockReset()
  })

  it('租户表单：渲染六维配额、状态与单位格式化', async () => {
    fetchMock.mockResolvedValue({ items: ITEMS })
    const wrapper = mountPanel({ formKey: 'tenant', recordId: '7' })
    await Promise.resolve()
    await Promise.resolve()
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const text = wrapper.text()
    expect(text).toContain('主题数')
    expect(text).toContain('已超限')
    expect(text).toContain('1 MiB')
    expect(text).toContain('2 KiB')
    expect(text).toContain('1000/s')
    expect(text).toContain('不限')
    const panel = wrapper.find('[data-test="msg-quota-panel"]')
    expect(panel.attributes('data-tenant')).toBe('7')
  })

  it('非租户表单：不请求、不渲染', async () => {
    const wrapper = mountPanel({ formKey: 'user', recordId: '7' })
    await Promise.resolve()
    expect(fetchMock).not.toHaveBeenCalled()
    expect(wrapper.find('[data-test="msg-quota-panel"]').exists()).toBe(false)
  })

  it('请求失败：降级为「暂不可用」，不影响宿主页', async () => {
    fetchMock.mockRejectedValue(new Error('boom'))
    const wrapper = mountPanel({ formKey: 'tenant', recordId: '7' })
    await Promise.resolve()
    await Promise.resolve()
    expect(wrapper.find('[data-test="msg-quota-unavailable"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('暂不可用')
  })

  it('宿主未注入记录主键、响应未带 items：空行与空占位', async () => {
    fetchMock.mockResolvedValue({} as never)
    const wrapper = mountPanel({ formKey: 'tenant' })
    await Promise.resolve()
    await Promise.resolve()
    const panel = wrapper.find('[data-test="msg-quota-panel"]')
    expect(panel.attributes('data-tenant')).toBe('')
    expect(wrapper.find('[data-test="msg-quota-table"]').exists()).toBe(true)
  })

  it('只读：不注册提交器（宿主保存不包含本页签）', async () => {
    const registerSubmitter = vi.fn()
    fetchMock.mockResolvedValue({ items: ITEMS })
    mountPanel({ formKey: 'tenant', recordId: '7', registerSubmitter })
    await Promise.resolve()
    await Promise.resolve()
    expect(registerSubmitter).not.toHaveBeenCalled()
  })
})
