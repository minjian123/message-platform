// kiwi_id: 2470
/** 主题选择件：远程搜索 / 命名空间收窄 / 选中回传 / 失败兜底。 */

import ElementPlus, { ElOption, ElSelect } from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet } = vi.hoisted(() => ({ scopeGet: vi.fn() }))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return { ...actual, requiredHostApi: () => ({ product: () => wrapScope({ get: scopeGet }) }) }
})

import AclTopicSelect from '../src/components/AclTopicSelect.vue'

/** 主题行。 */
const TOPIC = { id: '1', name: 'order.created', namespace: 'msg', partition_count: 1 }

beforeEach(() => {
  scopeGet.mockReset()
})

describe('AclTopicSelect（Kiwi 2470）', () => {
  it('按命名空间搜索；选中回传主题与命名空间', async () => {
    scopeGet.mockResolvedValue({ code: 0, data: { items: [TOPIC], page: 1, size: 20, total: 1 } })
    const wrapper = mount(AclTopicSelect, {
      props: { modelValue: '', namespace: 'msg' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledWith('/admin/topics', { namespace: 'msg', page: 1, size: 20 })
    expect(wrapper.findAllComponents(ElOption).map((item) => item.props('value'))).toEqual(['msg/order.created'])

    wrapper.findComponent(ElSelect).vm.$emit('update:modelValue', 'msg/order.created')
    expect(wrapper.emitted('select')?.[0]).toEqual([{ name: 'order.created', namespace: 'msg' }])

    wrapper.findComponent(ElSelect).vm.$emit('update:modelValue', 'missing.topic')
    expect(wrapper.emitted('select')).toHaveLength(1)
    wrapper.unmount()
  })

  it('接口失败：候选为空；命名空间变化重新搜索', async () => {
    scopeGet.mockRejectedValue({ code: 350001 })
    const wrapper = mount(AclTopicSelect, {
      props: { modelValue: '', namespace: '' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    await flushPromises()
    expect(wrapper.findAllComponents(ElOption)).toHaveLength(0)

    await wrapper.setProps({ namespace: 'order' })
    await flushPromises()
    expect(scopeGet).toHaveBeenLastCalledWith('/admin/topics', { namespace: 'order', page: 1, size: 20 })
    wrapper.unmount()
  })

  it('已选主题不在候选时补入，保证回显', async () => {
    scopeGet.mockResolvedValue({ code: 0, data: { items: [], page: 1, size: 20, total: 0 } })
    const wrapper = mount(AclTopicSelect, {
      props: { modelValue: '', namespace: 'msg' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    await wrapper.setProps({ modelValue: 'manual.topic' })
    expect(wrapper.findAllComponents(ElOption).map((item) => item.props('value'))).toContain('msg/manual.topic')
    wrapper.unmount()
  })

  it('关键字非空搜索与响应缺 items 分支', async () => {
    scopeGet.mockResolvedValue({ code: 0, data: { page: 1, size: 20, total: 0 } })
    const wrapper = mount(AclTopicSelect, {
      props: { modelValue: '', namespace: 'msg' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    const state = (wrapper.vm as unknown as { $: { setupState: { search: (keyword: string) => Promise<void> } } }).$.setupState
    await state.search('ord')
    expect(scopeGet).toHaveBeenLastCalledWith('/admin/topics', { namespace: 'msg', keyword: 'ord', page: 1, size: 20 })
    expect(wrapper.findAllComponents(ElOption)).toHaveLength(0)
    wrapper.unmount()
  })
})
