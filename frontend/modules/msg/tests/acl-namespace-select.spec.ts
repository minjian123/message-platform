// kiwi_id: 2470
/** 命名空间选择件：远程搜索 / 失败兜底 / 已选值回填。 */

import ElementPlus, { ElOption, ElSelect } from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet } = vi.hoisted(() => ({ scopeGet: vi.fn() }))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return { ...actual, requiredHostApi: () => ({ product: () => wrapScope({ get: scopeGet }) }) }
})

import AclNamespaceSelect from '../src/components/AclNamespaceSelect.vue'

/** 主题行。 */
const TOPIC = { id: '1', name: 'order.created', namespace: 'msg', partition_count: 1 }

beforeEach(() => {
  scopeGet.mockReset()
})

describe('AclNamespaceSelect（Kiwi 2470）', () => {
  it('挂载后取主题列表并去重命名空间；翻回已选值', async () => {
    scopeGet.mockResolvedValue({ code: 0, data: { items: [TOPIC, { ...TOPIC, id: '2', namespace: 'order' }], page: 1, size: 20, total: 2 } })
    const wrapper = mount(AclNamespaceSelect, {
      props: { modelValue: '' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    await flushPromises()
    expect(wrapper.findAllComponents(ElOption).map((item) => item.props('value'))).toEqual(['msg', 'order'])

    await wrapper.setProps({ modelValue: 'bms' })
    expect(wrapper.findAllComponents(ElOption).map((item) => item.props('value'))).toContain('bms')

    const select = wrapper.findComponent(ElSelect)
    select.vm.$emit('update:modelValue', 'order')
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual(['order'])

    await wrapper.setProps({ modelValue: 'msg' })
    await wrapper.setProps({ modelValue: '' })
    expect(wrapper.findAllComponents(ElOption).length).toBeGreaterThan(0)
    wrapper.unmount()
  })

  it('接口失败：不抛错、候选为空，仍可手输', async () => {
    scopeGet.mockRejectedValue({ code: 350001 })
    const wrapper = mount(AclNamespaceSelect, {
      props: { modelValue: 'msg' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    await flushPromises()
    expect(wrapper.findAllComponents(ElOption).map((item) => item.props('value'))).toEqual(['msg'])
    expect(wrapper.findComponent(ElSelect).props('allowCreate')).toBe(true)
    wrapper.unmount()
  })

  it('关键字非空搜索与响应缺 items 分支', async () => {
    scopeGet.mockResolvedValue({ code: 0, data: { page: 1, size: 20, total: 0 } })
    const wrapper = mount(AclNamespaceSelect, {
      props: { modelValue: '' },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    const state = (wrapper.vm as unknown as { $: { setupState: { search: (keyword: string) => Promise<void> } } }).$.setupState
    await state.search('ord')
    expect(scopeGet).toHaveBeenLastCalledWith('/admin/topics', { keyword: 'ord', page: 1, size: 20 })
    expect(wrapper.findAllComponents(ElOption)).toHaveLength(0)
    wrapper.unmount()
  })
})
