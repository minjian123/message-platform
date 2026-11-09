// kiwi_id: 2470
/** 主体选择件：已知缺权限直接手填 / 搜索失败降级 / 手填数字校验。 */

import ElementPlus, { ElSelect } from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { platformGet } = vi.hoisted(() => ({ platformGet: vi.fn() }))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return { ...actual, requiredHostApi: () => ({ get: platformGet }) }
})

import AclPrincipalSelect from '../src/components/AclPrincipalSelect.vue'

let mounted: VueWrapper | undefined

/** 等待异步结算。 */
async function settle(): Promise<void> {
  await flushPromises()
  await flushPromises()
}

/** 挂载组件。 */
function mountSelect(
  principalType: string,
  permissionCodes: readonly string[] | undefined,
  extra: { modelValue?: string; label?: string } = {},
) {
  const wrapper = mount(AclPrincipalSelect, {
    props: { principalType, modelValue: '', permissionCodes, ...extra },
    global: { plugins: [ElementPlus] },
  })
  mounted = wrapper
  return wrapper
}

beforeEach(() => {
  platformGet.mockReset()
})

afterEach(() => {
  mounted?.unmount()
  mounted = undefined
})

describe('主体选择件（Kiwi 2470）', () => {
  it('已知缺 user:query：直接手填，不发请求', async () => {
    const wrapper = mountSelect('user', ['msg:manage'])
    await settle()
    expect(wrapper.find('[data-test="acl-principal-manual"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="acl-principal-search"]').exists()).toBe(false)
    expect(platformGet).not.toHaveBeenCalled()
  })

  it('角色缺 role:query：直接手填', async () => {
    const wrapper = mountSelect('role', ['msg:manage'])
    await settle()
    expect(wrapper.find('[data-test="acl-principal-manual"]').exists()).toBe(true)
  })

  it('搜索失败：切换手填并提示，不阻断输入', async () => {
    platformGet.mockRejectedValueOnce({ code: 350027 })
    const wrapper = mountSelect('user', undefined)
    const select = wrapper.findComponent(ElSelect)
    select.vm.$emit('visible-change', true)
    await settle()
    expect(wrapper.find('[data-test="acl-principal-manual"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="acl-principal-search-failed"]').text()).toContain('已切换手填')
  })

  it('手填：合法数字回传，非法数字提示且不回传', async () => {
    const wrapper = mountSelect('user', ['msg:manage'])
    await settle()
    const input = wrapper.find('[data-test="acl-principal-manual"]')
    await input.setValue('7001')
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual(['7001'])

    await input.setValue('abc')
    expect(wrapper.find('[data-test="acl-principal-digits"]').exists()).toBe(true)
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual(['7001'])
  })

  it('切换主体类型：清空标识与名称，按新类型重新判定', async () => {
    const wrapper = mountSelect('user', ['msg:manage'])
    await settle()
    await wrapper.setProps({ principalType: 'role' })
    await settle()
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([''])
    expect(wrapper.find('[data-test="acl-principal-manual"]').exists()).toBe(true)
  })

  it('搜索成功：候选可见，选中回传标识与名称', async () => {
    platformGet.mockResolvedValueOnce({
      list: [{ id: '7001', username: 'zhangsan', name: '张三', status: 'enabled', last_login_at: null }],
      total: 1,
      page: 1,
      size: 20,
    })
    const wrapper = mountSelect('user', ['user:query'])
    const select = wrapper.findComponent(ElSelect)
    select.vm.$emit('visible-change', true)
    await settle()
    select.vm.$emit('update:modelValue', '7001')
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual(['7001'])
    expect(wrapper.emitted('update:label')?.at(-1)).toEqual(['张三（zhangsan）'])
  })

  it('角色搜索成功；用户昵称为空时用账号；未知标识只清空名称', async () => {
    platformGet.mockResolvedValueOnce({
      list: [{ id: '11', code: 'ops', name: '运维', status: 'enabled', role_type: 'custom', builtin: false, subject_count: 0 }],
      total: 1,
      page: 1,
      size: 20,
    })
    let wrapper = mountSelect('role', ['role:query'])
    let select = wrapper.findComponent(ElSelect)
    select.vm.$emit('visible-change', true)
    await settle()
    select.vm.$emit('update:modelValue', '11')
    expect(wrapper.emitted('update:label')?.at(-1)).toEqual(['运维'])
    await wrapper.setProps({ modelValue: '11' })
    await wrapper.setProps({ modelValue: '12' })
    wrapper.unmount()
    mounted = undefined

    platformGet.mockResolvedValueOnce({
      list: [{ id: '7002', username: 'lisi', name: '', status: 'enabled', last_login_at: null }],
      total: 1,
      page: 1,
      size: 20,
    })
    wrapper = mountSelect('user', ['user:query'])
    select = wrapper.findComponent(ElSelect)
    select.vm.$emit('visible-change', true)
    await settle()
    select.vm.$emit('update:modelValue', '7002')
    expect(wrapper.emitted('update:label')?.at(-1)).toEqual(['lisi'])

    select.vm.$emit('update:modelValue', '9999')
    expect(wrapper.emitted('update:label')?.at(-1)).toEqual([''])
  })

  it('展开收起 / 搜索时已知缺权限 / 已选值补入候选', async () => {
    const wrapper = mountSelect('user', ['user:query'])
    const select = wrapper.findComponent(ElSelect)
    select.vm.$emit('visible-change', false)
    expect(platformGet).not.toHaveBeenCalled()

    await wrapper.setProps({ permissionCodes: ['msg:manage'] })
    select.vm.$emit('visible-change', true)
    await settle()
    expect(wrapper.find('[data-test="acl-principal-manual"]').exists()).toBe(true)
    wrapper.unmount()
    mounted = undefined

    const other = mountSelect('user', ['user:query'])
    await other.setProps({ modelValue: '7003' })
    await other.setProps({ label: '张三' })
    await settle()
    platformGet.mockResolvedValueOnce({ list: [], total: 0, page: 1, size: 20 })
    other.findComponent(ElSelect).vm.$emit('visible-change', true)
    await settle()
    expect(other.findComponent(ElSelect).props('modelValue')).toBe('7003')

    await other.setProps({ label: '' })
    await other.setProps({ modelValue: '7004' })
    platformGet.mockResolvedValueOnce({ list: [], total: 0, page: 1, size: 20 })
    other.findComponent(ElSelect).vm.$emit('visible-change', true)
    await settle()
    expect(other.findComponent(ElSelect).props('modelValue')).toBe('7004')
  })

  it('平台返回缺 list：按空候选处理', async () => {
    platformGet.mockResolvedValueOnce({ total: 0, page: 1, size: 20 })
    const wrapper = mountSelect('role', ['role:query', 'user:query'])
    const select = wrapper.findComponent(ElSelect)
    select.vm.$emit('visible-change', true)
    await settle()
    expect(wrapper.findComponent(ElSelect).exists()).toBe(true)

    platformGet.mockResolvedValueOnce({ total: 0, page: 1, size: 20 })
    await wrapper.setProps({ principalType: 'user' })
    await settle()
    wrapper.findComponent(ElSelect).vm.$emit('visible-change', true)
    await settle()
    expect(wrapper.findComponent(ElSelect).exists()).toBe(true)
  })

  it('初始已选值：无名称用标识占位，有名称用名称占位', async () => {
    const withoutLabel = mountSelect('user', ['user:query'], { modelValue: '7005' })
    await settle()
    expect(withoutLabel.findComponent(ElSelect).props('modelValue')).toBe('7005')
    expect(withoutLabel.findComponent(ElSelect).text()).toContain('7005')
    withoutLabel.unmount()
    mounted = undefined

    const withLabel = mountSelect('user', ['user:query'], { modelValue: '7006', label: '李四' })
    await settle()
    expect(withLabel.findComponent(ElSelect).props('modelValue')).toBe('7006')
    expect(withLabel.findComponent(ElSelect).text()).toContain('李四')
  })

  it('切换角色且缺权限：清空后直接手填', async () => {
    const wrapper = mountSelect('user', ['msg:manage'])
    await settle()
    await wrapper.setProps({ principalType: 'role' })
    await settle()
    expect(wrapper.find('[data-test="acl-principal-manual"]').exists()).toBe(true)
  })
})
