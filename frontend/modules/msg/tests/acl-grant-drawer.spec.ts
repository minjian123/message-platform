// kiwi_id: 2470
/** 授予抽屉：规则行增删 / 校验 / 单条与批量提交 / 更新锁定 / 无变化 / 错误码。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet, scopePut, platformGet } = vi.hoisted(() => ({
  scopeGet: vi.fn(),
  scopePut: vi.fn(),
  platformGet: vi.fn(),
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => wrapScope({ get: scopeGet, put: scopePut }), get: platformGet }),
    hostPermissions: () => ['msg:manage', 'user:query', 'role:query'],
  }
})

import { ElSelect } from 'element-plus'
import AclGrantDrawer, { type AclGrantPrefill } from '../src/components/AclGrantDrawer.vue'
import AclNamespaceSelect from '../src/components/AclNamespaceSelect.vue'
import AclPrincipalSelect from '../src/components/AclPrincipalSelect.vue'
import AclTopicSelect from '../src/components/AclTopicSelect.vue'

/**
 * 按 data-test 找 ElSelect 组件（根 DOM 是 div，不能直接 setValue）。
 *
 * @param wrapper 挂载结果。
 * @param test data-test 值。
 */
function selectByTest(wrapper: VueWrapper, test: string) {
  return wrapper.findAllComponents(ElSelect).find((item) => item.attributes('data-test') === test)
}

/** 主题行。 */
const TOPIC = {
  id: '42',
  tenant_id: '1',
  name: 'order.created',
  namespace: 'msg',
  partition_count: 1,
  retention_ms: 86400000,
  retention_bytes: 0,
  cleanup_policy: 'delete',
  status: 'active',
  created_at: '2026-11-09T00:00:00Z',
  cleanup: null,
}
/** 用户行。 */
const USER = { id: '7001', username: 'zhangsan', name: '张三', status: 'enabled', last_login_at: null }

let mounted: VueWrapper | undefined

/** 等待异步结算。 */
async function settle(): Promise<void> {
  await flushPromises()
  await flushPromises()
}

/** 取 body 内元素（弹窗 teleport 到 body）。 */
function q(selector: string): HTMLElement | null {
  return document.body.querySelector<HTMLElement>(selector)
}

/** 挂载抽屉。 */
async function mountDrawer(mode: 'create' | 'update' | 'regrant', prefill: AclGrantPrefill | null = null) {
  const wrapper = mount(AclGrantDrawer, {
    props: {
      modelValue: true,
      mode,
      prefill,
      permissionCodes: ['msg:manage', 'user:query', 'role:query'],
    },
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  await settle()
  mounted = wrapper
  return wrapper
}

/** 更新模式预填。 */
const PREFILL: AclGrantPrefill = {
  namespace: 'msg',
  topic: 'order.created',
  principalType: 'user',
  principalKey: '7001',
  principalLabel: '张三',
  action: 'publish',
  effect: 'allow',
}

beforeEach(() => {
  scopeGet.mockReset()
  scopePut.mockReset()
  platformGet.mockReset()
  scopeGet.mockResolvedValue({ code: 0, data: { items: [TOPIC], page: 1, size: 20, total: 1 } })
  platformGet.mockResolvedValue({ list: [USER], total: 1, page: 1, size: 1 })
  scopePut.mockResolvedValue({ code: 0, data: { items: [], changed: ['9001'] } })
})

afterEach(() => {
  mounted?.unmount()
  mounted = undefined
  document.body.innerHTML = ''
})

describe('授予抽屉（Kiwi 2470）', () => {
  it('新建：默认一行，可增删；最后一行不可删', async () => {
    await mountDrawer('create')
    expect(q('[data-test="acl-grant-table"]')).not.toBeNull()
    q('[data-test="acl-grant-add"]')?.click()
    await settle()
    expect(document.body.querySelectorAll('[data-test="acl-grant-remove"]')).toHaveLength(2)
    const remove = document.body.querySelectorAll<HTMLElement>('[data-test="acl-grant-remove"]')
    remove[1]?.click()
    await settle()
    expect(document.body.querySelectorAll('[data-test="acl-grant-remove"]')).toHaveLength(1)
  })

  it('空行提交：校验失败且不发请求', async () => {
    await mountDrawer('create')
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(scopePut).not.toHaveBeenCalled()
    expect(document.body.textContent).toContain('请补全')
  })

  it('主题选中自动带出命名空间；单条提交按六字段组装', async () => {
    const wrapper = await mountDrawer('create')
    wrapper.findComponent(AclNamespaceSelect).vm.$emit('update:modelValue', 'msg')
    const topic = wrapper.findComponent(AclTopicSelect)
    topic.vm.$emit('update:modelValue', 'order.created')
    topic.vm.$emit('select', { name: 'order.created', namespace: 'msg' })
    wrapper.findComponent(AclPrincipalSelect).vm.$emit('update:modelValue', '7001')
    wrapper.findComponent(AclPrincipalSelect).vm.$emit('update:label', '张三')
    await settle()
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(scopePut).toHaveBeenCalledWith('/admin/acls', {
      items: [
        {
          namespace: 'msg',
          topic: 'order.created',
          principal_type: 'user',
          principal_key: '7001',
          action: 'publish',
          effect: 'allow',
        },
      ],
    })
    expect(wrapper.emitted('saved')?.[0]).toEqual([{ changed: 1 }])
    expect(wrapper.emitted('update:modelValue')?.[0]).toEqual([false])
  })

  it('批量多行：同批提交；重复规则被拦截', async () => {
    const wrapper = await mountDrawer('create')
    q('[data-test="acl-grant-add"]')?.click()
    await settle()
    for (const component of wrapper.findAllComponents(AclPrincipalSelect)) {
      component.vm.$emit('update:modelValue', '7001')
    }
    for (const component of wrapper.findAllComponents(AclTopicSelect)) {
      component.vm.$emit('update:modelValue', 'order.created')
      component.vm.$emit('select', { name: 'order.created', namespace: 'msg' })
    }
    await settle()
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(scopePut).not.toHaveBeenCalled()
    expect(document.body.textContent).toContain('同批存在重复规则')
  })

  it('幂等无变化：saved 传 changed=0', async () => {
    scopePut.mockResolvedValueOnce({ code: 0, data: { items: [], changed: [] } })
    const wrapper = await mountDrawer('update', PREFILL)
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(wrapper.emitted('saved')?.[0]).toEqual([{ changed: 0 }])
  })

  it('350027：显示页面级「无目标主题的管理权限」', async () => {
    scopePut.mockResolvedValueOnce({ code: 350027, data: null })
    await mountDrawer('update', PREFILL)
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(document.body.textContent).toContain('无目标主题的管理权限')
  })

  it('通用错误码：展示 error.<码> 文案；取消关闭', async () => {
    scopePut.mockResolvedValueOnce({ code: 350001, data: null })
    const wrapper = await mountDrawer('update', PREFILL)
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(document.body.textContent).toContain('主题不存在')
    q('[data-test="acl-grant-cancel"]')?.click()
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([false])
  })

  it('主体标识非数字：校验拦截；动作 / 效果可改', async () => {
    const wrapper = await mountDrawer('create')
    wrapper.findComponent(AclTopicSelect).vm.$emit('update:modelValue', 'order.created')
    wrapper.findComponent(AclTopicSelect).vm.$emit('select', { name: 'order.created', namespace: 'msg' })
    wrapper.findComponent(AclPrincipalSelect).vm.$emit('update:modelValue', 'abc')
    await settle()
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(scopePut).not.toHaveBeenCalled()
    expect(document.body.textContent).toContain('主体标识须为数字')

    wrapper.findComponent(AclPrincipalSelect).vm.$emit('update:modelValue', '7001')
    selectByTest(wrapper, 'acl-grant-action')?.vm.$emit('update:modelValue', 'subscribe')
    selectByTest(wrapper, 'acl-grant-effect')?.vm.$emit('update:modelValue', 'deny')
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(scopePut).toHaveBeenCalledWith('/admin/acls', {
      items: [expect.objectContaining({ action: 'subscribe', effect: 'deny' })],
    })
  })

  it('主体类型切换清空主体；单行时删除无效；超过上限提示', async () => {
    const wrapper = await mountDrawer('create')
    wrapper.findComponent(AclPrincipalSelect).vm.$emit('update:modelValue', '7001')
    const typeSelect = selectByTest(wrapper, 'acl-grant-principal-type')
    typeSelect?.vm.$emit('update:modelValue', 'role')
    typeSelect?.vm.$emit('change', 'role')
    await settle()
    expect(wrapper.findComponent(AclPrincipalSelect).props('principalType')).toBe('role')

    q('[data-test="acl-grant-remove"]')?.click()
    await settle()
    expect(document.body.querySelectorAll('[data-test="acl-grant-remove"]')).toHaveLength(1)

    for (let index = 1; index < 50; index += 1) {
      q('[data-test="acl-grant-add"]')?.click()
    }
    await settle()
    q('[data-test="acl-grant-add"]')?.click()
    await settle()
    expect(document.body.textContent).toContain('单批规则过多')
  })

  it('changed 缺省：按 0 条变更反馈', async () => {
    scopePut.mockResolvedValueOnce({ code: 0, data: { items: [] } })
    const wrapper = await mountDrawer('update', PREFILL)
    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(wrapper.emitted('saved')?.[0]).toEqual([{ changed: 0 }])
  })

  it('更新模式：五字段锁定、仅效果可改；重新授予提示恢复启用', async () => {
    const wrapper = await mountDrawer('update', PREFILL)
    expect(document.body.textContent).toContain('命名空间 / 主题 / 主体 / 动作不可改')
    expect(q('[data-test="acl-grant-add"]')).toBeNull()
    expect(q('[data-test="acl-grant-locked"]')).not.toBeNull()

    q('[data-test="acl-grant-submit"]')?.click()
    await settle()
    expect(scopePut).toHaveBeenCalledWith('/admin/acls', {
      items: [
        {
          namespace: 'msg',
          topic: 'order.created',
          principal_type: 'user',
          principal_key: '7001',
          action: 'publish',
          effect: 'allow',
        },
      ],
    })

    wrapper.unmount()
    document.body.innerHTML = ''
    await mountDrawer('regrant', PREFILL)
    expect(q('[data-test="acl-grant-regrant"]')).not.toBeNull()
  })
})
