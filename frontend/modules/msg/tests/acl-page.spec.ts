// kiwi_id: 2470
/** 主题授权页：列表 / 过滤 / 分页 / 深链 / 四态 / 权限 / 名称解析 / 收回 / 编辑入口。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet, scopePut, scopeDel, platformGet, permissionRef, routeState, replace, push } = vi.hoisted(() => ({
  scopeGet: vi.fn(),
  scopePut: vi.fn(),
  scopeDel: vi.fn(),
  platformGet: vi.fn(),
  permissionRef: { value: ['msg:manage'] as readonly string[] | undefined },
  routeState: {
    value: { path: '/msg/acls', params: {} as Record<string, string>, query: {} as Record<string, string> },
  },
  replace: vi.fn(),
  push: vi.fn(),
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return {
    ...actual,
    requiredHostApi: () => ({
      product: () => wrapScope({ get: scopeGet, put: scopePut, del: scopeDel }),
      get: platformGet,
    }),
    hostPermissions: () => permissionRef.value,
  }
})

vi.mock('vue-router', () => ({
  useRoute: () => routeState.value,
  useRouter: () => ({ push, replace }),
}))

import { ElSelect } from 'element-plus'
import AclGrantDrawer from '../src/components/AclGrantDrawer.vue'
import MsgConfirmDialog from '../src/components/MsgConfirmDialog.vue'
import ReadOnlyList from '../src/components/ReadOnlyList.vue'
import AclPage from '../src/views/AclPage.vue'

/**
 * 按 data-test 找 ElSelect 组件（根 DOM 是 div，不能直接 setValue）。
 *
 * @param wrapper 挂载结果。
 * @param test data-test 值。
 */
function selectByTest(wrapper: VueWrapper, test: string) {
  return wrapper.findAllComponents(ElSelect).find((item) => item.attributes('data-test') === test)
}

/** 用户行。 */
const USER = { id: '7001', username: 'zhangsan', name: '张三', status: 'enabled', last_login_at: null }
/** 角色行。 */
const ROLE = {
  id: '11',
  code: 'ops',
  name: '运维角色',
  status: 'enabled',
  role_type: 'custom',
  builtin: false,
  subject_count: 1,
}

/** ACL 行（启用用户 / 启用角色 / 停用用户）。 */
const ITEMS = [
  {
    acl_id: '9001',
    namespace: 'msg',
    topic: 'order.created',
    principal_type: 'user',
    principal_key: '7001',
    action: 'publish',
    effect: 'allow',
    status: 'enabled',
    updated_at: '2026-11-09T10:00:00Z',
  },
  {
    acl_id: '9002',
    namespace: 'msg',
    topic: 'order.created',
    principal_type: 'role',
    principal_key: '11',
    action: 'subscribe',
    effect: 'deny',
    status: 'enabled',
    updated_at: '2026-11-09T09:00:00Z',
  },
  {
    acl_id: '9003',
    namespace: 'msg',
    topic: 'order.paid',
    principal_type: 'user',
    principal_key: '7002',
    action: 'describe',
    effect: 'allow',
    status: 'disabled',
    updated_at: '2026-11-08T09:00:00Z',
  },
]

let mounted: VueWrapper | undefined

/** 挂载页面（弹窗 teleport 到 body）。 */
async function mountPage(): Promise<VueWrapper> {
  const wrapper = mount(AclPage, {
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  await settle()
  mounted = wrapper
  return wrapper
}

/** 等待多层异步取数结算。 */
async function settle(): Promise<void> {
  await flushPromises()
  await flushPromises()
  await flushPromises()
}

/** 取 body 内元素（弹窗内容在 body）。 */
function q(selector: string): HTMLElement | null {
  return document.body.querySelector<HTMLElement>(selector)
}

beforeEach(() => {
  scopeGet.mockReset()
  scopePut.mockReset()
  scopeDel.mockReset()
  platformGet.mockReset()
  permissionRef.value = ['msg:manage', 'user:query', 'role:query']
  routeState.value = { path: '/msg/acls', params: {}, query: {} }
  replace.mockReset()
  push.mockReset()
  scopeGet.mockResolvedValue({ code: 0, data: { items: ITEMS, page: 1, size: 20, total: 3 } })
  platformGet.mockImplementation(async (_service: string, path: string) => {
    if (path === '/users') {
      return { list: [USER], total: 1, page: 1, size: 2 }
    }
    if (path === '/roles') {
      return { list: [ROLE], total: 1, page: 1, size: 1 }
    }
    throw { code: 10001 }
  })
  scopeDel.mockResolvedValue({ code: 0, data: { acl_id: '9001', changed: true } })
})

afterEach(() => {
  mounted?.unmount()
  mounted = undefined
  document.body.innerHTML = ''
})

describe('主题授权页：列表与四态（Kiwi 2470）', () => {
  it('渲染行 / 名称 / 动作 / 效果 / 状态，停用行无收回', async () => {
    const page = await mountPage()
    expect(page.find('[data-test="page-acls"]').exists()).toBe(true)
    expect(page.findAll('[data-test="acl-principal-name"]').map((node) => node.text())).toEqual([
      '张三',
      '运维角色',
    ])
    expect(page.findAll('[data-test="acl-principal-unavailable"]').map((node) => node.text())).toEqual([
      '名称暂不可用',
    ])
    expect(page.findAll('[data-test="acl-effect-tag"]').map((node) => node.text())).toEqual(['允许', '拒绝', '允许'])
    expect(page.findAll('[data-test="acl-status-tag"]').map((node) => node.text())).toEqual(['已启用', '已启用', '已停用'])
    expect(page.findAll('[data-test="acl-revoke"]')).toHaveLength(2)
    expect(page.findAll('[data-test="acl-update"]')).toHaveLength(2)
    expect(page.findAll('[data-test="acl-regrant"]')).toHaveLength(1)
  })

  it('深链：初始 query 回填筛选与页码', async () => {
    routeState.value = {
      path: '/msg/acls',
      params: {},
      query: { namespace: 'msg', principal_type: 'user', page: '2' },
    }
    await mountPage()
    expect(scopeGet).toHaveBeenCalledWith('/admin/acls', {
      namespace: 'msg',
      principal_type: 'user',
      page: 2,
      size: 20,
    })
  })

  it('应用筛选 / 重置 / 翻页写入路由查询参数', async () => {
    const page = await mountPage()
    await page.find('[data-test="acl-namespace"]').setValue('msg')
    await page.find('[data-test="acl-topic"]').setValue('order.created')
    await page.find('[data-test="acl-query"]').trigger('click')
    await settle()
    expect(scopeGet).toHaveBeenLastCalledWith(
      '/admin/acls',
      expect.objectContaining({ namespace: 'msg', topic: 'order.created', page: 1, size: 20 }),
    )
    expect(replace).toHaveBeenLastCalledWith({
      path: '/msg/acls',
      query: { namespace: 'msg', topic: 'order.created' },
    })

    const view = page.findComponent(ReadOnlyList)
    view.vm.$emit('update:page', 2)
    await settle()
    expect(replace).toHaveBeenLastCalledWith({
      path: '/msg/acls',
      query: { namespace: 'msg', topic: 'order.created', page: '2' },
    })

    await page.find('[data-test="acl-reset"]').trigger('click')
    await settle()
    expect(replace).toHaveBeenLastCalledWith({ path: '/msg/acls', query: {} })
  })

  it('非法页码 / 页长回落；全量筛选与页长写回 URL；打开新建抽屉', async () => {
    routeState.value = { path: '/msg/acls', params: {}, query: { page: '0', size: 'abc' } }
    const page = await mountPage()
    expect(scopeGet).toHaveBeenCalledWith('/admin/acls', { page: 1, size: 20 })

    await page.find('[data-test="acl-namespace"]').setValue('msg')
    await page.find('[data-test="acl-topic"]').setValue('order.created')
    selectByTest(page, 'acl-principal-type')?.vm.$emit('update:modelValue', 'role')
    await page.find('[data-test="acl-principal-key"]').setValue('11')
    selectByTest(page, 'acl-action')?.vm.$emit('update:modelValue', 'subscribe')
    selectByTest(page, 'acl-status')?.vm.$emit('update:modelValue', 'enabled')
    await page.find('[data-test="acl-query"]').trigger('click')
    await settle()
    expect(replace).toHaveBeenLastCalledWith({
      path: '/msg/acls',
      query: {
        namespace: 'msg',
        topic: 'order.created',
        principal_type: 'role',
        principal_key: '11',
        action: 'subscribe',
        status: 'enabled',
      },
    })

    await page.find('[data-test="acl-grant"]').trigger('click')
    await settle()
    const drawer = page.findComponent(AclGrantDrawer)
    expect(drawer.props('mode')).toBe('create')
    expect(drawer.props('prefill')).toBeNull()
  })

  it('页长非缺省时也写回 URL', async () => {
    routeState.value = { path: '/msg/acls', params: {}, query: { size: '10' } }
    const page = await mountPage()
    await page.find('[data-test="acl-query"]').trigger('click')
    await settle()
    expect(replace).toHaveBeenLastCalledWith({ path: '/msg/acls', query: { size: '10' } })
  })

  it('空态 / 错误态 / 无权限态', async () => {
    scopeGet.mockResolvedValueOnce({ code: 0, data: { items: [], page: 1, size: 20, total: 0 } })
    let page = await mountPage()
    expect(page.find('[data-test="readonly-empty"]').exists()).toBe(true)
    page.unmount()
    document.body.innerHTML = ''

    scopeGet.mockRejectedValueOnce({ code: 350001 })
    page = await mountPage()
    expect(page.find('[data-test="readonly-error"]').exists()).toBe(true)
    expect(page.text()).toContain('主题不存在')
    page.unmount()
    document.body.innerHTML = ''

    permissionRef.value = ['msg:query']
    page = await mountPage()
    expect(page.find('[data-test="readonly-denied"]').exists()).toBe(true)
    expect(page.text()).toContain('无「msg:manage」管理权限')
  })

  it('名称解析失败：显示标识 + 名称暂不可用，不阻断列表', async () => {
    platformGet.mockRejectedValue({ code: 10007 })
    const page = await mountPage()
    expect(page.findAll('[data-test="acl-principal-unavailable"]')).toHaveLength(3)
    expect(page.text()).toContain('7001')
  })
})

describe('主题授权页：收回与编辑入口（Kiwi 2470）', () => {
  it('收回：二次确认 → DELETE → 反馈并刷新', async () => {
    const page = await mountPage()
    await page.find('[data-test="acl-revoke"]').trigger('click')
    await settle()
    expect(q('[data-test="confirm-warning"]')?.textContent).toContain('立即失去')
    q('[data-test="confirm-ok"]')?.click()
    await settle()
    expect(scopeDel).toHaveBeenCalledWith('/admin/acls/9001', {})
    expect(page.find('[data-test="acl-feedback"]').text()).toContain('已收回授权')
  })

  it('收回名称降级：主体名称不可用时用占位文案', async () => {
    platformGet.mockRejectedValue({ code: 350027 })
    const page = await mountPage()
    await page.find('[data-test="acl-revoke"]').trigger('click')
    await settle()
    expect(q('[data-test="confirm-impact"]')?.textContent).toContain('名称暂不可用（7001）')
  })

  it('收回取消：弹窗双向绑定关闭', async () => {
    const page = await mountPage()
    await page.find('[data-test="acl-revoke"]').trigger('click')
    await settle()
    q('[data-test="confirm-cancel"]')?.click()
    await settle()
    expect(page.findComponent(MsgConfirmDialog).props('modelValue')).toBe(false)
  })

  it('收回无变化：提示无变化', async () => {
    scopeDel.mockResolvedValueOnce({ code: 0, data: { acl_id: '9001', changed: false } })
    const page = await mountPage()
    await page.find('[data-test="acl-revoke"]').trigger('click')
    await settle()
    q('[data-test="confirm-ok"]')?.click()
    await settle()
    expect(page.find('[data-test="acl-feedback"]').text()).toContain('无变化')
  })

  it('更新 / 重新授予：打开预填抽屉；停止态提示恢复启用', async () => {
    const page = await mountPage()
    await page.find('[data-test="acl-update"]').trigger('click')
    await settle()
    let drawer = page.findComponent(AclGrantDrawer)
    expect(drawer.props('mode')).toBe('update')
    expect(drawer.props('prefill')).toMatchObject({ principalKey: '7001', effect: 'allow' })
    drawer.vm.$emit('update:modelValue', false)
    await settle()

    await page.find('[data-test="acl-regrant"]').trigger('click')
    await settle()
    drawer = page.findComponent(AclGrantDrawer)
    expect(drawer.props('mode')).toBe('regrant')
    expect(q('[data-test="acl-grant-regrant"]')).not.toBeNull()
  })

  it('授予成功反馈：由抽屉 saved 事件驱动', async () => {
    const page = await mountPage()
    page.findComponent(AclGrantDrawer).vm.$emit('saved', { changed: 1 })
    await settle()
    expect(page.find('[data-test="acl-feedback"]').text()).toContain('授权已保存')

    page.findComponent(AclGrantDrawer).vm.$emit('saved', { changed: 0 })
    await settle()
    expect(page.find('[data-test="acl-feedback"]').text()).toContain('无变化')
  })

  it('收回失败：350027 用页面级文案，其他错误按错误码', async () => {
    scopeDel.mockRejectedValueOnce({ code: 350027 })
    let page = await mountPage()
    await page.find('[data-test="acl-revoke"]').trigger('click')
    await settle()
    q('[data-test="confirm-ok"]')?.click()
    await settle()
    expect(page.find('[data-test="acl-revoke-error"]').text()).toContain('无目标主题的管理权限')
    page.unmount()
    document.body.innerHTML = ''

    scopeDel.mockRejectedValueOnce({ code: 350001 })
    page = await mountPage()
    await page.find('[data-test="acl-revoke"]').trigger('click')
    await settle()
    q('[data-test="confirm-ok"]')?.click()
    await settle()
    expect(page.find('[data-test="acl-revoke-error"]').text()).toContain('主题不存在')
  })

  it('未知效果 / 状态值：回落通用文案，不抛错', async () => {
    scopeGet.mockResolvedValueOnce({
      code: 0,
      data: {
        items: [{ ...ITEMS[0], effect: 'other', status: 'other', principal_key: '7003' }],
        page: 1,
        size: 20,
        total: 1,
      },
    })
    const page = await mountPage()
    expect(page.findAll('[data-test="acl-effect-tag"]')[0]?.text()).toBe('效果')
    expect(page.findAll('[data-test="acl-status-tag"]')[0]?.text()).toBe('状态')
  })
})
