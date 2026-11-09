// kiwi_id: 2470
/**
 * 主题授权页实现快照：把页面在组件测试环境中的真实 DOM 输出为静态 HTML，
 * 供无头浏览器截图与原型 09 逐页对照（截图证据来源，不参与源码覆盖率口径）。
 */

import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet, platformGet, permissionRef, routeState, replace, push } = vi.hoisted(() => ({
  scopeGet: vi.fn(),
  platformGet: vi.fn(),
  permissionRef: { value: ['msg:manage'] as readonly string[] | undefined },
  routeState: { value: { path: '/msg/acls', params: {} as Record<string, string>, query: {} as Record<string, string> } },
  replace: vi.fn(),
  push: vi.fn(),
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => wrapScope({ get: scopeGet }), get: platformGet }),
    hostPermissions: () => permissionRef.value,
  }
})

vi.mock('vue-router', () => ({
  useRoute: () => routeState.value,
  useRouter: () => ({ push, replace }),
}))

import AclPage from '../src/views/AclPage.vue'
import AclPrincipalSelect from '../src/components/AclPrincipalSelect.vue'
import AclTopicSelect from '../src/components/AclTopicSelect.vue'

const OUT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../../msg文档/项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_07_主题授权管理界面/测试/资源/01_测试记录_06-7/实现快照',
)

const ACL_ITEMS = [
  {
    acl_id: '9001',
    namespace: 'msg',
    topic: 'order.created',
    principal_type: 'user',
    principal_key: '7001',
    action: 'publish',
    effect: 'allow',
    status: 'enabled',
    updated_at: '2026-11-09T10:20:31Z',
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
    updated_at: '2026-11-09T09:58:02Z',
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
    updated_at: '2026-11-08T18:04:55Z',
  },
]

/** 列表成功返回。 */
const ACL_PAGE = { code: 0, data: { items: ACL_ITEMS, page: 1, size: 20, total: 3 } }

/** 平台目录（用户 / 角色）批量解析返回。 */
const USER_LIST = {
  list: [
    { id: '7001', username: 'zhangsan', name: '张三', status: 'enabled', last_login_at: null },
    { id: '7002', username: 'lisi', name: '李四', status: 'enabled', last_login_at: null },
  ],
  total: 2,
  page: 1,
  size: 2,
}

const ROLE_LIST = {
  list: [
    {
      id: '11',
      code: 'order-sync',
      name: '订单同步角色',
      status: 'enabled',
      role_type: 'custom',
      builtin: false,
      subject_count: 1,
    },
  ],
  total: 1,
  page: 1,
  size: 1,
}

/**
 * 把实现 DOM 包成可截图的静态页。
 *
 * 样式由基座原型资源的 Element Plus 真样式提供；路径相对本文件生成位置
 * （`msg/msg文档/…/实现快照/` 上溯 10 级到 `msg/`，再走 `bms文档` 软链）。
 *
 * @param title 页面标题。
 * @param body 页面 DOM。
 * @returns 静态页 HTML。
 */
function page(title: string, body: string): string {
  return `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8" />
<title>实现渲染快照 · ${title}</title>
<link rel="stylesheet" href="../../../../../../../../../../bms文档/设计/原型设计/资源/vendor/element-plus.css" />
<style>
body{font-family:"Microsoft YaHei",Arial,sans-serif;margin:0;padding:24px;background:#f7f8fa;color:#303133}
h1{font-size:20px;margin:0 0 16px}
.frame{background:#fff;border:1px solid #dcdfe6;border-radius:6px;padding:18px}
h2{font-size:15px;margin:12px 0}
.hint{color:#909399;font-size:12px}
.admin-page__filters{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.acl-principal-cell{display:flex;flex-direction:column;gap:1px}
.acl-principal-cell__missing{color:#e6a23c;font-size:11px}
.acl-principal-cell__key{color:#909399;font-family:Consolas,Monaco,monospace;font-size:11px}
</style></head><body>
<h1>实现渲染快照 · ${title}</h1>
<div class="frame">${body}</div>
</body></html>`
}

/** 等待多层异步取数结算。 */
async function settle(): Promise<void> {
  await flushPromises()
  await flushPromises()
  await flushPromises()
  await flushPromises()
}

/** 默认平台目录返回（用户 / 角色各一次批量请求）。 */
function mockDirectory(): void {
  platformGet.mockImplementation(async (_service: string, path: string) => (path === '/users' ? USER_LIST : ROLE_LIST))
}

/** 页面外层 DOM + 传送门里的浮层（抽屉 / 对话框），保证快照含弹窗。 */
function withOverlays(wrapper: VueWrapper): string {
  const overlays = Array.from(document.querySelectorAll('.el-overlay'))
    .map((node) => node.outerHTML)
    .join('')
  return `${wrapper.html()}${overlays}`
}

/**
 * 挂载页面、按需打开浮层并导出快照。
 *
 * @param name 快照文件名（不含扩展名）。
 * @param title 快照标题。
 * @param prepare 打开浮层等准备动作。
 */
async function exportSnapshot(name: string, title: string, prepare?: (wrapper: VueWrapper) => Promise<void>): Promise<void> {
  const wrapper = mount(AclPage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
  await settle()
  if (prepare !== undefined) {
    await prepare(wrapper)
    await settle()
  }
  const html = page(title, withOverlays(wrapper))
  writeFileSync(resolve(OUT, `${name}.html`), html, 'utf8')
  expect(html.length).toBeGreaterThan(100)
  expect(wrapper.text()).toContain('主题授权')
  wrapper.unmount()
}

/**
 * 填充抽屉里的规则行（直接向子组件回传值，免去逐级打开下拉）。
 *
 * @param wrapper 页面包装器。
 * @param rows 目标行内容。
 */
function fillRows(
  wrapper: VueWrapper,
  rows: Array<{ topic: string; key: string; label: string }>,
): void {
  const topics = wrapper.findAllComponents(AclTopicSelect)
  const principals = wrapper.findAllComponents(AclPrincipalSelect)
  rows.forEach((row, index) => {
    topics[index]?.vm.$emit('update:modelValue', row.topic)
    principals[index]?.vm.$emit('update:modelValue', row.key)
    principals[index]?.vm.$emit('update:label', row.label)
  })
}

beforeEach(() => {
  scopeGet.mockReset()
  platformGet.mockReset()
  permissionRef.value = ['msg:manage', 'user:query', 'role:query']
  routeState.value.query = {}
})

describe('主题授权页实现快照（Kiwi 2470）', () => {
  beforeAll(() => {
    mkdirSync(OUT, { recursive: true })
  })

  it('导出列表真实 DOM 快照供原型对照', async () => {
    scopeGet.mockResolvedValue(ACL_PAGE)
    mockDirectory()
    await exportSnapshot('09_主题授权', '主题授权')
  })

  it('导出更新抽屉真实 DOM 快照', async () => {
    scopeGet.mockResolvedValue(ACL_PAGE)
    mockDirectory()
    await exportSnapshot('09b_主题授权_更新抽屉', '主题授权 · 更新抽屉', async (wrapper) => {
      await wrapper.find('[data-test="acl-update"]').trigger('click')
    })
  })

  it('导出收回确认真实 DOM 快照', async () => {
    scopeGet.mockResolvedValue(ACL_PAGE)
    mockDirectory()
    await exportSnapshot('09c_主题授权_收回确认', '主题授权 · 收回确认', async (wrapper) => {
      await wrapper.find('[data-test="acl-revoke"]').trigger('click')
    })
  })

  it('导出单条授予真实 DOM 快照', async () => {
    scopeGet.mockResolvedValue(ACL_PAGE)
    platformGet.mockResolvedValue({ items: [], page: 1, size: 20, total: 0 })
    mockDirectory()
    await exportSnapshot('09g_主题授权_单条授予', '主题授权 · 单条授予', async (wrapper) => {
      await wrapper.find('[data-test="acl-grant"]').trigger('click')
      await settle()
      fillRows(wrapper, [{ topic: 'order.created', key: '7001', label: '张三' }])
    })
  })

  it('导出批量规则真实 DOM 快照', async () => {
    scopeGet.mockResolvedValue(ACL_PAGE)
    platformGet.mockResolvedValue({ items: [], page: 1, size: 20, total: 0 })
    mockDirectory()
    await exportSnapshot('09h_主题授权_批量规则', '主题授权 · 批量规则', async (wrapper) => {
      await wrapper.find('[data-test="acl-grant"]').trigger('click')
      await settle()
      await wrapper.find('[data-test="acl-grant-add"]').trigger('click')
      await wrapper.find('[data-test="acl-grant-add"]').trigger('click')
      await settle()
      fillRows(wrapper, [
        { topic: 'order.created', key: '7001', label: '张三' },
        { topic: 'order.created', key: '7002', label: '李四' },
        { topic: 'order.paid', key: '11', label: '订单同步角色' },
      ])
    })
  })

  it('导出名称降级真实 DOM 快照（平台目录失败）', async () => {
    scopeGet.mockResolvedValue(ACL_PAGE)
    platformGet.mockRejectedValue({ code: 350027 })
    await exportSnapshot('09d_主题授权_名称降级', '主题授权 · 名称降级')
  })

  it('导出无权限真实 DOM 快照', async () => {
    permissionRef.value = ['msg:read']
    scopeGet.mockResolvedValue(ACL_PAGE)
    await exportSnapshot('09e_主题授权_无权限', '主题授权 · 无权限')
  })

  it('导出错误态真实 DOM 快照', async () => {
    scopeGet.mockRejectedValue({ code: 350027 })
    await exportSnapshot('09f_主题授权_错误态', '主题授权 · 错误态')
  })
})
