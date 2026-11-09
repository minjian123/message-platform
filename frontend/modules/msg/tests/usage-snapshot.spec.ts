// kiwi_id: 2471
/**
 * 租户用量实现快照：把独立页与租户记录页页签在组件测试环境中的真实 DOM 输出为静态 HTML，
 * 供无头浏览器截图与原型 10 逐场景对照（截图证据来源，不参与源码覆盖率口径）。
 */

import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { provideModuleSlotContext } from '@bms/ui_ep'
import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { defineComponent, h } from 'vue'
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

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

vi.mock('vue-router', () => ({
  useRoute: () => ({ path: '/tenant/records/1', params: {}, query: {} }),
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
}))

import UsagePanel from '../src/components/UsagePanel.vue'
import UsagePage from '../src/views/UsagePage.vue'

const OUT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../../msg文档/项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_08_租户用量视图/测试/资源/01_测试记录_06-8/实现快照',
)

/** 契约样例（四列 + 一行真实值）。 */
const VIEW = {
  columns: [
    { key: 'topic_count', label: '主题数' },
    { key: 'partition_count', label: '分区数' },
    { key: 'storage_bytes', label: '消息存储' },
    { key: 'delivered_today', label: '今日投递量' },
  ],
  items: [{ topic_count: 1280, partition_count: 15360, storage_bytes: 9610825277440, delivered_today: 1234567 }],
  total: 1,
}

/** 单列不可用样例（存储采集失败）。 */
const PARTIAL = { ...VIEW, items: [{ ...VIEW.items[0], storage_bytes: null }] }

/**
 * 把实现 DOM 包成可截图的静态页。
 *
 * 样式由基座原型资源的 Element Plus 真样式提供；路径相对本文件生成位置
 * （`msg/msg文档/…/实现快照/` 上溯 10 级到 `msg/`，再走 `bms文档` 软链）。
 *
 * @param title 快照标题。
 * @param body 页面 DOM。
 * @returns 静态页 HTML。
 */
function page(title: string, body: string): string {
  return `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8" />
<title>实现渲染快照 · ${title}</title>
<link rel="stylesheet" href="../../../../../../../../../../bms文档/设计/原型设计/资源/vendor/element-plus.css" />
<style>
:root{--bms-spacing-sm:8px;--bms-spacing-lg:16px;--bms-space-1:8px;--bms-space-2:12px;
--bms-color-border:#dcdfe6;--bms-color-bg:#fff;--bms-radius-md:6px;--bms-radius-sm:4px;
--bms-color-text:#303133;--bms-color-text-secondary:#909399;--bms-color-fill-light:#f5f7fa;
--bms-font-size:13px;--bms-color-warning:#e6a23c}
body{font-family:"Microsoft YaHei",Arial,sans-serif;margin:0;padding:24px;background:#f7f8fa;color:#303133}
h1{font-size:20px;margin:0 0 16px}
.frame{background:#fff;border:1px solid #dcdfe6;border-radius:6px;padding:18px}
h2{font-size:15px;margin:12px 0}
.hint{color:#909399;font-size:12px}
/* 以下为模块 scoped 样式在快照中的等价还原（快照不含构建期 data-v 属性） */
.usage-cards{display:grid;gap:var(--bms-space-2)}
.usage-cards--4{grid-template-columns:repeat(4,minmax(0,1fr))}
.usage-cards--2{grid-template-columns:repeat(2,minmax(0,1fr))}
.bms-metric-card{display:flex;flex-direction:column;gap:var(--bms-spacing-sm);padding:var(--bms-spacing-lg);
border:1px solid var(--bms-color-border);border-radius:var(--bms-radius-md);background:var(--bms-color-bg)}
.bms-metric-card__header{display:flex;align-items:center;justify-content:space-between}
.bms-metric-card__title{color:var(--bms-color-text-secondary);font-size:var(--bms-font-size)}
.bms-metric-card__refresh{display:none}
.bms-metric-card__value{display:flex;align-items:baseline;gap:var(--bms-spacing-sm)}
.bms-metric-card__number{font-size:24px;font-weight:600}
.bms-metric-card__unit{color:var(--bms-color-text-secondary);font-size:14px}
.bms-metric-card__caption{color:var(--bms-color-text-secondary);font-size:12px}
.bms-metric-card__placeholder{color:var(--bms-color-warning);font-size:14px}
.usage-card__head{color:var(--bms-color-text-secondary);font-size:13px}
.usage-card__degraded{color:var(--bms-color-warning);font-size:16px}
.usage-card__note{color:var(--bms-color-text-secondary);font-size:12px}
.usage-panel{display:flex;flex-direction:column;gap:var(--bms-space-2);min-height:160px}
.usage-panel__hint{display:flex;align-items:center;gap:4px;margin:0;color:var(--bms-color-text-secondary);font-size:12px}
.usage-panel__sep{opacity:.6}
.usage-panel__actions{display:flex;justify-content:flex-end}
.usage-note{margin-top:var(--bms-space-2);padding:var(--bms-space-2);border-radius:var(--bms-radius-sm);
background:var(--bms-color-fill-light)}
.usage-note__title{margin:0 0 var(--bms-space-1);font-size:13px;font-weight:600}
.usage-note__list{margin:0;padding-left:18px;color:var(--bms-color-text-secondary);font-size:12px;line-height:1.7}
.admin-page{display:flex;flex-direction:column;gap:var(--bms-space-2)}
.admin-page__head{display:flex;align-items:flex-start;justify-content:space-between;gap:var(--bms-space-2)}
.admin-page__hint{margin:0;color:var(--bms-color-text-secondary);font-size:12px}
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

/**
 * 挂载独立页并导出快照。
 *
 * @param name 快照文件名（不含扩展名）。
 * @param title 快照标题。
 */
async function exportPage(name: string, title: string): Promise<void> {
  const wrapper = mount(UsagePage, { global: { plugins: [ElementPlus] }, attachTo: document.body })
  await settle()
  const html = page(title, wrapper.html())
  writeFileSync(resolve(OUT, `${name}.html`), html, 'utf8')
  expect(html.length).toBeGreaterThan(100)
  wrapper.unmount()
}

/**
 * 挂载页签（以宿主槽上下文）并导出快照。
 *
 * @param name 快照文件名（不含扩展名）。
 * @param title 快照标题。
 * @param context 槽上下文。
 */
async function exportPanel(name: string, title: string, context: Record<string, unknown>): Promise<void> {
  const Host = defineComponent({
    setup() {
      provideModuleSlotContext(() => context)
      return () => h(UsagePanel)
    },
  })
  const wrapper: VueWrapper = mount(Host, { global: { plugins: [ElementPlus] }, attachTo: document.body })
  await settle()
  const html = page(title, wrapper.html())
  writeFileSync(resolve(OUT, `${name}.html`), html, 'utf8')
  expect(html.length).toBeGreaterThan(100)
  wrapper.unmount()
}

beforeEach(() => {
  scopeGet.mockReset()
  permissionRef.value = ['msg:query']
})

describe('租户用量实现快照（Kiwi 2471）', () => {
  beforeAll(() => {
    mkdirSync(OUT, { recursive: true })
  })

  it('导出租户记录页页签真实 DOM 快照（2×2 四卡 + 租户对照 + 跳转）', async () => {
    scopeGet.mockResolvedValue(VIEW)
    await exportPanel('10d_页签_租户记录页', '租户用量 · 消息用量页签', { formKey: 'tenant', recordId: '2026011500001' })
  })

  it('导出独立页真实 DOM 快照（就绪：四卡 + 口径说明）', async () => {
    scopeGet.mockResolvedValue(VIEW)
    await exportPage('10_独立页', '租户用量 · 独立页')
  })

  it('导出单列不可用快照（存储采集失败）', async () => {
    scopeGet.mockResolvedValue(PARTIAL)
    await exportPage('10b_独立页_单列不可用', '租户用量 · 单列不可用')
  })

  it('导出请求失败快照（整页降级 + 重试）', async () => {
    scopeGet.mockRejectedValue(new Error('network down'))
    await exportPage('10c_独立页_请求失败', '租户用量 · 请求失败')
  })

  it('导出无权限快照（提示条替代四卡）', async () => {
    permissionRef.value = ['msg:manage']
    await exportPage('10e_独立页_无权限', '租户用量 · 无权限')
  })
})
