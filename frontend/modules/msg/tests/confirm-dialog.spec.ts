// kiwi_id: 2466
/** 二次确认弹窗：影响范围行 / 危险色 / 提交中禁用 / 事件 / 显隐。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { afterEach, describe, expect, it } from 'vitest'

import MsgConfirmDialog from '../src/components/MsgConfirmDialog.vue'

/** 影响范围样例。 */
const ROWS = [
  { label: '分区数', value: '2' },
  { label: '死信数', value: '1', tone: 'danger' as const },
]

let mounted: VueWrapper | undefined

/**
 * 挂载弹窗（内容经 teleport 渲染到 body，故断言走 `document`）。
 *
 * @param props 组件属性。
 * @returns 挂载结果。
 */
async function mountDialog(props: Record<string, unknown>): Promise<VueWrapper> {
  const wrapper = mount(MsgConfirmDialog, {
    props: { modelValue: true, title: '删除主题', ...props },
    global: { plugins: [ElementPlus] },
    attachTo: document.body,
  })
  await flushPromises()
  mounted = wrapper
  return wrapper
}

/**
 * 取 body 内某选择器的第一个元素。
 *
 * @param selector CSS 选择器。
 * @returns 元素（不存在返回 null）。
 */
function q(selector: string): HTMLElement | null {
  return document.body.querySelector<HTMLElement>(selector)
}

afterEach(() => {
  mounted?.unmount()
  mounted = undefined
  document.body.innerHTML = ''
})

describe('MsgConfirmDialog（Kiwi 2466）', () => {
  it('渲染影响范围行与危险值样式，标题与风险提示取传入文案', async () => {
    await mountDialog({ rows: ROWS, warning: '不可恢复' })
    expect(q('[data-test="confirm-warning"]')).not.toBeNull()
    expect(q('[data-test="confirm-impact"]')?.querySelectorAll('td').length).toBeGreaterThanOrEqual(4)
    expect(q('.confirm-danger')?.textContent).toBe('1')
    expect(q('.el-dialog__title')?.textContent).toBe('删除主题')
  })

  it('空影响范围：不渲染表格，仍可确认', async () => {
    const wrapper = await mountDialog({})
    expect(q('[data-test="confirm-impact"]')).toBeNull()
    q('[data-test="confirm-ok"]')?.click()
    await flushPromises()
    expect(wrapper.emitted('confirm')).toHaveLength(1)
  })

  it('loading：两个按钮都禁用，确认按钮不再触发事件', async () => {
    const wrapper = await mountDialog({ loading: true })
    const ok = q('[data-test="confirm-ok"]') as HTMLButtonElement
    const cancel = q('[data-test="confirm-cancel"]') as HTMLButtonElement
    expect(ok.disabled).toBe(true)
    expect(cancel.disabled).toBe(true)
    ok.click()
    await flushPromises()
    expect(wrapper.emitted('confirm')).toBeUndefined()
  })

  it('取消：发出 update:modelValue(false) 与 cancel', async () => {
    const wrapper = await mountDialog({})
    q('[data-test="confirm-cancel"]')?.click()
    await flushPromises()
    expect(wrapper.emitted('update:modelValue')?.[0]).toEqual([false])
    expect(wrapper.emitted('cancel')).toHaveLength(1)
  })

  it('自定义按钮文案与危险色', async () => {
    await mountDialog({ danger: true, confirmText: '确认重置', cancelText: '返回' })
    expect(q('[data-test="confirm-ok"]')?.textContent?.trim()).toBe('确认重置')
    expect(q('[data-test="confirm-cancel"]')?.textContent?.trim()).toBe('返回')
    expect(q('[data-test="confirm-ok"]')?.className).toContain('danger')
  })

  it('默认按钮文案取 i18n；关闭时同步显隐', async () => {
    const wrapper = await mountDialog({})
    expect(q('[data-test="confirm-ok"]')?.textContent?.trim()).toBe('确认')
    expect(q('[data-test="confirm-cancel"]')?.textContent?.trim()).toBe('取消')
    await wrapper.setProps({ modelValue: false })
    await flushPromises()
    expect(wrapper.emitted('update:modelValue')).toBeUndefined()
  })
})
