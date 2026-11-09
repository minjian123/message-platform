// kiwi_id: 2471
/** 租户用量四卡：标题取列名 / 千分位 / 字节自适应单位 / 布局 / 加载与单列不可用。 */

import ElementPlus from 'element-plus'
import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'

import UsageCards from '../src/components/UsageCards.vue'
import { message } from '../src/i18n/messages'
import type { UsageCardRow } from '../src/services/usage-service'

/** 四列卡片行（与端点列序一致）。 */
const ROWS: UsageCardRow[] = [
  { key: 'topic_count', label: '主题数', value: 1280 },
  { key: 'partition_count', label: '分区数', value: 15360 },
  { key: 'storage_bytes', label: '消息存储', value: 3650722201 },
  { key: 'delivered_today', label: '今日投递量', value: 182430 },
]

/**
 * 挂载四卡。
 *
 * @param props 组件入参。
 */
function mountCards(props: Record<string, unknown>) {
  return mount(UsageCards, { props: { rows: ROWS, state: 'ready', ...props }, global: { plugins: [ElementPlus] } })
}

describe('UsageCards（Kiwi 2471）', () => {
  it('就绪：标题取列名，计数千分位，字节拆成自适应单位', () => {
    const wrapper = mountCards({})
    const text = wrapper.text()
    expect(text).toContain('主题数')
    expect(text).toContain('分区数')
    expect(text).toContain('消息存储')
    expect(text).toContain('今日投递量')
    expect(text).toContain('1,280')
    expect(text).toContain('15,360')
    expect(text).toContain('182,430')
    expect(text).toContain('3.4')
    expect(text).toContain('GiB')
    expect(text).toContain(message('msg.usage.caption.topicCount'))
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(4)
  })

  it('布局：独立页四列、页签 2×2', () => {
    const page = mountCards({ layout: 4 })
    expect(page.find('[data-test="usage-cards"]').attributes('data-layout')).toBe('4')
    expect(page.find('[data-test="usage-cards"]').classes()).toContain('usage-cards--4')
    const panel = mountCards({ layout: 2 })
    expect(panel.find('[data-test="usage-cards"]').attributes('data-layout')).toBe('2')
    expect(panel.find('[data-test="usage-cards"]').classes()).toContain('usage-cards--2')
  })

  it('首次加载（无数据）：四块骨架，不出空卡', () => {
    const wrapper = mountCards({ rows: [], state: 'loading' })
    expect(wrapper.findAll('[data-test="usage-skeleton"]')).toHaveLength(4)
    expect(wrapper.findAll('[data-test^="usage-card-"]')).toHaveLength(0)
  })

  it('刷新中（已有数据）：保留旧值，不换成加载文案', () => {
    const wrapper = mountCards({ state: 'loading' })
    expect(wrapper.findAll('[data-test="usage-skeleton"]')).toHaveLength(0)
    expect(wrapper.text()).toContain('1,280')
  })

  it('单列不可用：该卡保留标题与脚注并显示「暂不可用」，其余三卡照常', () => {
    const rows = ROWS.map((row) => (row.key === 'storage_bytes' ? { ...row, value: null } : row))
    const wrapper = mountCards({ rows })
    const degraded = wrapper.find('[data-test="usage-card-storage_bytes"]')
    expect(degraded.text()).toContain('消息存储')
    expect(degraded.text()).toContain(message('msg.quota.state.unavailable'))
    expect(degraded.text()).toContain(message('msg.usage.caption.storage'))
    expect(wrapper.find('[data-test="usage-card-head"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('1,280')
    expect(wrapper.text()).not.toContain('GiB')
  })

  it('未知列：无脚注文案（不显示键名）', () => {
    const wrapper = mountCards({ rows: [{ key: 'extra_metric', label: '新增列', value: 7 }] })
    expect(wrapper.text()).toContain('新增列')
    expect(wrapper.text()).not.toContain('extra_metric')
    expect(wrapper.text()).not.toContain(message('msg.usage.caption.topicCount'))
  })
})
