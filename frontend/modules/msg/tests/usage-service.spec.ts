// kiwi_id: 2471
/** 租户用量取数层：路径 / 列序映射 / 空数据兜底 / 契约违背 / 脚注键。 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet } = vi.hoisted(() => ({ scopeGet: vi.fn() }))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return { ...actual, requiredHostApi: () => ({ product: () => wrapScope({ get: scopeGet }) }) }
})

import {
  captionKeyOf,
  fetchTenantUsage,
  loadUsageCards,
  toCardRows,
  USAGE_CAPTION_KEYS,
  type TenantUsageView,
} from '../src/services/usage-service'

/** 契约样例（四列元数据 + 一行值）。 */
const VIEW: TenantUsageView = {
  columns: [
    { key: 'topic_count', label: '主题数' },
    { key: 'partition_count', label: '分区数' },
    { key: 'storage_bytes', label: '消息存储' },
    { key: 'delivered_today', label: '今日投递量' },
  ],
  items: [{ topic_count: 24, partition_count: 96, storage_bytes: 3650722201, delivered_today: 182430 }],
  total: 1,
}

describe('usage-service（Kiwi 2471）', () => {
  beforeEach(() => {
    scopeGet.mockReset()
  })

  it('取数走管理面同源端点并原样返回视图', async () => {
    scopeGet.mockResolvedValue(VIEW)
    const view = await fetchTenantUsage()
    expect(scopeGet).toHaveBeenCalledWith('/admin/tenant-usage', {})
    expect(view).toEqual(VIEW)
  })

  it('响应 data 为空按契约违背处理（10000）', async () => {
    scopeGet.mockResolvedValue(null)
    await expect(fetchTenantUsage()).rejects.toMatchObject({ code: 10000 })
  })

  it('按列顺序映射四行，取值不做任何重算', () => {
    const rows = toCardRows(VIEW)
    expect(rows.map((row) => row.key)).toEqual([
      'topic_count',
      'partition_count',
      'storage_bytes',
      'delivered_today',
    ])
    expect(rows.map((row) => row.label)).toEqual(['主题数', '分区数', '消息存储', '今日投递量'])
    expect(rows.map((row) => row.value)).toEqual([24, 96, 3650722201, 182430])
  })

  it('items 为空：四列均为 null（卡片各显示暂不可用，不报错）', () => {
    const rows = toCardRows({ columns: VIEW.columns, items: [], total: 0 })
    expect(rows).toHaveLength(4)
    expect(rows.every((row) => row.value === null)).toBe(true)
  })

  it('列元数据缺失按契约违背处理（10000）', () => {
    expect(() => toCardRows({ columns: [], items: [], total: 0 })).toThrowError(
      expect.objectContaining({ code: 10000 }),
    )
    expect(() => toCardRows({ total: 0 })).toThrowError(expect.objectContaining({ code: 10000 }))
  })

  it('items 字段缺失：四列均为 null（字段可选，缺省即不可用）', () => {
    const rows = toCardRows({ columns: VIEW.columns, total: 0 })
    expect(rows).toHaveLength(4)
    expect(rows.every((row) => row.value === null)).toBe(true)
  })

  it('loader 返回卡片行与总数（本页无分页）', async () => {
    scopeGet.mockResolvedValue(VIEW)
    const page = await loadUsageCards()
    expect(page.total).toBe(4)
    expect(page.items).toHaveLength(4)
  })

  it('脚注文案键：四列各有键，未知列给空串', () => {
    expect(captionKeyOf('topic_count')).toBe('msg.usage.caption.topicCount')
    expect(captionKeyOf('delivered_today')).toBe('msg.usage.caption.deliveredToday')
    expect(captionKeyOf('unknown_column')).toBe('')
    expect(Object.keys(USAGE_CAPTION_KEYS)).toEqual([
      'topic_count',
      'partition_count',
      'storage_bytes',
      'delivered_today',
    ])
  })
})
