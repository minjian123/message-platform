/**
 * 租户用量取数（06-8）：同源管理面端点 → 四张卡片行。
 *
 * 端点 `GET /api/v1/msg/admin/tenant-usage` 与 05-1 平台投影端点共用后端同一取数装配；
 * 本层只做「列键 → 行值」取值与列序保持，**不做任何统计计算**。
 */

import type { msg } from '@msg/api-types'

import { MsgQueryError, getData, requiredData } from './product-api'

type Schema = msg.components['schemas']

/** 一列的元数据（取生成物，不手写契约类型）。 */
export type UsageColumn = Schema['UsageColumnView']
/** 用量行（四列值）。 */
export type TenantUsageRow = Schema['TenantUsageRow']
/** 用量视图。 */
export type TenantUsageView = Schema['TenantUsageView']

/** 页面渲染单元：一列 → 一张卡。 */
export interface UsageCardRow {
  /** 列键（端点 `columns[].key`）。 */
  key: string
  /** 展示名（端点 `columns[].label`，不在前端硬编码）。 */
  label: string
  /** 列值（`null` 表示该列当前不可用）。 */
  value: number | null
}

/** 列键 → 卡片脚注文案键（卡片上的一句话口径）。 */
export const USAGE_CAPTION_KEYS: Record<string, string> = {
  topic_count: 'msg.usage.caption.topicCount',
  partition_count: 'msg.usage.caption.partitionCount',
  storage_bytes: 'msg.usage.caption.storage',
  delivered_today: 'msg.usage.caption.deliveredToday',
}

/**
 * 取列键对应的脚注文案键（未知列给空串，不显示脚注）。
 *
 * @param key 列键。
 * @returns 文案键。
 */
export function captionKeyOf(key: string): string {
  return USAGE_CAPTION_KEYS[key] ?? ''
}

/**
 * 取当前租户用量视图。
 *
 * @returns 用量视图（`data` 为空视为契约违背）。
 * @throws MsgQueryError 响应 `data` 为空（10000）。
 */
export async function fetchTenantUsage(): Promise<TenantUsageView> {
  const view = await getData<TenantUsageView>('/admin/tenant-usage')
  return requiredData(view)
}

/**
 * 把用量视图映射成四张卡片行（按 `columns` 顺序，不重排、不补列）。
 *
 * @param view 用量视图。
 * @returns 卡片行（顺序即端点列序）。
 * @throws MsgQueryError 列元数据缺失（10000，契约违背：端点必带四列）。
 */
export function toCardRows(view: TenantUsageView): UsageCardRow[] {
  const columns: UsageColumn[] = view.columns ?? []
  if (columns.length === 0) {
    throw new MsgQueryError(10000)
  }
  const row = (view.items ?? [])[0] as Record<string, number | null | undefined> | undefined
  return columns.map((column) => ({
    key: column.key,
    label: column.label,
    value: row?.[column.key] ?? null,
  }))
}

/**
 * 只读视图取数函数（`BaseMsgReadOnlyView` 的 loader；`total` 取卡片数，本页无分页）。
 *
 * @returns 卡片行与总数。
 */
export async function loadUsageCards(): Promise<{ items: readonly UsageCardRow[]; total: number }> {
  const rows = toCardRows(await fetchTenantUsage())
  return { items: rows, total: rows.length }
}
