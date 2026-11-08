/** 配额投影契约（与后端 `schemas/quota.py` 的 `QuotaView` 同形）。 */

/** 一条配额视图。 */
export interface QuotaItem {
  quota_key: string
  namespace: string
  limit: number | null
  soft_limit: number | null
  hard_limit: number | null
  unit: string
  period: string
  usage: number | null
  state: string
  source: string
  updated_at: string | null
}

/** 查询响应。 */
export interface QuotaView {
  items: QuotaItem[]
}

/** 平台统一响应包装（`ApiResponse`）。 */
export interface ApiEnvelope<T> {
  code: number
  message?: string | null
  data: T
}
