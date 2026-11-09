/** 配额数据服务（产品域寻址，请求 / 响应契约类型取自 `@msg/api-types`）。 */

import type { msg } from '@msg/api-types'

import { getData, putData } from './product-api'

type Schema = msg.components['schemas']

/**
 * 查询当前租户六维配额。
 *
 * @param namespace 作用域（空串＝租户级）。
 * @returns 配额视图。
 */
export async function fetchQuotas(namespace = ''): Promise<Schema['QuotaView']> {
  const view = await getData<Schema['QuotaView']>('/admin/quotas', namespace === '' ? undefined : { namespace })
  return view ?? { items: [] }
}

/**
 * 批量调整配额（整批校验、整批失败；保存后即时生效）。
 *
 * @param items 调整项（`limit` 为 0 表示不限，`soft_limit` 为 null 表示按硬限派生）。
 * @returns 调整后的配额视图。
 */
export async function updateQuotas(items: readonly Schema['QuotaUpdateItem'][]): Promise<Schema['QuotaView']> {
  const body: Schema['QuotaUpdateRequest'] = { items: [...items] }
  const view = await putData<Schema['QuotaView']>('/admin/quotas', body)
  return view ?? { items: [] }
}
