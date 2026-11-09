/** 配额数据服务（产品域寻址，请求 / 响应契约类型取自 `@msg/api-types`）。 */

import type { ModuleApiScope } from '@bms/core'
import type { msg } from '@msg/api-types'

import { requiredHostApi } from '../runtime'

type Schema = msg.components['schemas']

/** 产品键（与平台产品登记同源）。 */
export const MSG_PRODUCT = 'msg'
/** 产品命名空间段。 */
export const MSG_NAMESPACE = 'msg'

/**
 * 配额管理面作用域。
 *
 * @returns 产品域请求作用域。
 */
function scope(): ModuleApiScope {
  return requiredHostApi().product(MSG_PRODUCT, MSG_NAMESPACE)
}

/**
 * 查询当前租户六维配额。
 *
 * @param namespace 作用域（空串＝租户级）。
 * @returns 配额视图。
 */
export async function fetchQuotas(namespace = ''): Promise<Schema['QuotaView']> {
  const response = await scope().get<Schema['ApiResponse_QuotaView_']>(
    '/admin/quotas',
    namespace === '' ? undefined : { namespace },
  )
  return response.data ?? { items: [] }
}

/**
 * 批量调整配额（整批校验、整批失败；保存后即时生效）。
 *
 * @param items 调整项（`limit` 为 0 表示不限，`soft_limit` 为 null 表示按硬限派生）。
 * @returns 调整后的配额视图。
 * @throws Error 业务码非 0（由调用方按 `errorCodeOf` 取码映射文案）。
 */
export async function updateQuotas(items: readonly Schema['QuotaUpdateItem'][]): Promise<Schema['QuotaView']> {
  const body: Schema['QuotaUpdateRequest'] = { items: [...items] }
  const response = await scope().put<Schema['ApiResponse_QuotaView_']>('/admin/quotas', body)
  if (response.code !== 0) {
    throw new QuotaUpdateError(response.code)
  }
  return response.data ?? { items: [] }
}

/** 配额调整失败（携带后端业务码，供页面映射文案键）。 */
export class QuotaUpdateError extends Error {
  /** 后端业务码。 */
  readonly code: number

  /**
   * @param code 后端业务码。
   */
  constructor(code: number) {
    super(String(code))
    this.name = 'QuotaUpdateError'
    this.code = code
  }
}
