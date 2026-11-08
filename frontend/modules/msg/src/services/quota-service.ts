/** 配额数据服务（产品域寻址：`/api/msg/v1/msg/admin/quotas`）。 */

import type { ModuleApiScope } from '@bms/core'

import type { ApiEnvelope, QuotaView } from '../domain'
import { requiredHostApi } from '../runtime'

/** 产品键（与平台产品登记同源）。 */
export const MSG_PRODUCT = 'msg'
/** 产品命名空间段（对外 `/api/msg/v1/msg/...`）。 */
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
export async function fetchQuotas(namespace = ''): Promise<QuotaView> {
  const response = await scope().get<ApiEnvelope<QuotaView>>(
    '/admin/quotas',
    namespace === '' ? undefined : { namespace },
  )
  return response.data
}
