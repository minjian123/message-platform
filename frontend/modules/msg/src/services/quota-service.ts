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
