/**
 * 模块运行期宿主能力持有（只读消费宿主注入上下文）。
 *
 * 配额插件只读：取数经宿主请求层（`api.product('msg', 'msg')`），不自建 HTTP、不自拼网关前缀。
 * 上下文缺失即降级为「暂不可用」，不影响宿主页。
 */

import type { ModuleApi, ModuleHostContext } from '@bms/core'

/** 配额查询权限码（与后端 `msg:query` 同源）。 */
export const MSG_QUERY_PERMISSION = 'msg:query'

/** 宿主请求能力缺失时的哨兵错误。 */
export const MODULE_API_ABSENT = 'MODULE_API_ABSENT'

/** 宿主注入的请求能力（未注入为 `undefined`）。 */
let hostApi: ModuleApi | undefined

/**
 * 缓存宿主注入的请求能力。
 *
 * @param context 宿主注入上下文（只读快照）。
 */
export function applyHostContext(context: ModuleHostContext): void {
  hostApi = context.api
}

/**
 * 取宿主请求能力。
 *
 * @returns 请求能力。
 * @throws Error 哨兵错误（`MODULE_API_ABSENT`）。
 */
export function requiredHostApi(): ModuleApi {
  if (hostApi === undefined) {
    throw new Error(MODULE_API_ABSENT)
  }
  return hostApi
}

/**
 * 是否请求能力缺失（哨兵判定）。
 *
 * @param error 捕获的错误。
 * @returns 是否缺失。
 */
export function isApiAbsent(error: unknown): boolean {
  return error instanceof Error && error.message === MODULE_API_ABSENT
}
