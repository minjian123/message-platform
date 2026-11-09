/**
 * 模块运行期宿主能力持有（只读消费宿主注入上下文）。
 *
 * 取数经宿主请求层（`api.product('msg', 'msg')`），不自建 HTTP、不自拼网关前缀。
 * 上下文缺失即降级为「暂不可用」，不影响宿主页；权限集合只作展示层判定，
 * 路由与菜单拦截由宿主守卫按 `meta.perm` 承担。
 */

import type { ModuleApi, ModuleHostContext } from '@bms/core'

/** 配额查询权限码（与后端 `msg:query` 同源）。 */
export const MSG_QUERY_PERMISSION = 'msg:query'

/** 写操作权限码（与后端 `msg:manage` 同源；写按钮按它渲染，不抬升页面权限）。 */
export const MSG_MANAGE_PERMISSION = 'msg:manage'

/** 宿主请求能力缺失时的哨兵错误。 */
export const MODULE_API_ABSENT = 'MODULE_API_ABSENT'

/** 宿主注入的请求能力（未注入为 `undefined`）。 */
let hostApi: ModuleApi | undefined

/** 宿主注入用户已持有的权限码（未注入为 `undefined`，与宿主守卫「权限未装载放行」同口径）。 */
let hostPermissionCodes: readonly string[] | undefined

/**
 * 从宿主注入的用户对象读取权限码集合。
 *
 * @param user 宿主注入的用户快照（形态未知，只取 `codes` / `permissions` 字符串数组）。
 * @returns 权限码集合；读不到返回 `undefined`。
 */
function readPermissionCodes(user: unknown): readonly string[] | undefined {
  if (typeof user !== 'object' || user === null) {
    return undefined
  }
  const snapshot = user as Record<string, unknown>
  const value = snapshot.codes ?? snapshot.permissions
  if (!Array.isArray(value)) {
    return undefined
  }
  return value.filter((item): item is string => typeof item === 'string')
}

/**
 * 缓存宿主注入的请求能力与用户权限快照。
 *
 * @param context 宿主注入上下文（只读快照）。
 */
export function applyHostContext(context: ModuleHostContext): void {
  hostApi = context.api
  hostPermissionCodes = readPermissionCodes(context.user)
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
 * 宿主用户已持有的权限码（未装载为 `undefined`）。
 *
 * @returns 权限码集合。
 */
export function hostPermissions(): readonly string[] | undefined {
  return hostPermissionCodes
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
