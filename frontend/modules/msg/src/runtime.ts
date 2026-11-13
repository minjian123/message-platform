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

/** 宿主当前语言（缺省 `zh-cn`；只保留中 / 英两档，与模块文案包一致）。 */
let hostLocaleTag = 'zh-cn'

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
 * 归一化语言标识（只保留中 / 英两档，与模块文案包 `msg:zh-cn` / `msg:en` 一致）。
 *
 * @param tag 语言标识（可能带地区，如 `en-US`）。
 * @returns `en` 或 `zh-cn`。
 */
export function normalizeLocale(tag: string | undefined): string {
  if (tag !== undefined && tag.trim().toLowerCase().startsWith('en')) {
    return 'en'
  }
  return 'zh-cn'
}

/**
 * 从宿主上下文解析当前语言。
 *
 * 取用顺序：`i18n.defaultLocale` → `i18n.locale` → `i18n.locales()` 首个 → `user.locale`；
 * 都取不到返回 `undefined`（调用方回落 `zh-cn`，**不读浏览器语言**——产品语言以宿主注入为准）。
 *
 * @param context 宿主注入上下文。
 * @returns 归一化语言标识；无法判定返回 `undefined`。
 */
function readLocaleTag(context: ModuleHostContext): string | undefined {
  const i18n = context.i18n as Record<string, unknown> | undefined
  if (typeof i18n === 'object' && i18n !== null) {
    const direct = i18n['defaultLocale'] ?? i18n['locale']
    if (typeof direct === 'string' && direct !== '') {
      return normalizeLocale(direct)
    }
    const locales = i18n['locales']
    if (typeof locales === 'function') {
      const values = (locales as () => unknown)()
      if (Array.isArray(values) && typeof values[0] === 'string') {
        return normalizeLocale(values[0])
      }
    }
  }
  const user = context.user as Record<string, unknown> | undefined
  if (typeof user === 'object' && user !== null && typeof user['locale'] === 'string') {
    return normalizeLocale(user['locale'])
  }
  return undefined
}

/**
 * 宿主当前语言（模块文案与 `message()` 的缺省语言）。
 *
 * @returns `en` 或 `zh-cn`。
 */
export function hostLocale(): string {
  return hostLocaleTag
}

/**
 * 缓存宿主注入的请求能力、用户权限快照与语言。
 *
 * @param context 宿主注入上下文（只读快照）。
 */
export function applyHostContext(context: ModuleHostContext): void {
  hostApi = context.api
  hostPermissionCodes = readPermissionCodes(context.user)
  hostLocaleTag = readLocaleTag(context) ?? 'zh-cn'
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
