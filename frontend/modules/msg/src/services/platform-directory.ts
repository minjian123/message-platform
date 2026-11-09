/**
 * 平台用户 / 角色目录（**平台服务通道**：`api.get('platform', ...)`）。
 *
 * 仅用于主题授权的主体候选与名称回显：
 * - 用户 / 角色主数据归 bms 平台，msg 侧不保存目录、不写库；
 * - 名称按主键批量解析（`ids` 逗号分隔，单次 ≤200），避免逐行 N+1；
 * - 无 `user:query` / `role:query` 或接口失败时由页面降级手填，不阻断授权。
 */

import type { platform } from '@bms/api_types'

import { requiredHostApi } from '../runtime'
import { MsgQueryError, toParams } from './product-api'

type Schemas = platform.components['schemas']

/** 平台服务键。 */
export const PLATFORM_SERVICE = 'platform'
/** 用户查询权限码。 */
export const USER_QUERY_PERMISSION = 'user:query'
/** 角色查询权限码。 */
export const ROLE_QUERY_PERMISSION = 'role:query'
/** 单次按主键批量解析上限（与平台 `ids` 参数一致）。 */
export const MAX_PRINCIPAL_IDS = 200

/** 平台用户行（契约生成类型）。 */
export type PlatformUserItem = Schemas['UserItem']
/** 平台用户分页响应。 */
export type PlatformUserPage = Schemas['BasePageResponse_UserItem_']
/** 平台角色行（契约生成类型）。 */
export type PlatformRoleItem = Schemas['RoleItem']
/** 平台角色分页响应。 */
export type PlatformRolePage = Schemas['BasePageResponse_RoleItem_']

/**
 * 用户 / 角色搜索（关键字 + 分页）。
 *
 * @param keyword 关键字（账号 / 姓名，或角色码 / 名称）。
 * @param size 每页条数（缺省 20）。
 * @returns 分页响应。
 */
export function searchPlatformUsers(keyword: string, size = 20): Promise<PlatformUserPage> {
  return requiredHostApi().get<PlatformUserPage>(
    PLATFORM_SERVICE,
    '/users',
    toParams({ kw: keyword, page: 1, size }),
  )
}

/**
 * 角色搜索（关键字 + 分页）。
 *
 * @param keyword 关键字（角色码 / 名称）。
 * @param size 每页条数（缺省 20）。
 * @returns 分页响应。
 */
export function searchPlatformRoles(keyword: string, size = 20): Promise<PlatformRolePage> {
  return requiredHostApi().get<PlatformRolePage>(
    PLATFORM_SERVICE,
    '/roles',
    toParams({ kw: keyword, page: 1, size }),
  )
}

/**
 * 按主键批量取用户（空集合不发请求）。
 *
 * @param ids 用户主键（十进制字符串）。
 * @returns 分页响应；空集合返回 `null`。
 * @throws MsgQueryError 超过单次上限（10001）。
 */
export function fetchPlatformUsersByIds(ids: readonly string[]): Promise<PlatformUserPage | null> {
  return fetchByIds<PlatformUserPage>('/users', ids)
}

/**
 * 按主键批量取角色（空集合不发请求）。
 *
 * @param ids 角色主键（十进制字符串）。
 * @returns 分页响应；空集合返回 `null`。
 * @throws MsgQueryError 超过单次上限（10001）。
 */
export function fetchPlatformRolesByIds(ids: readonly string[]): Promise<PlatformRolePage | null> {
  return fetchByIds<PlatformRolePage>('/roles', ids)
}

/**
 * 用户主键 → 显示名（昵称优先，空则账号）。
 *
 * @param page 平台用户分页。
 * @returns 主键 → 名称。
 */
export function mapUserNames(page: PlatformUserPage): Map<string, string> {
  const names = new Map<string, string>()
  for (const item of page.list ?? []) {
    names.set(item.id, item.name === '' ? item.username : item.name)
  }
  return names
}

/**
 * 角色主键 → 角色名。
 *
 * @param page 平台角色分页。
 * @returns 主键 → 名称。
 */
export function mapRoleNames(page: PlatformRolePage): Map<string, string> {
  const names = new Map<string, string>()
  for (const item of page.list ?? []) {
    names.set(item.id, item.name)
  }
  return names
}

/**
 * 通用按主键批量取数（空集合不发请求、超限拒绝）。
 *
 * @param path 相对平台服务前缀的路径。
 * @param ids 主键清单。
 * @returns 分页响应；空集合返回 `null`。
 */
function fetchByIds<T>(path: string, ids: readonly string[]): Promise<T | null> {
  if (ids.length === 0) {
    return Promise.resolve(null)
  }
  if (ids.length > MAX_PRINCIPAL_IDS) {
    throw new MsgQueryError(10001)
  }
  return requiredHostApi().get<T>(
    PLATFORM_SERVICE,
    path,
    toParams({ ids: ids.join(','), page: 1, size: ids.length }),
  )
}
