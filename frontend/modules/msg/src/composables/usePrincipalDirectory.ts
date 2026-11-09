/**
 * 主体名称目录投影（页面生命周期缓存，不持久化、不跨租户复用）。
 *
 * 只做「按主键批量解析 + 权限 / 失败降级」；不实现用户 / 角色搜索（搜索由候选组件直接经服务层发起）。
 */

import { ref, shallowRef, type Ref } from 'vue'

import type { AclItem } from '../services/acl-service'
import {
  ROLE_QUERY_PERMISSION,
  USER_QUERY_PERMISSION,
  fetchPlatformRolesByIds,
  fetchPlatformUsersByIds,
  mapRoleNames,
  mapUserNames,
} from '../services/platform-directory'

/** 主体名称目录状态。 */
export interface PrincipalDirectoryState {
  /** 用户主键 → 名称。 */
  userNames: Ref<Map<string, string>>
  /** 角色主键 → 名称。 */
  roleNames: Ref<Map<string, string>>
  /** 用户目录是否不可用（无权限 / 接口失败；本页不再尝试）。 */
  userUnavailable: Ref<boolean>
  /** 角色目录是否不可用。 */
  roleUnavailable: Ref<boolean>
  /** 是否正在解析。 */
  resolving: Ref<boolean>
  /**
   * 解析当页主体名称。
   *
   * @param rows 当页 ACL 行。
   * @param codes 已持有权限码（未装载为 `undefined`）。
   */
  resolve: (rows: readonly AclItem[], codes: readonly string[] | undefined) => Promise<void>
  /**
   * 取名称。
   *
   * @param principalType 主体类型（user / role）。
   * @param principalKey 主体标识。
   * @returns 名称；未解析到返回空串。
   */
  label: (principalType: string, principalKey: string) => string
  /** 清空缓存（重新挂载 / 租户切换时调用）。 */
  reset: () => void
}

/**
 * 创建主体名称目录投影。
 *
 * @returns 响应式名称表与解析入口。
 */
export function usePrincipalDirectory(): PrincipalDirectoryState {
  const userNames = shallowRef<Map<string, string>>(new Map())
  const roleNames = shallowRef<Map<string, string>>(new Map())
  const userUnavailable = ref(false)
  const roleUnavailable = ref(false)
  const resolving = ref(false)
  let sequence = 0

  /**
   * 判断某类型是否还能查询（已知无权限 / 已失败即不再请求）。
   *
   * @param type 主体类型。
   * @param codes 已持有权限码。
   * @returns 是否可查询。
   */
  function canQuery(type: 'user' | 'role', codes: readonly string[] | undefined): boolean {
    if (type === 'user') {
      if (userUnavailable.value) {
        return false
      }
      if (codes !== undefined && !codes.includes(USER_QUERY_PERMISSION)) {
        userUnavailable.value = true
        return false
      }
      return true
    }
    if (roleUnavailable.value) {
      return false
    }
    if (codes !== undefined && !codes.includes(ROLE_QUERY_PERMISSION)) {
      roleUnavailable.value = true
      return false
    }
    return true
  }

  /**
   * 收集尚未缓存的主键（同页去重）。
   *
   * @param rows 当页 ACL 行。
   * @param type 主体类型。
   * @param cached 已缓存名称表。
   * @returns 待解析主键。
   */
  function pendingKeys(rows: readonly AclItem[], type: 'user' | 'role', cached: Map<string, string>): string[] {
    const keys = new Set<string>()
    for (const row of rows) {
      if (row.principal_type === type && !cached.has(row.principal_key)) {
        keys.add(row.principal_key)
      }
    }
    return [...keys]
  }

  /**
   * 解析当页主体名称（用户 / 角色各一次批量请求）。
   *
   * @param rows 当页 ACL 行。
   * @param codes 已持有权限码。
   */
  async function resolve(rows: readonly AclItem[], codes: readonly string[] | undefined): Promise<void> {
    const userKeys = canQuery('user', codes) ? pendingKeys(rows, 'user', userNames.value) : []
    const roleKeys = canQuery('role', codes) ? pendingKeys(rows, 'role', roleNames.value) : []
    if (userKeys.length === 0 && roleKeys.length === 0) {
      return
    }
    const current = ++sequence
    resolving.value = true
    const [users, roles] = await Promise.allSettled([
      userKeys.length === 0 ? Promise.resolve(null) : fetchPlatformUsersByIds(userKeys),
      roleKeys.length === 0 ? Promise.resolve(null) : fetchPlatformRolesByIds(roleKeys),
    ])
    if (current !== sequence) {
      resolving.value = false
      return
    }
    if (users.status === 'fulfilled') {
      if (users.value !== null) {
        userNames.value = new Map([...userNames.value, ...mapUserNames(users.value)])
      }
    } else {
      userUnavailable.value = true
    }
    if (roles.status === 'fulfilled') {
      if (roles.value !== null) {
        roleNames.value = new Map([...roleNames.value, ...mapRoleNames(roles.value)])
      }
    } else {
      roleUnavailable.value = true
    }
    resolving.value = false
  }

  /**
   * 取名称。
   *
   * @param principalType 主体类型。
   * @param principalKey 主体标识。
   * @returns 名称；未解析到返回空串。
   */
  function label(principalType: string, principalKey: string): string {
    return (principalType === 'role' ? roleNames.value : userNames.value).get(principalKey) ?? ''
  }

  /** 清空缓存。 */
  function reset(): void {
    sequence += 1
    userNames.value = new Map()
    roleNames.value = new Map()
    userUnavailable.value = false
    roleUnavailable.value = false
    resolving.value = false
  }

  return { userNames, roleNames, userUnavailable, roleUnavailable, resolving, resolve, label, reset }
}
