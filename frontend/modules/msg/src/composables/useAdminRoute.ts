/**
 * 管理页路由上下文（只读消费共享路由实例：路径参数 / 查询参数 / 返回列表）。
 *
 * 不自建路由、不改写宿主路由表；只做深链取值与 `push` 导航。
 */

import { useRoute, useRouter } from 'vue-router'

/** 路由上下文。 */
export interface AdminRouteState {
  /** 路径参数（值统一字符串）。 */
  params: Record<string, string>
  /** 查询参数（值统一字符串）。 */
  query: Record<string, string>
  /** 跳转到模块内路径。 */
  go: (path: string) => void
}

/**
 * 取当前路由上下文。
 *
 * @returns 路由上下文。
 */
export function useAdminRoute(): AdminRouteState {
  const route = useRoute()
  const router = useRouter()

  /** 把路由参数归一为字符串表（数组取首项）。 */
  function asStrings(source: Record<string, unknown>): Record<string, string> {
    const result: Record<string, string> = {}
    for (const [key, value] of Object.entries(source)) {
      if (typeof value === 'string') {
        result[key] = value
      } else if (Array.isArray(value) && typeof value[0] === 'string') {
        result[key] = value[0]
      }
    }
    return result
  }

  return {
    params: asStrings(route.params as Record<string, unknown>),
    query: asStrings(route.query as Record<string, unknown>),
    go: (path: string) => {
      void router.push(path)
    },
  }
}
