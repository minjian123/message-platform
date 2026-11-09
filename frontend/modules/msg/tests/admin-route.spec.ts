// kiwi_id: 2464
/** 管理页路由上下文：路径 / 查询参数归一化与深链导航（护栏用例）。 */

import { describe, expect, it, vi } from 'vitest'

const { push, replace, route } = vi.hoisted(() => ({
  push: vi.fn(),
  replace: vi.fn(),
  route: {
    value: {
      path: '/msg/acls',
      params: {} as Record<string, unknown>,
      query: {} as Record<string, unknown>,
    },
  },
}))

vi.mock('vue-router', () => ({
  useRoute: () => route.value,
  useRouter: () => ({ push, replace }),
}))

import { useAdminRoute } from '../src/composables/useAdminRoute'

describe('useAdminRoute（Kiwi 2464）', () => {
  it('字符串 / 字符串数组 / 非字符串 / 空数组 / 数字值分别归一', () => {
    route.value = {
      params: { topicId: '42', repeated: ['a', 'b'], numeric: 7, empty: [], badArray: [3] },
      query: { namespace: 'bms', repeated: ['x', 'y'] },
    }
    const state = useAdminRoute()
    expect(state.params).toEqual({ topicId: '42', repeated: 'a' })
    expect(state.query).toEqual({ namespace: 'bms', repeated: 'x' })
  })

  it('空参数表与深链导航', () => {
    route.value = { path: '/msg/acls', params: {}, query: {} }
    const state = useAdminRoute()
    expect(state.path).toBe('/msg/acls')
    expect(state.params).toEqual({})
    expect(state.query).toEqual({})
    state.go('/msg/topics')
    expect(push).toHaveBeenCalledWith('/msg/topics')
  })

  it('替换查询参数不新增历史记录', () => {
    route.value = { path: '/msg/acls', params: {}, query: {} }
    const state = useAdminRoute()
    state.replaceQuery({ namespace: 'msg', page: '2' })
    expect(replace).toHaveBeenCalledWith({ path: '/msg/acls', query: { namespace: 'msg', page: '2' } })
  })
})
