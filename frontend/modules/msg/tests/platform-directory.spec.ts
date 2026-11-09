// kiwi_id: 2470
/** 平台用户 / 角色目录：搜索 / ids 批量 / 名称映射 / 上限与空集合。 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { platformGet } = vi.hoisted(() => ({ platformGet: vi.fn() }))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return { ...actual, requiredHostApi: () => ({ get: platformGet }) }
})

import {
  MAX_PRINCIPAL_IDS,
  fetchPlatformRolesByIds,
  fetchPlatformUsersByIds,
  mapRoleNames,
  mapUserNames,
  searchPlatformRoles,
  searchPlatformUsers,
} from '../src/services/platform-directory'

describe('platform-directory（Kiwi 2470）', () => {
  beforeEach(() => {
    platformGet.mockReset()
  })

  it('用户 / 角色搜索：平台服务通道 + kw / page / size', async () => {
    platformGet.mockResolvedValueOnce({ list: [], total: 0, page: 1, size: 20 })
    await searchPlatformUsers('张')
    expect(platformGet).toHaveBeenCalledWith('platform', '/users', { kw: '张', page: 1, size: 20 })

    platformGet.mockResolvedValueOnce({ list: [], total: 0, page: 1, size: 10 })
    await searchPlatformRoles('ops', 10)
    expect(platformGet).toHaveBeenCalledWith('platform', '/roles', { kw: 'ops', page: 1, size: 10 })
  })

  it('ids 批量：逗号拼接 + 页长取集合大小', async () => {
    platformGet.mockResolvedValueOnce({ list: [], total: 0, page: 1, size: 2 })
    await fetchPlatformUsersByIds(['7001', '7002'])
    expect(platformGet).toHaveBeenCalledWith('platform', '/users', { ids: '7001,7002', page: 1, size: 2 })

    platformGet.mockResolvedValueOnce({ list: [], total: 0, page: 1, size: 1 })
    await fetchPlatformRolesByIds(['11'])
    expect(platformGet).toHaveBeenCalledWith('platform', '/roles', { ids: '11', page: 1, size: 1 })
  })

  it('空集合不发请求；超过上限拒绝', async () => {
    await expect(fetchPlatformUsersByIds([])).resolves.toBeNull()
    expect(platformGet).not.toHaveBeenCalled()

    const tooMany = Array.from({ length: MAX_PRINCIPAL_IDS + 1 }, (_, index) => String(index + 1))
    expect(() => fetchPlatformRolesByIds(tooMany)).toThrow(expect.objectContaining({ code: 10001 }))
  })

  it('名称映射：用户昵称优先、空则账号；角色取名称', () => {
    const users = mapUserNames({
      list: [
        { id: '1', username: 'zhangsan', name: '张三', status: 'enabled' },
        { id: '2', username: 'lisi', name: '', status: 'enabled' },
      ],
      total: 2,
      page: 1,
      size: 2,
    } as never)
    expect(users.get('1')).toBe('张三')
    expect(users.get('2')).toBe('lisi')

    const roles = mapRoleNames({
      list: [{ id: '11', code: 'ops', name: '运维', status: 'enabled', role_type: 'custom', builtin: false, subject_count: 0 }],
      total: 1,
      page: 1,
      size: 1,
    } as never)
    expect(roles.get('11')).toBe('运维')

    expect(mapUserNames({ total: 0, page: 1, size: 0 } as never).size).toBe(0)
    expect(mapRoleNames({ total: 0, page: 1, size: 0 } as never).size).toBe(0)
  })
})
