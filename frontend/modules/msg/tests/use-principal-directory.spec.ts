// kiwi_id: 2470
/** 主体名称目录投影：权限缺失不发请求 / 批量去重 / 失败降级 / 页面生命周期缓存。 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { fetchUsers, fetchRoles } = vi.hoisted(() => ({
  fetchUsers: vi.fn(),
  fetchRoles: vi.fn(),
}))

vi.mock('../src/services/platform-directory', () => ({
  USER_QUERY_PERMISSION: 'user:query',
  ROLE_QUERY_PERMISSION: 'role:query',
  fetchPlatformUsersByIds: fetchUsers,
  fetchPlatformRolesByIds: fetchRoles,
  mapUserNames: (page: { list: { id: string; username: string; name: string }[] }) =>
    new Map(page.list.map((item) => [item.id, item.name === '' ? item.username : item.name])),
  mapRoleNames: (page: { list: { id: string; name: string }[] }) =>
    new Map(page.list.map((item) => [item.id, item.name])),
}))

import { usePrincipalDirectory } from '../src/composables/usePrincipalDirectory'
import type { AclItem } from '../src/services/acl-service'

/** ACL 行样例。 */
function row(principalType: string, principalKey: string): AclItem {
  return {
    acl_id: '1',
    namespace: 'msg',
    topic: 'order.created',
    principal_type: principalType,
    principal_key: principalKey,
    action: 'publish',
    effect: 'allow',
    status: 'enabled',
    updated_at: null,
  }
}

describe('usePrincipalDirectory（Kiwi 2470）', () => {
  beforeEach(() => {
    fetchUsers.mockReset()
    fetchRoles.mockReset()
  })

  it('已知缺 user:query / role:query：不发请求并置不可用', async () => {
    const directory = usePrincipalDirectory()
    await directory.resolve([row('user', '7001'), row('role', '11')], ['msg:manage'])
    expect(fetchUsers).not.toHaveBeenCalled()
    expect(fetchRoles).not.toHaveBeenCalled()
    expect(directory.userUnavailable.value).toBe(true)
    expect(directory.roleUnavailable.value).toBe(true)
    expect(directory.label('user', '7001')).toBe('')
  })

  it('权限未装载：按类型各一次批量请求，翻页复用缓存', async () => {
    fetchUsers.mockResolvedValueOnce({ list: [{ id: '7001', username: 'z', name: '张三' }] })
    fetchRoles.mockResolvedValueOnce({ list: [{ id: '11', name: '运维' }] })
    const directory = usePrincipalDirectory()
    await directory.resolve([row('user', '7001'), row('role', '11'), row('user', '7001')], undefined)
    expect(fetchUsers).toHaveBeenCalledTimes(1)
    expect(fetchUsers).toHaveBeenCalledWith(['7001'])
    expect(fetchRoles).toHaveBeenCalledWith(['11'])
    expect(directory.label('user', '7001')).toBe('张三')
    expect(directory.label('role', '11')).toBe('运维')

    await directory.resolve([row('user', '7001')], undefined)
    expect(fetchUsers).toHaveBeenCalledTimes(1)
  })

  it('批量失败：置不可用，未命中显示空名称', async () => {
    fetchUsers.mockRejectedValueOnce({ code: 350027 })
    const directory = usePrincipalDirectory()
    await directory.resolve([row('user', '7001')], undefined)
    expect(directory.userUnavailable.value).toBe(true)
    expect(directory.label('user', '7001')).toBe('')
  })

  it('reset 清空名称与不可用标记', async () => {
    fetchUsers.mockResolvedValueOnce({ list: [{ id: '7001', username: 'z', name: '张三' }] })
    const directory = usePrincipalDirectory()
    await directory.resolve([row('user', '7001')], undefined)
    expect(directory.label('user', '7001')).toBe('张三')
    directory.reset()
    expect(directory.label('user', '7001')).toBe('')
    expect(directory.userUnavailable.value).toBe(false)
  })

  it('失败后再次解析：直接跳过已不可用类型', async () => {
    fetchUsers.mockRejectedValueOnce({ code: 10007 })
    fetchRoles.mockRejectedValueOnce({ code: 10007 })
    const directory = usePrincipalDirectory()
    await directory.resolve([row('user', '7001'), row('role', '11')], undefined)
    await directory.resolve([row('user', '7001'), row('role', '11')], undefined)
    expect(fetchUsers).toHaveBeenCalledTimes(1)
    expect(fetchRoles).toHaveBeenCalledTimes(1)
  })

  it('只有角色行：用户侧不发请求；批量返回 null 不写名称', async () => {
    fetchUsers.mockResolvedValueOnce(null)
    fetchRoles.mockResolvedValueOnce({ list: [{ id: '11', name: '运维' }] })
    const directory = usePrincipalDirectory()
    await directory.resolve([row('role', '11')], undefined)
    expect(fetchUsers).not.toHaveBeenCalled()
    expect(directory.label('role', '11')).toBe('运维')

    fetchUsers.mockResolvedValueOnce(null)
    await directory.resolve([row('user', '7002')], undefined)
    expect(directory.label('user', '7002')).toBe('')
  })

  it('解析期间 reset：过期结果丢弃', async () => {
    let release: (value: unknown) => void = () => undefined
    const pending = new Promise((resolve) => {
      release = resolve
    })
    fetchUsers.mockReturnValueOnce(pending)
    const directory = usePrincipalDirectory()
    const task = directory.resolve([row('user', '7001')], undefined)
    directory.reset()
    release({ list: [{ id: '7001', username: 'z', name: '张三' }] })
    await task
    expect(directory.label('user', '7001')).toBe('')
    expect(directory.resolving.value).toBe(false)
  })
})
