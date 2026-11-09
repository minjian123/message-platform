// kiwi_id: 2470
/** 主题授权取数层：列表 / 授予 / 收回 / 空数据 / 错误码 / 投影取数器。 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { api } = vi.hoisted(() => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
  },
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  const { wrapScope } = await import('./support/product-mock')
  return { ...actual, requiredHostApi: () => ({ product: () => wrapScope(api) }) }
})

import {
  EMPTY_ACL_PAGE,
  fetchAcls,
  grantAcls,
  loadAcls,
  revokeAcl,
  type AclItem,
  type AclRuleInput,
} from '../src/services/acl-service'

/** 列表行样例。 */
const ITEM: AclItem = {
  acl_id: '9001',
  namespace: 'msg',
  topic: 'order.created',
  principal_type: 'user',
  principal_key: '7001',
  action: 'publish',
  effect: 'allow',
  status: 'enabled',
  updated_at: '2026-11-09T10:00:00Z',
}

/** 授予项样例。 */
const RULE: AclRuleInput = {
  namespace: 'msg',
  topic: 'order.created',
  principal_type: 'user',
  principal_key: '7001',
  action: 'publish',
  effect: 'allow',
}

describe('acl-service（Kiwi 2470）', () => {
  beforeEach(() => {
    for (const method of ['get', 'post', 'put', 'patch', 'del'] as const) {
      api[method].mockReset()
    }
  })

  it('列表：路径 / 参数透传，空数据回落空页', async () => {
    api.get.mockResolvedValueOnce({ items: [ITEM], page: 2, size: 10, total: 1 })
    await expect(fetchAcls({ namespace: 'msg', page: 2, size: 10 })).resolves.toEqual({
      items: [ITEM],
      page: 2,
      size: 10,
      total: 1,
    })
    expect(api.get).toHaveBeenCalledWith('/admin/acls', { namespace: 'msg', page: 2, size: 10 })

    api.get.mockResolvedValueOnce(null)
    await expect(fetchAcls({ page: 3, size: 5 })).resolves.toEqual({ ...EMPTY_ACL_PAGE, page: 3, size: 5 })

    api.get.mockResolvedValueOnce(null)
    await expect(fetchAcls()).resolves.toEqual(EMPTY_ACL_PAGE)
  })

  it('投影取数器把分页响应转成 items + total', async () => {
    api.get.mockResolvedValueOnce({ items: [ITEM], page: 1, size: 20, total: 1 })
    await expect(loadAcls({ page: 1, size: 20 })).resolves.toEqual({ items: [ITEM], total: 1 })
    api.get.mockResolvedValueOnce({ total: 0 })
    await expect(loadAcls({})).resolves.toEqual({ items: [], total: 0 })
  })

  it('授予：整批请求体与 changed 透传', async () => {
    api.put.mockResolvedValueOnce({ items: [ITEM], changed: ['9001'] })
    await expect(grantAcls([RULE])).resolves.toEqual({ items: [ITEM], changed: ['9001'] })
    expect(api.put).toHaveBeenCalledWith('/admin/acls', { items: [RULE] })

    api.put.mockResolvedValueOnce({ items: [ITEM], changed: [] })
    await expect(grantAcls([RULE])).resolves.toEqual({ items: [ITEM], changed: [] })
  })

  it('授予空响应视为契约违背（10000）', async () => {
    api.put.mockResolvedValueOnce(null)
    await expect(grantAcls([RULE])).rejects.toMatchObject({ code: 10000 })
  })

  it('收回：按 acl_id 调 DELETE', async () => {
    api.del.mockResolvedValueOnce({ acl_id: '9001', changed: true })
    await expect(revokeAcl('9001')).resolves.toEqual({ acl_id: '9001', changed: true })
    expect(api.del).toHaveBeenCalledWith('/admin/acls/9001', {})

    api.del.mockResolvedValueOnce({ acl_id: '9001', changed: false })
    await expect(revokeAcl('9001')).resolves.toEqual({ acl_id: '9001', changed: false })
  })

  it('错误码由宿主 / mock 抛出（350027）', async () => {
    api.get.mockResolvedValueOnce({ code: 350027, data: null })
    await expect(fetchAcls()).rejects.toMatchObject({ code: 350027 })
  })
})
