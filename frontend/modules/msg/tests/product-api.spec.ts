// kiwi_id: 2470
/** 产品域请求 helper：宿主已解包 / 空数据必填 / 参数清理 / 错误码透传。 */

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
  MSG_NAMESPACE,
  MSG_PRODUCT,
  MsgQueryError,
  delData,
  getData,
  patchData,
  postData,
  putData,
  requiredData,
  toParams,
} from '../src/services/product-api'

describe('product-api（Kiwi 2470）', () => {
  beforeEach(() => {
    api.get.mockReset()
    api.post.mockReset()
    api.put.mockReset()
    api.patch.mockReset()
    api.del.mockReset()
  })

  it('产品键 / 命名空间同源，参数清理保留 0 / false', () => {
    expect(MSG_PRODUCT).toBe('msg')
    expect(MSG_NAMESPACE).toBe('msg')
    expect(toParams(undefined)).toEqual({})
    expect(toParams({ a: '', b: null, c: undefined, d: 0, e: false, f: 'x' })).toEqual({ d: 0, e: false, f: 'x' })
  })

  it('GET 直接返回宿主解包后的 data（含信封兼容）', async () => {
    api.get.mockResolvedValueOnce({ items: [] })
    await expect(getData('/x', { a: 1 })).resolves.toEqual({ items: [] })
    expect(api.get).toHaveBeenCalledWith('/x', { a: 1 })

    api.get.mockResolvedValueOnce({ code: 0, data: { ok: true } })
    await expect(getData('/y')).resolves.toEqual({ ok: true })
  })

  it('业务码非 0 由宿主 / mock 抛出并带 code', async () => {
    api.get.mockResolvedValueOnce({ code: 350027, data: null })
    await expect(getData('/x')).rejects.toMatchObject({ code: 350027 })
    api.get.mockResolvedValueOnce({ code: 350027, data: null })
    await expect(getData('/x')).rejects.toBeInstanceOf(MsgQueryError)
  })

  it('requiredData 空值抛 10000，非空原样返回', () => {
    expect(requiredData({ a: 1 })).toEqual({ a: 1 })
    expect(() => requiredData(null)).toThrow(MsgQueryError)
    expect(() => requiredData(undefined)).toThrow(MsgQueryError)
    try {
      requiredData(null)
    } catch (error) {
      expect((error as MsgQueryError).code).toBe(10000)
    }
  })

  it('POST / PUT / PATCH / DELETE 各自走对应方法', async () => {
    api.post.mockResolvedValueOnce({ id: '1' })
    api.put.mockResolvedValueOnce({ id: '2' })
    api.patch.mockResolvedValueOnce({ id: '3' })
    api.del.mockResolvedValueOnce({ changed: true })
    await expect(postData('/a', { x: 1 })).resolves.toEqual({ id: '1' })
    await expect(putData('/b', { x: 2 })).resolves.toEqual({ id: '2' })
    await expect(patchData('/c', { x: 3 })).resolves.toEqual({ id: '3' })
    await expect(delData('/d', { confirm: true })).resolves.toEqual({ changed: true })
    expect(api.post).toHaveBeenCalledWith('/a', { x: 1 })
    expect(api.put).toHaveBeenCalledWith('/b', { x: 2 })
    expect(api.patch).toHaveBeenCalledWith('/c', { x: 3 })
    expect(api.del).toHaveBeenCalledWith('/d', { confirm: true })
  })
})
