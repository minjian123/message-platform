/**
 * 测试侧产品域 mock：模拟宿主请求层已解包统一响应。
 *
 * 既有用例大量按 `{ code, message, data }` 信封准备数据；宿主真实行为是「成功返回 data、
 * 业务码非 0 抛带 code 的错误」。本包装把信封在测试边界解包，既保留既有断言，又与真机一致。
 */

import { MsgQueryError } from '../../src/services/query-error'

/** 统一响应信封的最小形状。 */
interface Envelope {
  code: number
  data?: unknown
}

/**
 * 是否统一响应信封。
 *
 * @param value 候选值。
 * @returns 是否信封。
 */
function isEnvelope(value: unknown): value is Envelope {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  const record = value as Record<string, unknown>
  return typeof record.code === 'number' && 'data' in record
}

/**
 * 解包信封（业务码非 0 抛带 code 的错误；原始 data 原样返回）。
 *
 * @param value mock 返回值。
 * @returns 解包后的数据。
 */
export function unwrapMock<T>(value: T): T {
  if (!isEnvelope(value)) {
    return value
  }
  if (value.code !== 0) {
    throw new MsgQueryError(value.code)
  }
  return value.data as T
}

/**
 * 包装一个产品域 mock 作用域：所有方法与 `request` 统一解包。
 *
 * @param scope mock 作用域。
 * @returns 与宿主语义一致的作用域。
 */
export function wrapScope<T extends object>(scope: T): T {
  const methods = ['get', 'post', 'put', 'patch', 'del', 'request'] as const
  const wrapped: Record<string, (...args: unknown[]) => Promise<unknown>> = {}
  for (const method of methods) {
    const original = (scope as Record<string, unknown>)[method]
    if (typeof original !== 'function') {
      continue
    }
    wrapped[method] = async (...args: unknown[]) =>
      unwrapMock(await (original as (...inner: unknown[]) => unknown)(...args))
  }
  return wrapped as T
}
