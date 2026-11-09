/**
 * 产品域请求 helper（msg 管理模块统一入口）。
 *
 * 宿主 `api.product(产品键, 域)` 已统一解包统一响应：成功返回 `data`，业务码非 0 由宿主
 * 抛带 `code` 的错误（401 刷新 / 跳登录亦由宿主处理）。本模块**不再二次判断信封**，只负责：
 * 产品键 / 域段、空查询参数清理、必填 `data` 校验与 GET / 写方法直调。
 *
 * 说明：本文件是 06-7 对既有取数层偏差的统一修正——此前 `admin-service` / `quota-service`
 * 按 `{ code, data }` 信封解包，真实宿主下会把 `data` 当信封读取而报错；测试 mock 信封掩盖了问题。
 */

import type { ModuleApiScope } from '@bms/core'

import { requiredHostApi } from '../runtime'
import { MsgQueryError } from './query-error'

export { MsgQueryError } from './query-error'

/** 产品键（与平台产品登记同源）。 */
export const MSG_PRODUCT = 'msg'
/** 产品命名空间段。 */
export const MSG_NAMESPACE = 'msg'

/**
 * 产品域请求作用域。
 *
 * @returns 产品域作用域。
 */
export function productScope(): ModuleApiScope {
  return requiredHostApi().product(MSG_PRODUCT, MSG_NAMESPACE)
}

/**
 * 清理查询参数（丢掉 `undefined` / `null` / 空串，保留 `0` / `false`）。
 *
 * @param params 查询参数对象。
 * @returns 宿主请求层可接收的参数表。
 */
export function toParams(params: object | undefined): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  if (params === undefined) {
    return result
  }
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      result[key] = value
    }
  }
  return result
}

/**
 * 必填 `data`（空视为契约违背）。
 *
 * @param data 宿主解包后的数据。
 * @returns 非空数据。
 * @throws MsgQueryError `data` 为空（10000）。
 */
export function requiredData<T>(data: T | null | undefined): T {
  if (data === null || data === undefined) {
    throw new MsgQueryError(10000)
  }
  return data
}

/**
 * GET 产品域资源。
 *
 * @param path 相对产品域前缀的路径。
 * @param params 查询参数。
 * @returns 宿主解包后的数据。
 */
export function getData<T>(path: string, params?: object): Promise<T> {
  return productScope().get<T>(path, toParams(params))
}

/**
 * POST 产品域资源（宿主自动生成幂等键）。
 *
 * @param path 相对产品域前缀的路径。
 * @param body 请求体。
 * @returns 宿主解包后的数据。
 */
export function postData<T>(path: string, body?: unknown): Promise<T> {
  return productScope().post<T>(path, body)
}

/**
 * PUT 产品域资源（宿主自动生成幂等键）。
 *
 * @param path 相对产品域前缀的路径。
 * @param body 请求体。
 * @returns 宿主解包后的数据。
 */
export function putData<T>(path: string, body?: unknown): Promise<T> {
  return productScope().put<T>(path, body)
}

/**
 * PATCH 产品域资源（宿主自动生成幂等键）。
 *
 * @param path 相对产品域前缀的路径。
 * @param body 请求体。
 * @returns 宿主解包后的数据。
 */
export function patchData<T>(path: string, body?: unknown): Promise<T> {
  return productScope().patch<T>(path, body)
}

/**
 * DELETE 产品域资源。
 *
 * @param path 相对产品域前缀的路径。
 * @param params 查询参数。
 * @returns 宿主解包后的数据。
 */
export function delData<T>(path: string, params?: object): Promise<T> {
  return productScope().del<T>(path, toParams(params))
}
