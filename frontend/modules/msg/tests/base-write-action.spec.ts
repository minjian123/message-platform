// kiwi_id: 2466
/** 产品段写操作基类：继承链 / 权限 / 错误码 / 二次确认 / 结果态。 */

import { BaseAsyncTask } from '@bms/core'
import { describe, expect, it } from 'vitest'

import {
  BaseMsgWriteAction,
  WRITE_ERROR_CODES,
  writeErrorKey,
  type ImpactRow,
} from '../src/base/BaseMsgWriteAction'

/** 影响范围样例。 */
const ROWS: readonly ImpactRow[] = [
  { label: '分区数', value: '2' },
  { label: '死信数', value: '1', tone: 'danger' },
]

describe('BaseMsgWriteAction（Kiwi 2466）', () => {
  it('继承链：BaseMsgWriteAction → BaseAsyncTask（组件根链上派生）', () => {
    const action = new BaseMsgWriteAction({ runner: async () => 'ok' })
    expect(action).toBeInstanceOf(BaseAsyncTask)
    expect(action.permission).toBe('msg:manage')
  })

  it('错误码集合覆盖写操作场景，未命中回落兜底键', () => {
    expect(WRITE_ERROR_CODES).toContain(350028)
    expect(writeErrorKey(350028)).toBe('error.350028')
    expect(writeErrorKey(999999)).toBe('error.fallback')
    expect(writeErrorKey(undefined)).toBe('error.fallback')
    expect(new BaseMsgWriteAction({ runner: async () => 1 }).errorKey()).toBe('error.fallback')
  })

  it('权限判定：未装载放行、缺码拒绝、含码放行', () => {
    const action = new BaseMsgWriteAction({ runner: async () => 1 })
    expect(action.allows(undefined)).toBe(true)
    expect(action.allows(['msg:query'])).toBe(false)
    expect(action.allows(['msg:manage'])).toBe(true)
  })

  it('无写权限：不发请求，置 blocked 与 30001', async () => {
    let called = 0
    const action = new BaseMsgWriteAction({
      runner: async () => {
        called += 1
        return 'ok'
      },
    })
    await action.run(['msg:query'])
    expect(called).toBe(0)
    expect(action.blocked).toBe(true)
    expect(action.errorCode).toBe(30001)
    expect(action.status).toBe('idle')
  })

  it('提交成功：status=done 且结果可取，重新执行前清掉上一轮错误码', async () => {
    const action = new BaseMsgWriteAction({ runner: async () => 'created' })
    await action.run(['msg:manage'])
    expect(action.status).toBe('done')
    expect(action.result).toBe('created')
    expect(action.errorCode).toBeUndefined()
  })

  it('提交失败：捕获业务码并置 error', async () => {
    const action = new BaseMsgWriteAction({
      runner: async () => {
        throw Object.assign(new Error('boom'), { code: 350028 })
      },
    })
    await action.run(['msg:manage'])
    expect(action.status).toBe('error')
    expect(action.errorCode).toBe(350028)
    expect(action.errorKey()).toBe('error.350028')
  })

  it('确认要求：有影响范围行即需要确认，可显式覆盖；acknowledge / reset 幂等', () => {
    const withRows = new BaseMsgWriteAction({ runner: async () => 1, impactRows: ROWS })
    expect(withRows.requiresConfirm).toBe(true)
    expect(withRows.impactRows).toHaveLength(2)
    expect(withRows.acknowledged).toBe(false)
    withRows.acknowledge()
    expect(withRows.acknowledged).toBe(true)

    const configOnly = new BaseMsgWriteAction({ runner: async () => 1 })
    expect(configOnly.requiresConfirm).toBe(false)

    const forced = new BaseMsgWriteAction({ runner: async () => 1, requiresConfirm: true })
    expect(forced.requiresConfirm).toBe(true)

    const reset = new BaseMsgWriteAction({ runner: async () => 1, impactRows: ROWS })
    reset.acknowledge()
    reset.progress = { value: 1 }
    reset.reset()
    expect([reset.acknowledged, reset.progress, reset.errorCode, reset.blocked]).toEqual([
      false,
      undefined,
      undefined,
      false,
    ])
  })
})
