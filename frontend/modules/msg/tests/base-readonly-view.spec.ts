// kiwi_id: 2464
/** 产品段前端基类：继承链 / 四态与错误码文案键 / 筛选 / 竞态 / 权限判定。 */

import { BaseDataState, BaseTable } from '@bms/core'
import { describe, expect, it } from 'vitest'

import {
  BaseMsgReadOnlyView,
  ERROR_KEY_FALLBACK,
  QUERY_ERROR_CODES,
  errorCodeOf,
  errorKeyOf,
  stateKeyOf,
} from '../src/base/BaseMsgReadOnlyView'
import { MSG_QUERY_PERMISSION } from '../src/runtime'

describe('BaseMsgReadOnlyView（Kiwi 2464）', () => {
  it('继承平台 BaseTable → BaseDataState，横切取自链上基类', () => {
    const view = new BaseMsgReadOnlyView<string>()
    expect(view).toBeInstanceOf(BaseTable)
    expect(view).toBeInstanceOf(BaseDataState)
    expect(view.identifier).toBe('table')
    expect(view.permission).toBe(MSG_QUERY_PERMISSION)
    view.setPage(3)
    view.setPageSize(50)
    expect(view.page).toBe(3)
    expect(view.pageSize).toBe(50)
    expect(view.queryParams()).toEqual({ page: 3, size: 50 })
    view.setRows(['a', 'b'], 7)
    expect(view.rows).toEqual(['a', 'b'])
    expect(view.total).toBe(7)
  })

  it('初始筛选条件与组合查询参数', () => {
    const view = new BaseMsgReadOnlyView({ namespace: 'bms', topic: 'order.created' })
    expect(view.queryParams()).toEqual({ namespace: 'bms', topic: 'order.created', page: 1, size: view.pageSize })
  })

  it('setFilter 支持设置 / 清除 / 删除未定义值，clearFilters 全清', () => {
    const view = new BaseMsgReadOnlyView()
    view.setFilter('namespace', 'bms')
    view.setFilter('topic', 'order.created')
    expect(view.filters).toEqual({ namespace: 'bms', topic: 'order.created' })
    view.setFilter('topic', '')
    expect(view.filters).toEqual({ namespace: 'bms' })
    view.setFilter('namespace', undefined)
    expect(view.filters).toEqual({})
    view.setFilter('partition', 2)
    view.clearFilters()
    expect(view.filters).toEqual({})
  })

  it('错误码 → 文案键：九个登记码、未登记码与缺失码回落兜底', () => {
    expect(QUERY_ERROR_CODES).toHaveLength(9)
    for (const code of QUERY_ERROR_CODES) {
      expect(errorKeyOf(code)).toBe(`error.${String(code)}`)
    }
    expect(errorKeyOf(999999)).toBe(ERROR_KEY_FALLBACK)
    expect(errorKeyOf(undefined)).toBe(ERROR_KEY_FALLBACK)
  })

  it('展示态 → 文案键：五态全覆盖，ready 无文案', () => {
    expect(stateKeyOf('loading')).toBe('state.loading')
    expect(stateKeyOf('empty')).toBe('state.empty')
    expect(stateKeyOf('error')).toBe('state.error')
    expect(stateKeyOf('denied')).toBe('state.denied')
    expect(stateKeyOf('choose')).toBe('state.choose')
    expect(stateKeyOf('ready')).toBe('')
  })

  it('异常取码：非对象 / 缺码 / 非数值 / NaN / 正常数值', () => {
    expect(errorCodeOf('boom')).toBeUndefined()
    expect(errorCodeOf(null)).toBeUndefined()
    expect(errorCodeOf({})).toBeUndefined()
    expect(errorCodeOf({ code: '350001' })).toBeUndefined()
    expect(errorCodeOf({ code: Number.NaN })).toBeUndefined()
    expect(errorCodeOf({ code: 350001 })).toBe(350001)
  })

  it('结算成功：空页 / 有数据 / 过期令牌丢弃', () => {
    const view = new BaseMsgReadOnlyView<string>()
    const first = view.beginLoad()
    expect(view.state).toBe('loading')
    expect(view.settleRows(first, [], 0)).toBe(true)
    expect(view.state).toBe('empty')
    expect(view.total).toBe(0)

    const second = view.beginLoad()
    const stale = view.beginLoad()
    expect(view.settleRows(second, ['a'], 1)).toBe(false)
    expect(view.settleRows(stale, ['b'], 2)).toBe(true)
    expect(view.state).toBe('ready')
    expect(view.rows).toEqual(['b'])
    expect(view.total).toBe(2)
  })

  it('结算失败：按码记文案键，过期令牌不覆盖，成功后清空', () => {
    const view = new BaseMsgReadOnlyView<string>()
    const first = view.beginLoad()
    const latest = view.beginLoad()
    expect(view.settleFailure(first, 350001)).toBe(false)
    expect(view.settleFailure(latest, 350027)).toBe(true)
    expect(view.state).toBe('error')
    expect(view.errorTextKey()).toBe('error.350027')
    expect(view.stateTextKey()).toBe('state.error')

    const next = view.beginLoad()
    expect(view.settleRows(next, ['row'], 1)).toBe(true)
    expect(view.errorTextKey()).toBe(ERROR_KEY_FALLBACK)
    expect(view.stateTextKey()).toBe('')
  })

  it('结算失败可以不带码（兜底文案）', () => {
    const view = new BaseMsgReadOnlyView()
    const token = view.beginLoad()
    expect(view.settleFailure(token)).toBe(true)
    expect(view.errorTextKey()).toBe(ERROR_KEY_FALLBACK)
  })

  it('权限判定：集合未装载放行、含码放行、无码拒绝', () => {
    const view = new BaseMsgReadOnlyView()
    expect(view.allows(undefined)).toBe(true)
    expect(view.allows(['msg:manage', MSG_QUERY_PERMISSION])).toBe(true)
    expect(view.allows(['msg:manage'])).toBe(false)
  })
})
