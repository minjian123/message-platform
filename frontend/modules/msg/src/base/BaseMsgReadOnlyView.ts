/**
 * 产品段组件基类：msg 管理模块只读视图共用横切。
 *
 * 继承平台 `BaseTable`（→ `BaseDataState` → 组件根 → 体系根），分页 / 状态机 / 竞态一律取自链上基类；
 * 本类只补产品段语义：只读筛选条件、四态与错误码文案键、权限码判定入口。
 * 登记见 `msg文档/前端基类清单.md` §2 与 `设计/基类体系/前端基类实现契约.md` §7。
 */

import { BaseTable, type DataStateName } from '@bms/core'

import { MSG_QUERY_PERMISSION } from '../runtime'

/** 只读视图的展示态（在平台数据状态机上补「无权限」与「待选条件」两个展示态）。 */
export type ReadOnlyViewState = DataStateName | 'denied' | 'choose'

/** 筛选条件容器（键与只读端点查询参数同名，值不拼装租户参数）。 */
export type ReadOnlyFilters = Record<string, string | number | undefined>

/** 一次查询的入参（筛选条件 + 分页）。 */
export type ReadOnlyQueryParams = Record<string, string | number | undefined>

/** 本轮涉及的后端错误码（06-1-1 `_QUERY_ERRORS` + 契约层 / 基座通用码；06-6-2 补 `350016`）。 */
export const QUERY_ERROR_CODES: readonly number[] = [
  350001, 350009, 350010, 350011, 350012, 350013, 350016, 350027, 10001, 30001,
]

/** 未命中错误码时的兜底文案键。 */
export const ERROR_KEY_FALLBACK = 'error.fallback'

const REGISTERED_CODES = new Set<number>(QUERY_ERROR_CODES)

/**
 * 错误码 → 模块 i18n 文案键。
 *
 * @param code 后端业务码（缺失 / 未登记为 `undefined`）。
 * @returns 文案键。
 */
export function errorKeyOf(code: number | undefined): string {
  return code !== undefined && REGISTERED_CODES.has(code) ? `error.${String(code)}` : ERROR_KEY_FALLBACK
}

/**
 * 展示态 → 模块 i18n 文案键。
 *
 * @param state 展示态。
 * @returns 文案键；`ready` 无文案返回空串。
 */
export function stateKeyOf(state: ReadOnlyViewState): string {
  switch (state) {
    case 'loading':
      return 'state.loading'
    case 'empty':
      return 'state.empty'
    case 'error':
      return 'state.error'
    case 'denied':
      return 'state.denied'
    case 'choose':
      return 'state.choose'
    case 'ready':
      return ''
  }
}

/**
 * 从任意异常里提取后端业务码（取不到返回 `undefined`）。
 *
 * @param error 捕获的异常。
 * @returns 业务码。
 */
export function errorCodeOf(error: unknown): number | undefined {
  if (typeof error !== 'object' || error === null) {
    return undefined
  }
  const code = (error as { code?: unknown }).code
  return typeof code === 'number' && Number.isFinite(code) ? code : undefined
}

/**
 * 只读视图基类（产品段）。
 *
 * 不重写平台分页 / 状态机方法；只在加载编排里调用链上 `begin()` / `settle()` / `setRows()`。
 */
export class BaseMsgReadOnlyView<T = unknown> extends BaseTable {
  /** 只读视图所需权限码（与后端 `msg:query` 同源）。 */
  readonly permission: string = MSG_QUERY_PERMISSION

  /** 当前失败业务码（成功结算后清空）。 */
  errorCode: number | undefined

  /** 只读筛选条件（键与端点查询参数同名）。 */
  readonly filters: ReadOnlyFilters

  /**
   * 构造只读视图。
   *
   * @param initialFilters 初始筛选条件。
   */
  constructor(initialFilters: ReadOnlyFilters = {}) {
    super()
    this.filters = { ...initialFilters }
  }

  /**
   * 设置单个筛选条件（空串 / `undefined` 视为清除）。
   *
   * @param key 参数名。
   * @param value 参数值。
   */
  setFilter(key: string, value: string | number | undefined): void {
    if (value === undefined || value === '') {
      delete this.filters[key]
    } else {
      this.filters[key] = value
    }
  }

  /** 清空筛选条件。 */
  clearFilters(): void {
    for (const key of Object.keys(this.filters)) {
      delete this.filters[key]
    }
  }

  /**
   * 组合本次查询参数（筛选 + 链上分页值）。
   *
   * @returns 查询参数。
   */
  queryParams(): ReadOnlyQueryParams {
    return { ...this.filters, page: this.page, size: this.pageSize }
  }

  /**
   * 开始一次加载（取链上竞态令牌）。
   *
   * @returns 竞态令牌。
   */
  beginLoad(): number {
    return this.begin()
  }

  /**
   * 以令牌结算成功结果（过期令牌直接丢弃）。
   *
   * @param token 竞态令牌。
   * @param rows 当页行。
   * @param total 过滤后总数。
   * @returns 是否生效。
   */
  settleRows(token: number, rows: readonly T[], total: number): boolean {
    if (!this.settle(token, rows.length === 0 ? 'empty' : 'ready')) {
      return false
    }
    this.setRows(rows, total)
    this.errorCode = undefined
    return true
  }

  /**
   * 以令牌结算失败结果（过期令牌直接丢弃）。
   *
   * @param token 竞态令牌。
   * @param code 业务码（可缺省）。
   * @returns 是否生效。
   */
  settleFailure(token: number, code?: number): boolean {
    if (!this.settle(token, 'error')) {
      return false
    }
    this.errorCode = code
    return true
  }

  /**
   * 当前失败文案键。
   *
   * @returns 文案键。
   */
  errorTextKey(): string {
    return errorKeyOf(this.errorCode)
  }

  /**
   * 当前展示态文案键。
   *
   * @returns 文案键（`ready` 为空串）。
   */
  stateTextKey(): string {
    return stateKeyOf(this.state)
  }

  /**
   * 展示层权限判定（集合未装载时按宿主守卫口径放行）。
   *
   * @param codes 已持有权限码集合。
   * @returns 是否放行。
   */
  allows(codes: readonly string[] | undefined): boolean {
    return codes === undefined || codes.includes(this.permission)
  }
}
