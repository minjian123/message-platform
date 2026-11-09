/**
 * 产品段写操作基类：msg 管理模块六个写操作的共用横切。
 *
 * 继承平台 `BaseAsyncTask`（→ `BaseComponent` → 体系根）：提交 / 状态机 / 进度 / 取消 / 退避轮询一律取自链上基类；
 * 本类只补产品段语义：`msg:manage` 权限判定、错误码文案键、二次确认与影响范围、结果重置。
 * 登记见 `msg文档/前端基类清单.md` §2 与 `设计/基类体系/前端基类实现契约.md` §7。
 */

import { BaseAsyncTask } from '@bms/core'

import { MSG_MANAGE_PERMISSION, hostPermissions } from '../runtime'
import { errorCodeOf } from './BaseMsgReadOnlyView'

/** 影响范围一行（二次确认弹窗逐行渲染）。 */
export interface ImpactRow {
  /** 行标签（已解析的文案）。 */
  label: string
  /** 行值（已格式化的文本）。 */
  value: string
  /** 强调为危险值（如「删除后不可恢复」相关数量）。 */
  tone?: 'danger'
}

/** 写动作构造入参。 */
export interface WriteActionOptions<TResult> {
  /** 实际提交函数（页面经取数层组装，基类不拼请求）。 */
  runner: () => Promise<TResult>
  /** 影响范围行（删主题 / 位点重置给值；配置类操作为空）。 */
  impactRows?: readonly ImpactRow[]
  /** 是否强制二次确认（缺省：有影响范围行即需要）。 */
  requiresConfirm?: boolean
}

/** 写操作涉及的后端错误码（读路径集合之外的部分）。 */
export const WRITE_ERROR_CODES: readonly number[] = [
  350001, 350002, 350003, 350008, 350009, 350010, 350013, 350025, 350027, 350028, 10001, 30001,
]

/** 未命中错误码时的兜底文案键。 */
export const WRITE_ERROR_FALLBACK = 'error.fallback'

const REGISTERED_CODES = new Set<number>(WRITE_ERROR_CODES)

/**
 * 错误码 → 模块 i18n 文案键（写操作集合）。
 *
 * @param code 后端业务码（缺失 / 未登记为 `undefined`）。
 * @returns 文案键。
 */
export function writeErrorKey(code: number | undefined): string {
  return code !== undefined && REGISTERED_CODES.has(code) ? `error.${String(code)}` : WRITE_ERROR_FALLBACK
}

/**
 * 写操作基类（产品段）。
 *
 * 不重写链上 `submit()` / `cancel()` / `pollInterval`；`run()` 只做「权限前置 + 清态 + 交给链上提交」。
 */
export class BaseMsgWriteAction<TResult = unknown> extends BaseAsyncTask<TResult> {
  /** 写操作所需权限码（与后端 `msg:manage` 同源）。 */
  readonly permission: string = MSG_MANAGE_PERMISSION

  /** 本次提交的失败业务码（成功 / 重置后清空）。 */
  errorCode: number | undefined

  /** 影响范围行（二次确认展示；空表示无需确认）。 */
  readonly impactRows: readonly ImpactRow[]

  /** 是否需要二次确认。 */
  readonly requiresConfirm: boolean

  /** 二次确认是否已完成（弹窗确认回调里置真）。 */
  acknowledged = false

  /** 是否被权限拦下（无 `msg:manage` 时不发请求）。 */
  blocked = false

  /**
   * 构造写动作。
   *
   * @param options 提交函数与确认要求。
   */
  constructor(options: WriteActionOptions<TResult>) {
    super()
    this.impactRows = options.impactRows ?? []
    this.requiresConfirm = options.requiresConfirm ?? this.impactRows.length > 0
    // 捕获错误码后再抛出：链上 `submit()` 的 `status='error'` 语义不变，页面据 status + errorCode 渲染。
    this.executor = async () => {
      try {
        return await options.runner()
      } catch (error) {
        this.errorCode = errorCodeOf(error)
        throw error
      }
    }
  }

  /**
   * 展示层权限判定（权限集合未装载时按宿主守卫口径放行）。
   *
   * @param codes 已持有权限码集合。
   * @returns 是否放行。
   */
  allows(codes: readonly string[] | undefined): boolean {
    return codes === undefined || codes.includes(this.permission)
  }

  /**
   * 当前失败文案键。
   *
   * @returns 文案键。
   */
  errorKey(): string {
    return writeErrorKey(this.errorCode)
  }

  /** 标记「已确认」（只由确认弹窗的 confirm 回调调用）。 */
  acknowledge(): void {
    this.acknowledged = true
  }

  /** 清空本轮状态（重开弹窗 / 重试前调用；链上结果与进度同时清空）。 */
  reset(): void {
    this.errorCode = undefined
    this.blocked = false
    this.acknowledged = false
    this.progress = undefined
  }

  /**
   * 执行一次写操作（权限前置 + 清态 + 链上提交）。
   *
   * @param codes 已持有权限码集合（缺省取宿主注入快照）。
   * @returns 完成后 resolve（失败由 `status` / `errorCode` 表达，不抛出）。
   */
  async run(codes: readonly string[] | undefined = hostPermissions()): Promise<void> {
    this.errorCode = undefined
    if (!this.allows(codes)) {
      this.blocked = true
      // 与后端 `30001` 同码：展示层不自己造码，也不发一次注定被拒的请求。
      this.errorCode = 30001
      return
    }
    this.blocked = false
    await this.submit()
  }
}
