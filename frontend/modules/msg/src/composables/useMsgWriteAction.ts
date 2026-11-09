/**
 * `BaseMsgWriteAction` 的 Vue 投影（薄适配，不是可继承类、不入基类清单）。
 *
 * 只做「核心实例 ↔ 框架响应式」：状态、错误码、进度、结果、确认标记；判定（权限 / 确认要求 / 错误码映射）
 * 全在基类。页面一律经本投影发起写操作，不直接实例化基类。
 */

import { getCurrentScope, onScopeDispose, ref, shallowRef, type Ref } from 'vue'

import {
  BaseMsgWriteAction,
  writeErrorKey,
  type ImpactRow,
  type WriteActionOptions,
} from '../base/BaseMsgWriteAction'
import { hostPermissions } from '../runtime'
import type { TaskProgress, TaskStatus } from '@bms/core'

/** 投影状态。 */
export interface WriteActionStateRef<TResult> {
  /** 链上任务状态。 */
  status: Ref<TaskStatus>
  /** 失败业务码。 */
  errorCode: Ref<number | undefined>
  /** 失败文案键（成功 / 未提交为空串）。 */
  errorKey: Ref<string>
  /** 是否被权限拦下。 */
  blocked: Ref<boolean>
  /** 提交进度（链上回传）。 */
  progress: Ref<TaskProgress | undefined>
  /** 提交结果（成功后可用）。 */
  result: Ref<TResult | undefined>
  /** 影响范围行。 */
  impactRows: readonly ImpactRow[]
  /** 是否需要二次确认。 */
  requiresConfirm: boolean
  /** 是否已确认。 */
  acknowledged: Ref<boolean>
  /** 是否可执行（有权限且不在提交中）。 */
  canSubmit: Ref<boolean>
  /** 执行一次（成功后由页面关闭弹窗并刷新）。 */
  run: () => Promise<void>
  /** 标记已确认。 */
  acknowledge: () => void
  /** 清空本轮状态。 */
  reset: () => void
  /** 取消订阅（幂等；组件卸载自动调用）。 */
  dispose: () => void
}

/**
 * 创建写操作投影。
 *
 * @param options 提交函数与确认要求。
 * @returns 响应式状态与操作入口。
 */
export function useMsgWriteAction<TResult>(options: WriteActionOptions<TResult>): WriteActionStateRef<TResult> {
  const core = new BaseMsgWriteAction<TResult>(options)
  const codes = hostPermissions()

  const status = ref<TaskStatus>(core.status)
  const errorCode = ref<number | undefined>(core.errorCode)
  const errorKey = ref('')
  const blocked = ref(false)
  const progress = shallowRef<TaskProgress | undefined>(undefined)
  const result = shallowRef<TResult | undefined>(undefined)
  const acknowledged = ref(false)
  const canSubmit = ref(core.allows(codes))
  let disposed = false

  /** 把核心实例状态同步到响应式引用。 */
  function sync(): void {
    status.value = core.status
    errorCode.value = core.errorCode
    errorKey.value = core.errorCode === undefined ? '' : writeErrorKey(core.errorCode)
    blocked.value = core.blocked
    progress.value = core.progress
    result.value = core.result
    acknowledged.value = core.acknowledged
    canSubmit.value = core.allows(codes) && core.status !== 'running'
  }

  const unsubscribe = core.onLifecycle(() => {
    sync()
  })

  /** 执行一次写操作。 */
  async function run(): Promise<void> {
    await core.run(codes)
    sync()
  }

  /** 标记已确认。 */
  function acknowledge(): void {
    core.acknowledge()
    sync()
  }

  /** 清空本轮状态。 */
  function reset(): void {
    core.reset()
    sync()
  }

  /** 取消订阅（幂等）。 */
  function dispose(): void {
    if (disposed) {
      return
    }
    disposed = true
    core.cancel()
    unsubscribe()
  }

  if (getCurrentScope() !== undefined) {
    onScopeDispose(dispose)
  }

  return {
    status,
    errorCode,
    errorKey,
    blocked,
    progress,
    result,
    impactRows: core.impactRows,
    requiresConfirm: core.requiresConfirm,
    acknowledged,
    canSubmit,
    run,
    acknowledge,
    reset,
    dispose,
  }
}
