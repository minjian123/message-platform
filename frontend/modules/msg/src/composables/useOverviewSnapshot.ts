/**
 * 平台概览取数与轮询（概览页与工作台卡片共用同一份投影，不另造取数层）。
 *
 * 挂载取一次 + 每 30 秒刷新；卸载清定时器；刷新不清空已有数值（失败保留上次成功值）；
 * 竞态由基类 `BaseDataState` 令牌保证。
 */

import { getCurrentScope, onScopeDispose, computed, ref, type ComputedRef, type Ref } from 'vue'

import type { ReadOnlyViewState } from '../base/BaseMsgReadOnlyView'
import type { Overview } from '../services/admin-service'
import { fetchOverview } from '../services/admin-service'
import { OVERVIEW_POLL_INTERVAL_MS } from '../constants'
import { useMsgReadOnlyView } from './useMsgReadOnlyView'

/** 轮询选项。 */
export interface OverviewSnapshotOptions {
  /** 轮询间隔（毫秒；测试可注入短间隔）。 */
  intervalMs?: number
  /** 已持有权限码（缺省取宿主注入快照）。 */
  permissionCodes?: readonly string[] | undefined
  /** 是否自动开始（缺省 true）。 */
  autoStart?: boolean
}

/** 概览投影状态。 */
export interface OverviewSnapshotState {
  /** 最近一次成功快照（未取到为 null；刷新失败时保留上次成功值）。 */
  overview: ComputedRef<Overview | null>
  /** 展示态。 */
  state: Ref<ReadOnlyViewState>
  /** 展示态 / 错误码文案键。 */
  textKey: Ref<string>
  /** 是否首次加载。 */
  firstLoad: Ref<boolean>
  /** 最近一次成功刷新时刻（未成功过为 null）。 */
  lastUpdated: Ref<Date | null>
  /** 手动刷新。 */
  refresh: () => Promise<void>
  /** 停止轮询（不清空数据）。 */
  stop: () => void
  /** 开始轮询并立即取一次。 */
  start: () => void
  /** 停止并释放投影（幂等；组件卸载自动调用）。 */
  dispose: () => void
}

/**
 * 创建概览快照投影。
 *
 * @param options 轮询选项。
 * @returns 概览状态与操作入口。
 */
export function useOverviewSnapshot(options: OverviewSnapshotOptions = {}): OverviewSnapshotState {
  const view = useMsgReadOnlyView<Overview>({
    loader: async () => ({ items: [await fetchOverview()], total: 1 }),
    permissionCodes: options.permissionCodes,
    autoLoad: false,
  })
  const overview = computed(() => view.rows.value[0] ?? null)
  const lastUpdated = ref<Date | null>(null)
  const intervalMs = options.intervalMs ?? OVERVIEW_POLL_INTERVAL_MS
  let timer: ReturnType<typeof setInterval> | undefined
  let disposed = false

  /** 取一次并记录成功时刻（失败保留上次成功值）。 */
  async function refresh(): Promise<void> {
    await view.refresh()
    if (view.state.value === 'ready') {
      lastUpdated.value = new Date()
    }
  }

  /** 停止轮询（幂等）。 */
  function stop(): void {
    if (timer !== undefined) {
      clearInterval(timer)
      timer = undefined
    }
  }

  /** 开始轮询并立即取一次（无权限 / 已释放时不启动）。 */
  function start(): void {
    if (disposed || timer !== undefined || view.state.value === 'denied') {
      return
    }
    void refresh()
    timer = setInterval(() => {
      void refresh()
    }, intervalMs)
  }

  /** 停止并释放（幂等）。 */
  function dispose(): void {
    if (disposed) {
      return
    }
    disposed = true
    stop()
    view.dispose()
  }

  if (getCurrentScope() !== undefined) {
    onScopeDispose(dispose)
  }
  if (options.autoStart !== false) {
    start()
  }

  return {
    overview,
    state: view.state,
    textKey: view.textKey,
    firstLoad: view.firstLoad,
    lastUpdated,
    refresh,
    stop,
    start,
    dispose,
  }
}
