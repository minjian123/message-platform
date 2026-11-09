/**
 * `BaseMsgReadOnlyView` 的 Vue 投影（薄适配，不是可继承类、不入基类清单）。
 *
 * 只做「核心实例 ↔ 框架响应式」与加载编排；四态、错误码映射、竞态、筛选合法性由基类决定，
 * 页面一律经本投影消费，不直接实例化基类。
 */

import { getCurrentScope, onScopeDispose, ref, shallowRef, type Ref } from 'vue'

import {
  BaseMsgReadOnlyView,
  errorCodeOf,
  stateKeyOf,
  type ReadOnlyFilters,
  type ReadOnlyQueryParams,
  type ReadOnlyViewState,
} from '../base/BaseMsgReadOnlyView'
import { hostPermissions } from '../runtime'

/** 当页结果。 */
export interface ReadOnlyPage<T> {
  /** 当页行。 */
  items: readonly T[]
  /** 过滤后总数。 */
  total: number
}

/** 取数函数（由 `services/admin-service.ts` 提供）。 */
export type ReadOnlyLoader<T> = (params: ReadOnlyQueryParams) => Promise<ReadOnlyPage<T>>

/** 投影选项。 */
export interface ReadOnlyViewOptions<T> {
  /** 取数函数。 */
  loader: ReadOnlyLoader<T>
  /** 初始筛选条件。 */
  initialFilters?: ReadOnlyFilters
  /** 已持有权限码；缺省取宿主注入快照（显式传 `[]` 可模拟无权限）。 */
  permissionCodes?: readonly string[] | undefined
  /** 是否具备发起查询的前置条件（分区页须先选主题）。 */
  active?: boolean
  /** 创建时是否自动取一次（缺省 true；轮询 / 条件页显式关闭）。 */
  autoLoad?: boolean
}

/** 投影状态。 */
export interface ReadOnlyViewStateRef<T> {
  /** 当页行。 */
  rows: Ref<T[]>
  /** 过滤后总数。 */
  total: Ref<number>
  /** 当前页。 */
  page: Ref<number>
  /** 每页条数。 */
  pageSize: Ref<number>
  /** 展示态。 */
  state: Ref<ReadOnlyViewState>
  /** 当前展示态文案键。 */
  textKey: Ref<string>
  /** 筛选条件快照。 */
  filters: Ref<ReadOnlyFilters>
  /** 是否正在首次加载（用于区分骨架与轮询刷新）。 */
  firstLoad: Ref<boolean>
  /** 重新取数。 */
  refresh: () => Promise<void>
  /** 翻页。 */
  setPage: (page: number) => void
  /** 设置筛选条件并回到第一页。 */
  setFilter: (key: string, value: string | number | undefined) => void
  /** 清空筛选条件并回到第一页。 */
  reset: () => void
  /** 设置前置条件（不具备时展示「待选条件」且不发请求）。 */
  setActive: (active: boolean) => void
  /** 取消订阅（幂等；组件卸载自动调用）。 */
  dispose: () => void
}

/**
 * 创建只读视图投影。
 *
 * @param options 投影选项。
 * @returns 响应式状态与操作入口。
 */
export function useMsgReadOnlyView<T>(options: ReadOnlyViewOptions<T>): ReadOnlyViewStateRef<T> {
  const core = new BaseMsgReadOnlyView<T>(options.initialFilters)
  const permissionCodes = options.permissionCodes === undefined ? hostPermissions() : options.permissionCodes
  const denied = !core.allows(permissionCodes)
  const inactive = options.active === false

  const rows = shallowRef<T[]>([])
  const total = ref(0)
  const page = ref(core.page)
  const pageSize = ref(core.pageSize)
  const state = ref<ReadOnlyViewState>(denied ? 'denied' : inactive ? 'choose' : core.state)
  const textKey = ref(stateKeyOf(state.value))
  const filters = shallowRef<ReadOnlyFilters>({ ...core.filters })
  const firstLoad = ref(true)
  let active = !denied && !inactive
  let disposed = false

  /** 把核心实例状态同步到响应式引用。 */
  function sync(): void {
    rows.value = [...core.rows] as T[]
    total.value = core.total
    page.value = core.page
    pageSize.value = core.pageSize
    filters.value = { ...core.filters }
    state.value = denied ? 'denied' : active ? core.state : 'choose'
    textKey.value = core.state === 'error' ? core.errorTextKey() : stateKeyOf(state.value)
  }

  const unsubscribe = core.onStateChange(() => {
    sync()
  })

  /** 重新取数（无权限 / 缺前置条件时不发请求）。 */
  async function refresh(): Promise<void> {
    if (denied) {
      state.value = 'denied'
      textKey.value = stateKeyOf('denied')
      return
    }
    if (!active) {
      state.value = 'choose'
      textKey.value = stateKeyOf('choose')
      return
    }
    const token = core.beginLoad()
    sync()
    try {
      const result = await options.loader(core.queryParams())
      core.settleRows(token, result.items, result.total)
    } catch (error) {
      core.settleFailure(token, errorCodeOf(error))
    }
    firstLoad.value = false
    sync()
  }

  /**
   * 翻页并重新取数。
   *
   * @param next 目标页码。
   */
  function setPage(next: number): void {
    core.setPage(next)
    sync()
    void refresh()
  }

  /**
   * 设置筛选条件并回到第一页。
   *
   * @param key 参数名。
   * @param value 参数值。
   */
  function setFilter(key: string, value: string | number | undefined): void {
    core.setFilter(key, value)
    core.resetToFirstPage()
    sync()
    void refresh()
  }

  /** 清空筛选条件并回到第一页。 */
  function reset(): void {
    core.clearFilters()
    core.resetToFirstPage()
    sync()
    void refresh()
  }

  /**
   * 设置查询前置条件。
   *
   * @param next 是否具备条件。
   */
  function setActive(next: boolean): void {
    active = next && !denied
    sync()
    if (active) {
      void refresh()
    }
  }

  /** 取消订阅（幂等）。 */
  function dispose(): void {
    if (disposed) {
      return
    }
    disposed = true
    unsubscribe()
  }

  if (getCurrentScope() !== undefined) {
    onScopeDispose(dispose)
  }
  if (options.autoLoad !== false && active) {
    void refresh()
  }

  return {
    rows,
    total,
    page,
    pageSize,
    state,
    textKey,
    filters,
    firstLoad,
    refresh,
    setPage,
    setFilter,
    reset,
    setActive,
    dispose,
  }
}
