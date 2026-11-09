// kiwi_id: 2464
/** 投影：加载 / 空 / 错误 / 竞态 / 无权限 / 待选条件 / 翻页筛选 / 释放。 */

import { effectScope } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { useMsgReadOnlyView, type ReadOnlyLoader } from '../src/composables/useMsgReadOnlyView'
import type { ReadOnlyQueryParams } from '../src/base/BaseMsgReadOnlyView'

/** 等待投影内部的异步取数结算完成。 */
async function flush(): Promise<void> {
  await Promise.resolve()
  await Promise.resolve()
  await Promise.resolve()
}

describe('useMsgReadOnlyView（Kiwi 2464）', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  it('自动取一次：成功结算 ready，行 / 总数 / 文案键同步', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>().mockResolvedValue({ items: ['a', 'b'], total: 5 })
    const view = useMsgReadOnlyView({ loader, initialFilters: { namespace: 'bms' } })
    await flush()
    expect(loader).toHaveBeenCalledWith({ namespace: 'bms', page: 1, size: 20 })
    expect(view.state.value).toBe('ready')
    expect(view.rows.value).toEqual(['a', 'b'])
    expect(view.total.value).toBe(5)
    expect(view.firstLoad.value).toBe(false)
    expect(view.textKey.value).toBe('')
    view.dispose()
  })

  it('空页结算 empty，文案键为 state.empty', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>().mockResolvedValue({ items: [], total: 0 })
    const view = useMsgReadOnlyView({ loader })
    await flush()
    expect(view.state.value).toBe('empty')
    expect(view.textKey.value).toBe('state.empty')
    view.dispose()
  })

  it('取数抛业务码：结算 error 并按码映射文案键', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>().mockRejectedValue({ code: 350001 })
    const view = useMsgReadOnlyView({ loader })
    await flush()
    expect(view.state.value).toBe('error')
    expect(view.textKey.value).toBe('error.350001')
    view.dispose()
  })

  it('取数抛无码异常：回落兜底文案', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>().mockRejectedValue(new Error('boom'))
    const view = useMsgReadOnlyView({ loader })
    await flush()
    expect(view.textKey.value).toBe('error.fallback')
    view.dispose()
  })

  it('无权限：不取数、状态 denied，refresh 与 setActive 都保持拒绝', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>()
    const view = useMsgReadOnlyView({ loader, permissionCodes: [] })
    await flush()
    expect(loader).not.toHaveBeenCalled()
    expect(view.state.value).toBe('denied')
    await view.refresh()
    view.setActive(true)
    expect(loader).not.toHaveBeenCalled()
    expect(view.textKey.value).toBe('state.denied')
    view.dispose()
  })

  it('待选条件：初始 choose 且不取数，setActive(true) 后才加载', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>().mockResolvedValue({ items: ['x'], total: 1 })
    const view = useMsgReadOnlyView({ loader, active: false })
    await flush()
    expect(loader).not.toHaveBeenCalled()
    expect(view.state.value).toBe('choose')
    expect(view.textKey.value).toBe('state.choose')
    view.setActive(true)
    await flush()
    expect(loader).toHaveBeenCalledTimes(1)
    expect(view.state.value).toBe('ready')
    view.setActive(false)
    expect(view.state.value).toBe('choose')
    view.dispose()
  })

  it('autoLoad=false：不自动取数，手动 refresh 生效', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>().mockResolvedValue({ items: [], total: 0 })
    const view = useMsgReadOnlyView({ loader, autoLoad: false })
    await flush()
    expect(loader).not.toHaveBeenCalled()
    await view.refresh()
    expect(loader).toHaveBeenCalledTimes(1)
    expect(view.state.value).toBe('empty')
    view.dispose()
  })

  it('翻页 / 设置筛选 / 重置：参数与页码随链上基类变化', async () => {
    const seen: ReadOnlyQueryParams[] = []
    const loader = vi.fn<ReadOnlyLoader<string>>().mockImplementation(async (params) => {
      seen.push({ ...params })
      return { items: ['row'], total: 1 }
    })
    const view = useMsgReadOnlyView({ loader })
    await flush()
    view.setPage(3)
    await flush()
    view.setFilter('namespace', 'bms')
    await flush()
    view.setFilter('namespace', '')
    await flush()
    view.reset()
    await flush()
    expect(seen[0]).toEqual({ page: 1, size: 20 })
    expect(seen[1]).toEqual({ page: 3, size: 20 })
    expect(seen[2]).toEqual({ namespace: 'bms', page: 1, size: 20 })
    expect(view.page.value).toBe(1)
    expect(view.filters.value).toEqual({})
    view.dispose()
  })

  it('竞态：过期请求结果不覆盖最新一次', async () => {
    let finishFirst: ((value: { items: string[]; total: number }) => void) | undefined
    const first = new Promise<{ items: string[]; total: number }>((resolve) => {
      finishFirst = resolve
    })
    const loader = vi
      .fn<ReadOnlyLoader<string>>()
      .mockReturnValueOnce(first)
      .mockResolvedValueOnce({ items: ['latest'], total: 9 })
    const view = useMsgReadOnlyView({ loader })
    await flush()
    await view.refresh()
    finishFirst?.({ items: ['stale'], total: 1 })
    await flush()
    expect(view.rows.value).toEqual(['latest'])
    expect(view.total.value).toBe(9)
    view.dispose()
  })

  it('竞态：过期失败不覆盖最新成功结果', async () => {
    let rejectFirst: ((reason: unknown) => void) | undefined
    const first = new Promise<{ items: string[]; total: number }>((_resolve, reject) => {
      rejectFirst = reject
    })
    const loader = vi
      .fn<ReadOnlyLoader<string>>()
      .mockReturnValueOnce(first)
      .mockResolvedValueOnce({ items: ['ok'], total: 1 })
    const view = useMsgReadOnlyView({ loader })
    await flush()
    await view.refresh()
    rejectFirst?.({ code: 350009 })
    await flush()
    expect(view.state.value).toBe('ready')
    view.dispose()
  })

  it('dispose 幂等；effectScope 卸载自动释放', async () => {
    const loader = vi.fn<ReadOnlyLoader<string>>().mockResolvedValue({ items: [], total: 0 })
    const scope = effectScope()
    let view: ReturnType<typeof useMsgReadOnlyView<string>> | undefined
    scope.run(() => {
      view = useMsgReadOnlyView({ loader })
    })
    await flush()
    view?.dispose()
    view?.dispose()
    scope.stop()
    expect(view?.dispose).toBeTypeOf('function')
  })
})
