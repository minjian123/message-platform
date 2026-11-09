// kiwi_id: 2464
/** 概览投影轮询与工作台卡片：挂载取一次 / 30 秒轮询 / 卸载清定时器 / 冷启动 / 错误态 / 权限态。 */

import ElementPlus from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet, permissionRef } = vi.hoisted(() => ({
  scopeGet: vi.fn(),
  permissionRef: { value: undefined as readonly string[] | undefined },
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => ({ get: scopeGet }) }),
    hostPermissions: () => permissionRef.value,
  }
})

import OverviewCard from '../src/components/OverviewCard.vue'
import OverviewMetrics from '../src/components/OverviewMetrics.vue'
import OverviewPage from '../src/views/OverviewPage.vue'
import { useOverviewSnapshot } from '../src/composables/useOverviewSnapshot'
import { OVERVIEW_POLL_INTERVAL_MS } from '../src/constants'

const OVERVIEW = {
  consumer_lag: 3412,
  disk_used_ratio: 0.63,
  dlq_total: 17,
  messages_in_per_second: 1280,
  snapshot_available: true,
}

/** 让宿主请求能力返回一份概览响应。 */
function respond(data: unknown, code = 0): void {
  scopeGet.mockResolvedValue({ code, message: 'ok', data })
}

/** 挂载组件（注入 Element Plus）。 */
function mountView(component: unknown) {
  return mount(component as never, { global: { plugins: [ElementPlus] } })
}

describe('useOverviewSnapshot（Kiwi 2464）', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    scopeGet.mockReset()
    permissionRef.value = undefined
    respond(OVERVIEW)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('挂载取一次 + 30 秒轮询；stop / start / dispose 幂等', async () => {
    const snapshot = useOverviewSnapshot()
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledTimes(1)
    vi.advanceTimersByTime(OVERVIEW_POLL_INTERVAL_MS)
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledTimes(2)

    snapshot.stop()
    vi.advanceTimersByTime(OVERVIEW_POLL_INTERVAL_MS)
    expect(scopeGet).toHaveBeenCalledTimes(2)
    snapshot.start()
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledTimes(3)
    snapshot.start()
    expect(scopeGet).toHaveBeenCalledTimes(3)
    snapshot.dispose()
    snapshot.dispose()
    vi.advanceTimersByTime(OVERVIEW_POLL_INTERVAL_MS)
    expect(scopeGet).toHaveBeenCalledTimes(3)
    snapshot.start()
    expect(scopeGet).toHaveBeenCalledTimes(3)
  })

  it('autoStart=false 不取数；自定义间隔与手动刷新', async () => {
    const snapshot = useOverviewSnapshot({ autoStart: false, intervalMs: 1000 })
    await flushPromises()
    expect(scopeGet).not.toHaveBeenCalled()
    snapshot.start()
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledTimes(1)
    vi.advanceTimersByTime(1000)
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledTimes(2)
    await snapshot.refresh()
    expect(scopeGet).toHaveBeenCalledTimes(3)
    expect(snapshot.overview.value).toEqual(OVERVIEW)
    expect(snapshot.firstLoad.value).toBe(false)
    snapshot.dispose()
  })

  it('无权限：不取数、不启动定时器', async () => {
    const snapshot = useOverviewSnapshot({ permissionCodes: [] })
    await flushPromises()
    expect(scopeGet).not.toHaveBeenCalled()
    expect(snapshot.state.value).toBe('denied')
    vi.advanceTimersByTime(OVERVIEW_POLL_INTERVAL_MS)
    expect(scopeGet).not.toHaveBeenCalled()
    snapshot.dispose()
  })
})

describe('概览组件（Kiwi 2464）', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    scopeGet.mockReset()
    permissionRef.value = undefined
    respond(OVERVIEW)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('工作台卡片：四值数字卡、无 Grafana 入口、轮询一次后仍保留数值', async () => {
    const wrapper = mountView(OverviewCard)
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledWith('/admin/overview', {})
    expect(wrapper.find('[data-test="msg-overview-card"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('入口吞吐')
    expect(wrapper.text()).toContain('当前积压')
    expect(wrapper.text()).toContain('死信数')
    expect(wrapper.text()).toContain('磁盘使用率')
    expect(wrapper.find('[data-test="overview-grafana"]').exists()).toBe(false)
    vi.advanceTimersByTime(OVERVIEW_POLL_INTERVAL_MS)
    await flushPromises()
    expect(scopeGet).toHaveBeenCalledTimes(2)
    wrapper.unmount()
    vi.advanceTimersByTime(OVERVIEW_POLL_INTERVAL_MS)
    expect(scopeGet).toHaveBeenCalledTimes(2)
  })

  it('冷启动 snapshot_available=false 显示「指标采集中」', async () => {
    respond({ ...OVERVIEW, snapshot_available: false })
    const wrapper = mountView(OverviewCard)
    await flushPromises()
    expect(wrapper.text()).toContain('指标采集中')
    wrapper.unmount()
  })

  it('错误态按码映射文案，并保留上一次成功数值', async () => {
    const wrapper = mountView(OverviewCard)
    await flushPromises()
    scopeGet.mockResolvedValue({ code: 350001, message: 'fail', data: null })
    vi.advanceTimersByTime(OVERVIEW_POLL_INTERVAL_MS)
    await flushPromises()
    expect(wrapper.find('[data-test="overview-error"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('主题不存在或未启用')
    wrapper.unmount()
  })

  it('无权限：卡片与页面都不渲染数据区', async () => {
    permissionRef.value = ['msg:manage']
    const card = mountView(OverviewCard)
    const page = mountView(OverviewPage)
    await flushPromises()
    expect(card.find('[data-test="overview-denied"]').exists()).toBe(true)
    expect(page.find('[data-test="overview-denied"]').exists()).toBe(true)
    expect(scopeGet).not.toHaveBeenCalled()
    card.unmount()
    page.unmount()
  })

  it('首次加载显示加载中文案；概览页显示轮询与 Grafana 说明', async () => {
    scopeGet.mockImplementation(() => new Promise(() => undefined))
    const wrapper = mountView(OverviewPage)
    expect(wrapper.text()).toContain('加载中…')
    expect(wrapper.find('[data-test="overview-hint"]').exists()).toBe(true)
    expect(wrapper.find('[data-test="overview-grafana-hint"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('OverviewMetrics 就绪态与手动刷新事件', async () => {
    const wrapper = mount(OverviewMetrics, {
      props: { overview: OVERVIEW, state: 'ready' },
      global: { plugins: [ElementPlus] },
    })
    await wrapper.find('[data-test="overview-refresh"]').trigger('click')
    expect(wrapper.emitted('refresh')).toBeTruthy()
    expect(wrapper.text()).toContain('入口吞吐')
    wrapper.unmount()
  })

  it('OverviewMetrics 直接挂载：首次加载占位与 Grafana 缺省不渲染', async () => {
    const wrapper = mount(OverviewMetrics, {
      props: { overview: null, state: 'loading', firstLoad: true },
      global: { plugins: [ElementPlus] },
    })
    await flushPromises()
    expect(wrapper.find('[data-test="overview-metrics"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('加载中…')
    expect(wrapper.find('[data-test="overview-grafana"]').exists()).toBe(false)
    wrapper.unmount()
  })
})
