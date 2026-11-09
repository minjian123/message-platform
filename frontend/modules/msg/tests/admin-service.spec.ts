// kiwi_id: 2464
/** 只读取数层：产品域通道 / 路径与参数 / 业务码解包 / 空数据兜底 / 投影取数器。 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { scopeGet } = vi.hoisted(() => ({ scopeGet: vi.fn() }))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    requiredHostApi: () => ({ product: () => ({ get: scopeGet }) }),
  }
})

import {
  EMPTY_DELAY_SUMMARY,
  EMPTY_OVERVIEW,
  MSG_NAMESPACE,
  MSG_PRODUCT,
  MsgQueryError,
  fetchDelaySummary,
  fetchDelayed,
  fetchGroupDetail,
  fetchGroups,
  fetchMessages,
  fetchOverview,
  fetchPartitions,
  fetchTopicDetail,
  fetchTopics,
  loadDelayed,
  loadGroups,
  loadMessages,
  loadPartitions,
  loadTopics,
  toParams,
} from '../src/services/admin-service'
import { fetchQuotas } from '../src/services/quota-service'

/** 让宿主请求能力返回一份成功响应。 */
function respond(data: unknown, code = 0): void {
  scopeGet.mockResolvedValue({ code, message: 'ok', data })
}

const OVERVIEW = {
  consumer_lag: 12,
  disk_used_ratio: 0.63,
  dlq_total: 3,
  messages_in_per_second: 1280,
  snapshot_available: true,
}

describe('admin-service（Kiwi 2464）', () => {
  beforeEach(() => {
    scopeGet.mockReset()
  })

  it('产品键与命名空间同源，且路径不带 /api 前缀、不自拼租户参数', async () => {
    respond(OVERVIEW)
    await fetchOverview()
    expect(MSG_PRODUCT).toBe('msg')
    expect(MSG_NAMESPACE).toBe('msg')
    expect(scopeGet).toHaveBeenCalledWith('/admin/overview', {})
  })

  it('toParams 清理空值并保留 0 / false', () => {
    expect(toParams(undefined)).toEqual({})
    expect(toParams({ namespace: 'bms', topic: '', partition: undefined, level: null, page: 0, flag: false })).toEqual({
      namespace: 'bms',
      page: 0,
      flag: false,
    })
  })

  it('业务码非 0 抛 MsgQueryError', async () => {
    respond(null, 350001)
    await expect(fetchOverview()).rejects.toBeInstanceOf(MsgQueryError)
    await expect(fetchOverview()).rejects.toMatchObject({ code: 350001 })
  })

  it('概览：正常快照与 data 为空时回落占位', async () => {
    respond(OVERVIEW)
    await expect(fetchOverview()).resolves.toEqual(OVERVIEW)
    respond(null)
    await expect(fetchOverview()).resolves.toEqual(EMPTY_OVERVIEW)
  })

  it('主题列表 / 详情：参数透传、路径含主键、空数据兜底', async () => {
    respond({ items: [{ id: '1', name: 'order.created' }], page: 1, size: 20, total: 1 })
    await fetchTopics({ namespace: 'bms', keyword: 'order', page: 2, size: 10 })
    expect(scopeGet).toHaveBeenCalledWith('/admin/topics', { namespace: 'bms', keyword: 'order', page: 2, size: 10 })

    respond(null)
    await expect(fetchTopics()).resolves.toEqual({ items: [], page: 1, size: 20, total: 0 })

    respond({ id: '42', name: 'order.created' })
    await fetchTopicDetail('42')
    expect(scopeGet).toHaveBeenLastCalledWith('/admin/topics/42', {})
    respond(null)
    await expect(fetchTopicDetail('42')).resolves.toBeNull()
  })

  it('消费组列表 / 详情：详情路径逐段编码、空数据兜底', async () => {
    respond({ items: [], page: 1, size: 20, total: 0 })
    await fetchGroups({ namespace: 'bms', topic: 'order.created' })
    expect(scopeGet).toHaveBeenCalledWith('/admin/groups', { namespace: 'bms', topic: 'order.created' })
    respond(null)
    await expect(fetchGroups()).resolves.toEqual({ items: [], page: 1, size: 20, total: 0 })

    respond({ group: 'sync/1', members: [], partitions: [] })
    await fetchGroupDetail('bms', 'order.created', 'sync/1')
    expect(scopeGet).toHaveBeenLastCalledWith('/admin/groups/bms/order.created/sync%2F1', {})
    respond(null)
    await expect(fetchGroupDetail('bms', 'order.created', 'sync/1')).resolves.toBeNull()
  })

  it('分区 / 消息 / 延迟列表 / 延迟汇总：必填参数与空数据兜底', async () => {
    respond({ items: [], namespace: 'bms', topic: 'order.created', partition_count: 2, group: null })
    await fetchPartitions({ namespace: 'bms', topic: 'order.created' })
    expect(scopeGet).toHaveBeenCalledWith('/admin/partitions', { namespace: 'bms', topic: 'order.created' })
    respond(null)
    await expect(fetchPartitions({ namespace: 'bms', topic: 'order.created' })).resolves.toBeNull()

    respond({ items: [], namespace: 'bms', topic: 'order.created', page: 1, size: 20, total: 0 })
    await fetchMessages({ namespace: 'bms', topic: 'order.created', log_id: 'abc' })
    expect(scopeGet).toHaveBeenCalledWith('/admin/messages', { namespace: 'bms', topic: 'order.created', log_id: 'abc' })
    respond(null)
    await expect(fetchMessages({ namespace: 'bms', topic: 'order.created' })).resolves.toBeNull()

    respond({ items: [], page: 1, size: 20, total: 0 })
    await fetchDelayed({ level: '1s', page: 1 })
    expect(scopeGet).toHaveBeenCalledWith('/admin/delayed', { level: '1s', page: 1 })
    respond(null)
    await expect(fetchDelayed()).resolves.toEqual({ items: [], page: 1, size: 20, total: 0 })

    respond({ hint: 'stale', levels: [], namespace: null, quota_used_ratio: 0.9, stale_count: 2, topic: null, total: 2 })
    await fetchDelaySummary({ namespace: 'bms' })
    expect(scopeGet).toHaveBeenCalledWith('/admin/delayed/summary', { namespace: 'bms' })
    respond(null)
    await expect(fetchDelaySummary()).resolves.toEqual(EMPTY_DELAY_SUMMARY)
  })

  it('投影取数器：五个列表把分页响应转成 items + total，空响应回落空页', async () => {
    const params = { namespace: 'bms', topic: 'order.created', page: 1, size: 20 }
    respond({ items: [{ id: '1' }], page: 1, size: 20, total: 1 })
    await expect(loadTopics(params)).resolves.toEqual({ items: [{ id: '1' }], total: 1 })
    respond({ items: [], page: 1, size: 20, total: 0 })
    await expect(loadGroups(params)).resolves.toEqual({ items: [], total: 0 })
    respond({ items: [], page: 1, size: 20, total: 0 })
    await expect(loadDelayed(params)).resolves.toEqual({ items: [], total: 0 })

    respond(null)
    await expect(loadMessages(params)).resolves.toEqual({ items: [], total: 0 })
    await expect(loadPartitions(params)).resolves.toEqual({ items: [], total: 0 })
    await expect(loadTopics(params)).resolves.toEqual({ items: [], total: 0 })
  })

  it('存量配额取数：生成类型迁移后路径 / 参数 / 空 data 兜底不变', async () => {
    const view = { items: [{ quota_key: 'topic_count' }] }
    respond(view)
    await expect(fetchQuotas('bms')).resolves.toEqual(view)
    expect(scopeGet).toHaveBeenCalledWith('/admin/quotas', { namespace: 'bms' })
    respond(null)
    await expect(fetchQuotas()).resolves.toEqual({ items: [] })
    expect(scopeGet).toHaveBeenLastCalledWith('/admin/quotas', undefined)
  })

  it('主题列表响应缺 items 时回落空数组', async () => {
    respond({ page: 1, size: 20, total: 0 })
    await expect(loadTopics({})).resolves.toEqual({ items: [], total: 0 })
  })

  it('投影取数器：消息与分区有数据时按行组装', async () => {
    respond({ items: [{ log_id: 'a' }], page: 1, size: 20, total: 3 })
    await expect(loadMessages({ namespace: 'bms', topic: 't' })).resolves.toEqual({ items: [{ log_id: 'a' }], total: 3 })
    respond({ items: [{ partition: 0 }, { partition: 1 }], namespace: 'bms', topic: 't', partition_count: 2, group: null })
    await expect(loadPartitions({ namespace: 'bms', topic: 't' })).resolves.toEqual({
      items: [{ partition: 0 }, { partition: 1 }],
      total: 2,
    })
  })
})
