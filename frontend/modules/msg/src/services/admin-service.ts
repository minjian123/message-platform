/**
 * 管理模块只读取数（产品域寻址，经宿主注入能力，不自建 HTTP、不自拼前缀）。
 *
 * 请求 / 响应契约类型一律取自 `@msg/api-types` 生成物（唯一事实源 `deploy/contracts/msg.json`）；
 * 本模块不手写契约类型，不接收 / 拼装租户参数。
 */

import type { msg } from '@msg/api-types'
import type { ModuleApiScope } from '@bms/core'

import type { ReadOnlyQueryParams } from '../base/BaseMsgReadOnlyView'
import type { ReadOnlyLoader } from '../composables/useMsgReadOnlyView'
import { requiredHostApi } from '../runtime'

type Schema = msg.components['schemas']
type Ops = msg.operations

/** 主题列表响应。 */
export type TopicPage = Schema['TopicPage']
/** 主题列表行。 */
export type TopicItem = Schema['TopicItem']
/** 主题详情。 */
export type TopicDetail = Schema['TopicDetail']
/** 消费组列表响应。 */
export type GroupPage = Schema['GroupPage']
/** 消费组列表行。 */
export type GroupItem = Schema['GroupItem']
/** 消费组详情。 */
export type GroupDetail = Schema['GroupDetail']
/** 分区详情响应。 */
export type PartitionPage = Schema['PartitionPage']
/** 分区详情行。 */
export type PartitionItem = Schema['bms_msg__schemas__admin_view__PartitionItem']
/** 消息检索响应。 */
export type MessagePage = Schema['MessagePage']
/** 消息头行。 */
export type MessageItem = Schema['bms_msg__schemas__admin_view__MessageItem']
/** 延迟待投列表响应。 */
export type DelayedPage = Schema['DelayedPage']
/** 延迟待投行。 */
export type DelayedItem = Schema['DelayedItem']
/** 延迟汇总。 */
export type DelaySummary = Schema['DelaySummary']
/** 平台概览。 */
export type Overview = Schema['Overview']

/** 主题列表查询参数。 */
export type TopicQuery = NonNullable<Ops['list_topics_api_v1_msg_admin_topics_get']['parameters']['query']>
/** 消费组列表查询参数。 */
export type GroupQuery = NonNullable<Ops['list_groups_api_v1_msg_admin_groups_get']['parameters']['query']>
/** 分区详情查询参数。 */
export type PartitionQuery = Ops['list_partitions_api_v1_msg_admin_partitions_get']['parameters']['query']
/** 消息检索查询参数。 */
export type MessageQuery = Ops['search_messages_api_v1_msg_admin_messages_get']['parameters']['query']
/** 延迟待投查询参数。 */
export type DelayedQuery = NonNullable<Ops['list_delayed_api_v1_msg_admin_delayed_get']['parameters']['query']>
/** 延迟汇总查询参数。 */
export type DelaySummaryQuery = NonNullable<
  Ops['get_delay_summary_api_v1_msg_admin_delayed_summary_get']['parameters']['query']
>

/** 产品键（与平台产品登记同源）。 */
export const MSG_PRODUCT = 'msg'
/** 产品命名空间段。 */
export const MSG_NAMESPACE = 'msg'

/** 业务码非 0 时抛出（由投影捕获并映射模块 i18n 文案键）。 */
export class MsgQueryError extends Error {
  /** 后端业务码。 */
  readonly code: number

  /**
   * @param code 后端业务码。
   */
  constructor(code: number) {
    super(String(code))
    this.name = 'MsgQueryError'
    this.code = code
  }
}

/** 空概览（快照尚未产出时的结构占位，不伪造数值）。 */
export const EMPTY_OVERVIEW: Overview = {
  consumer_lag: 0,
  disk_used_ratio: 0,
  dlq_total: 0,
  messages_in_per_second: 0,
  snapshot_available: false,
}

/** 空延迟汇总。 */
export const EMPTY_DELAY_SUMMARY: DelaySummary = {
  hint: 'normal',
  levels: [],
  namespace: null,
  quota_used_ratio: 0,
  stale_count: 0,
  topic: null,
  total: 0,
}

/**
 * 产品域请求作用域。
 *
 * @returns 请求作用域。
 */
function scope(): ModuleApiScope {
  return requiredHostApi().product(MSG_PRODUCT, MSG_NAMESPACE)
}

/**
 * 清理查询参数（丢掉 `undefined` / `null` / 空串，避免把空筛选发给后端）。
 *
 * @param params 查询参数对象。
 * @returns 宿主请求层可接收的参数表。
 */
export function toParams(params: object | undefined): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  if (params === undefined) {
    return result
  }
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      result[key] = value
    }
  }
  return result
}

/**
 * 统一取数与业务码解包。
 *
 * @param path 相对产品域的资源路径。
 * @param params 查询参数。
 * @returns 统一响应体。
 * @throws MsgQueryError 业务码非 0。
 */
async function request<E extends { code: number }>(path: string, params?: object): Promise<E> {
  const envelope = await scope().get<E>(path, toParams(params))
  if (envelope.code !== 0) {
    throw new MsgQueryError(envelope.code)
  }
  return envelope
}

/** 平台概览四值。 */
export async function fetchOverview(): Promise<Overview> {
  const envelope = await request<Schema['ApiResponse_Overview_']>('/admin/overview')
  return envelope.data ?? EMPTY_OVERVIEW
}

/** 主题列表。 */
export async function fetchTopics(params: TopicQuery = {}): Promise<TopicPage> {
  const envelope = await request<Schema['ApiResponse_TopicPage_']>('/admin/topics', params)
  return envelope.data ?? { items: [], page: 1, size: 20, total: 0 }
}

/**
 * 主题详情（含分区列表）。
 *
 * @param topicId 主题主键（雪花，JSON 字符串）。
 */
export async function fetchTopicDetail(topicId: string): Promise<TopicDetail | null> {
  const envelope = await request<Schema['ApiResponse_TopicDetail_']>(`/admin/topics/${topicId}`)
  return envelope.data ?? null
}

/** 消费组列表。 */
export async function fetchGroups(params: GroupQuery = {}): Promise<GroupPage> {
  const envelope = await request<Schema['ApiResponse_GroupPage_']>('/admin/groups', params)
  return envelope.data ?? { items: [], page: 1, size: 20, total: 0 }
}

/**
 * 消费组详情。
 *
 * @param namespace 命名空间。
 * @param topic 主题名。
 * @param group 组名。
 */
export async function fetchGroupDetail(
  namespace: string,
  topic: string,
  group: string,
): Promise<GroupDetail | null> {
  const path = `/admin/groups/${encodeURIComponent(namespace)}/${encodeURIComponent(topic)}/${encodeURIComponent(group)}`
  const envelope = await request<Schema['ApiResponse_GroupDetail_']>(path)
  return envelope.data ?? null
}

/** 分区详情（必填命名空间与主题）。 */
export async function fetchPartitions(params: PartitionQuery): Promise<PartitionPage | null> {
  const envelope = await request<Schema['ApiResponse_PartitionPage_']>('/admin/partitions', params)
  return envelope.data ?? null
}

/** 消息检索（只返回头与摘要，不含正文）。 */
export async function fetchMessages(params: MessageQuery): Promise<MessagePage | null> {
  const envelope = await request<Schema['ApiResponse_MessagePage_']>('/admin/messages', params)
  return envelope.data ?? null
}

/** 延迟待投列表。 */
export async function fetchDelayed(params: DelayedQuery = {}): Promise<DelayedPage> {
  const envelope = await request<Schema['ApiResponse_DelayedPage_']>('/admin/delayed', params)
  return envelope.data ?? { items: [], page: 1, size: 20, total: 0 }
}

/** 延迟档位分布与堆积提示。 */
export async function fetchDelaySummary(params: DelaySummaryQuery = {}): Promise<DelaySummary> {
  const envelope = await request<Schema['ApiResponse_DelaySummary_']>('/admin/delayed/summary', params)
  return envelope.data ?? EMPTY_DELAY_SUMMARY
}

/** 主题列表投影取数器。 */
export const loadTopics: ReadOnlyLoader<TopicItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchTopics(params as TopicQuery)
  return { items: page.items ?? [], total: page.total }
}

/** 消费组列表投影取数器。 */
export const loadGroups: ReadOnlyLoader<GroupItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchGroups(params as GroupQuery)
  return { items: page.items, total: page.total }
}

/** 消息检索投影取数器。 */
export const loadMessages: ReadOnlyLoader<MessageItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchMessages(params as MessageQuery)
  return { items: page?.items ?? [], total: page?.total ?? 0 }
}

/** 延迟待投投影取数器。 */
export const loadDelayed: ReadOnlyLoader<DelayedItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchDelayed(params as DelayedQuery)
  return { items: page.items, total: page.total }
}

/** 分区详情投影取数器（无分页， Projection 只取当页行）。 */
export const loadPartitions: ReadOnlyLoader<PartitionItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchPartitions(params as PartitionQuery)
  const items = page?.items ?? []
  return { items, total: items.length }
}
