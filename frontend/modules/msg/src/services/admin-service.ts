/**
 * 管理模块只读取数（产品域寻址，经宿主注入能力，不自建 HTTP、不自拼前缀）。
 *
 * 请求 / 响应契约类型一律取自 `@msg/api-types` 生成物（唯一事实源 `deploy/contracts/msg.json`）；
 * 本模块不手写契约类型，不接收 / 拼装租户参数。
 */

import type { msg } from '@msg/api-types'

import type { ReadOnlyQueryParams } from '../base/BaseMsgReadOnlyView'
import type { ReadOnlyLoader } from '../composables/useMsgReadOnlyView'
import { delData, getData, patchData, postData, requiredData } from './product-api'

export { MsgQueryError, MSG_NAMESPACE, MSG_PRODUCT, toParams } from './product-api'

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
/** 连接列表响应。 */
export type ConnectionPage = Schema['ConnectionPage']
/** 连接列表行。 */
export type ConnectionItem = Schema['ConnectionItem']

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
/** 连接列表查询参数。 */
export type ConnectionQuery = NonNullable<Ops['list_connections_api_v1_msg_admin_connections_get']['parameters']['query']>

/** 业务码错误由 `product-api` 统一提供（此处仅重新导出，供既有引用）。 */

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



/** 平台概览四值。 */
export async function fetchOverview(): Promise<Overview> {
  const data = await getData<Schema['Overview']>('/admin/overview')
  return data ?? EMPTY_OVERVIEW
}

/** 主题列表。 */
export async function fetchTopics(params: TopicQuery = {}): Promise<TopicPage> {
  const data = await getData<Schema['TopicPage']>('/admin/topics', params)
  return data ?? { items: [], page: 1, size: 20, total: 0 }
}

/**
 * 主题详情（含分区列表）。
 *
 * @param topicId 主题主键（雪花，JSON 字符串）。
 */
export async function fetchTopicDetail(topicId: string): Promise<TopicDetail | null> {
  const data = await getData<Schema['TopicDetail']>(`/admin/topics/${topicId}`)
  return data ?? null
}

/** 消费组列表。 */
export async function fetchGroups(params: GroupQuery = {}): Promise<GroupPage> {
  const data = await getData<Schema['GroupPage']>('/admin/groups', params)
  return data ?? { items: [], page: 1, size: 20, total: 0 }
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
  const data = await getData<Schema['GroupDetail']>(path)
  return data ?? null
}

/** 分区详情（必填命名空间与主题）。 */
export async function fetchPartitions(params: PartitionQuery): Promise<PartitionPage | null> {
  const data = await getData<Schema['PartitionPage']>('/admin/partitions', params)
  return data ?? null
}

/** 消息检索（只返回头与摘要，不含正文）。 */
export async function fetchMessages(params: MessageQuery): Promise<MessagePage | null> {
  const data = await getData<Schema['MessagePage']>('/admin/messages', params)
  return data ?? null
}

/** 延迟待投列表。 */
export async function fetchDelayed(params: DelayedQuery = {}): Promise<DelayedPage> {
  const data = await getData<Schema['DelayedPage']>('/admin/delayed', params)
  return data ?? { items: [], page: 1, size: 20, total: 0 }
}

/** 延迟档位分布与堆积提示。 */
export async function fetchDelaySummary(params: DelaySummaryQuery = {}): Promise<DelaySummary> {
  const data = await getData<Schema['DelaySummary']>('/admin/delayed/summary', params)
  return data ?? EMPTY_DELAY_SUMMARY
}

/** 连接列表（单实例视图；地址已脱敏）。 */
export async function fetchConnections(params: ConnectionQuery = {}): Promise<ConnectionPage> {
  const data = await getData<Schema['ConnectionPage']>('/admin/connections', params)
  return data ?? { items: [], page: 1, size: 20, total: 0 }
}

/** 连接列表投影取数器。 */
export const loadConnections: ReadOnlyLoader<ConnectionItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchConnections(params as ConnectionQuery)
  return { items: page.items, total: page.total }
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

/** 删除受理结果。 */
export type TopicCleanupView = Schema['TopicCleanupView']
/** 影响预检五项。 */
export type DeleteImpact = Schema['DeleteImpact']
/** 位点重置结果。 */
export type OffsetResetResult = Schema['OffsetResetResult']
/** 新建主题请求体。 */
export type TopicCreateBody = Schema['TopicCreateRequest']
/** 保留期调整请求体。 */
export type RetentionPatch = Schema['TopicRetentionUpdate']
/** 位点重置请求体。 */
export type OffsetResetBody = Schema['OffsetResetRequest']

/** 新建主题（返回完整元数据）。 */
export async function createTopic(body: TopicCreateBody): Promise<TopicItem> {
  const data = await postData<Schema['TopicItem']>('/admin/topics', body)
  return requiredData(data)
}

/**
 * 删主题影响预检（五项影响范围；单项失败由后端记 0）。
 *
 * @param topicId 主题主键。
 */
export async function fetchDeleteImpact(topicId: string): Promise<DeleteImpact> {
  const data = await getData<Schema['DeleteImpact']>(`/admin/topics/${topicId}/delete-impact`)
  return (
    data ?? {
      partition_count: 0,
      message_total: 0,
      group_count: 0,
      delayed_total: 0,
      dlq_total: 0,
    }
  )
}

/**
 * 删主题（受理即返回：`confirm` 必须显式传 true）。
 *
 * @param topicId 主题主键。
 */
export async function deleteTopic(topicId: string): Promise<TopicCleanupView> {
  const data = await delData<Schema['TopicCleanupView']>(`/admin/topics/${topicId}`, { confirm: true })
  return requiredData(data)
}

/**
 * 停用主题（只停数据面）。
 *
 * @param topicId 主题主键。
 * @returns 更新后的主题。
 */
export async function disableTopic(topicId: string): Promise<TopicItem> {
  const data = await postData<Schema['TopicItem']>(`/admin/topics/${topicId}/disable`, {})
  return requiredData(data)
}

/**
 * 启用主题。
 *
 * @param topicId 主题主键。
 * @returns 更新后的主题。
 */
export async function enableTopic(topicId: string): Promise<TopicItem> {
  const data = await postData<Schema['TopicItem']>(`/admin/topics/${topicId}/enable`, {})
  return requiredData(data)
}

/**
 * 删除一条消息（墓碑标记）。
 *
 * @param logId 记录 ID。
 * @returns 删除结果。
 */
export async function deleteMessage(logId: string): Promise<Schema['MessageDeleteResult']> {
  const data = await postData<Schema['MessageDeleteResult']>(
    `/admin/messages/${encodeURIComponent(logId)}/delete`,
    {},
  )
  return requiredData(data)
}

/**
 * 扩分区（只增不减；后端复核 `350009`）。
 *
 * @param topicId 主题主键。
 * @param partitionCount 目标分区数。
 */
export async function expandPartitions(topicId: string, partitionCount: number): Promise<TopicDetail> {
  const data = await postData<Schema['TopicDetail']>(
    `/admin/topics/${topicId}/partitions`,
    {
      partition_count: partitionCount,
    } satisfies Schema['TopicPartitionExpandRequest'],
  )
  return requiredData(data)
}

/**
 * 改保留期 / 容量上限（部分更新，至少一项）。
 *
 * @param topicId 主题主键。
 * @param patch 变更项。
 */
export async function updateRetention(topicId: string, patch: RetentionPatch): Promise<TopicItem> {
  const data = await patchData<Schema['TopicItem']>(`/admin/topics/${topicId}`, patch)
  return requiredData(data)
}

/**
 * 位点重置（`confirm` 由调用方在确认回调里显式置 true）。
 *
 * @param body 重置请求。
 */
export async function resetOffset(body: OffsetResetBody): Promise<OffsetResetResult> {
  const data = await postData<Schema['OffsetResetResult']>('/admin/offsets/reset', body)
  return requiredData(data)
}

/** 死信列表响应。 */
export type DlqPage = Schema['DlqList']
/** 死信列表行。 */
export type DlqItem = Schema['DlqItem']
/** 死信详情。 */
export type DlqDetail = Schema['DlqDetail']
/** 死信处置结果。 */
export type DlqDisposeResult = Schema['DlqDisposeResult']
/** 死信列表查询参数。 */
export type DlqQuery = NonNullable<Ops['list_dead_letters_api_v1_msg_admin_dlq_get']['parameters']['query']>

/** 死信列表（默认排除已归档）。 */
export async function fetchDeadLetters(params: DlqQuery = {}): Promise<DlqPage> {
  const data = await getData<Schema['DlqList']>('/admin/dlq', params)
  return data ?? { items: [], page: 1, size: 20, total: 0 }
}

/**
 * 死信详情（含失败上下文）。
 *
 * @param dlqId 死信主键（雪花，JSON 字符串）。
 */
export async function fetchDeadLetter(dlqId: string): Promise<DlqDetail | null> {
  const data = await getData<Schema['DlqDetail']>(`/admin/dlq/${dlqId}`)
  return data ?? null
}

/**
 * 重放死信（按原分区 / offset 建单条重放任务）。
 *
 * @param dlqId 死信主键。
 * @param ratePerSecond 重放速率（条/秒）。
 */
export async function replayDeadLetter(dlqId: string, ratePerSecond: number): Promise<DlqDisposeResult> {
  const data = await postData<Schema['DlqDisposeResult']>(`/admin/dlq/${dlqId}/replay`, {
    confirm: true,
    rate_per_second: ratePerSecond,
  } satisfies Schema['DlqReplayRequest'])
  return requiredData(data)
}

/**
 * 忽略死信（原因必填）。
 *
 * @param dlqId 死信主键。
 * @param reason 忽略原因。
 */
export async function ignoreDeadLetter(dlqId: string, reason: string): Promise<DlqDisposeResult> {
  const data = await postData<Schema['DlqDisposeResult']>(`/admin/dlq/${dlqId}/ignore`, {
    confirm: true,
    reason,
  } satisfies Schema['DlqIgnoreRequest'])
  return requiredData(data)
}

/**
 * 归档死信（从活跃列表隐藏）。
 *
 * @param dlqId 死信主键。
 * @param reason 归档原因（可选）。
 */
export async function archiveDeadLetter(dlqId: string, reason?: string): Promise<DlqDisposeResult> {
  const body: Schema['DlqArchiveRequest'] = { confirm: true }
  if (reason !== undefined && reason !== '') {
    body.reason = reason
  }
  const data = await postData<Schema['DlqDisposeResult']>(`/admin/dlq/${dlqId}/archive`, body)
  return requiredData(data)
}

/** 死信列表投影取数器。 */
export const loadDeadLetters: ReadOnlyLoader<DlqItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchDeadLetters(params as DlqQuery)
  return { items: page.items, total: page.total }
}

/** 重放任务列表响应。 */
export type ReplayTaskPage = Schema['ReplayTaskPage']
/** 重放任务行 / 详情。 */
export type ReplayTaskItem = Schema['ReplayTaskItem']
/** 受影响消费组影响视图。 */
export type AffectedGroup = Schema['AffectedGroupItem']
/** 建重放任务请求体。 */
export type ReplayCreateBody = Schema['ReplayRequest']
/** 重放任务列表查询参数。 */
export type ReplayQuery = NonNullable<Ops['list_replay_api_v1_msg_admin_replay_get']['parameters']['query']>

/** 重放任务列表（创建时间倒序；`total` 为 ACL 过滤前计数）。 */
export async function fetchReplayTasks(params: ReplayQuery = {}): Promise<ReplayTaskPage> {
  const data = await getData<Schema['ReplayTaskPage']>('/admin/replay', params)
  return data ?? { items: [], page: 1, size: 20, total: 0 }
}

/**
 * 重放任务详情（终态且无快照时后端顺带落一次快照）。
 *
 * @param taskId 任务主键（雪花，JSON 字符串）。
 */
export async function fetchReplayTask(taskId: string): Promise<ReplayTaskItem | null> {
  const data = await getData<Schema['ReplayTaskItem']>(`/admin/replay/${taskId}`)
  return data ?? null
}

/**
 * 建重放任务（`confirm` 恒 true：批量重放属高危写操作，只由确认回调触发）。
 *
 * @param body 条件与速率。
 */
export async function createReplayTask(body: Omit<ReplayCreateBody, 'confirm'>): Promise<ReplayTaskItem> {
  const data = await postData<Schema['ReplayTaskItem']>('/admin/replay', { ...body, confirm: true })
  return requiredData(data)
}

/**
 * 暂停任务（批边界生效）。
 *
 * @param taskId 任务主键。
 */
export async function pauseReplayTask(taskId: string): Promise<ReplayTaskItem> {
  const data = await postData<Schema['ReplayTaskItem']>(`/admin/replay/${taskId}/pause`, {})
  return requiredData(data)
}

/**
 * 恢复任务（仅接受已暂停）。
 *
 * @param taskId 任务主键。
 */
export async function resumeReplayTask(taskId: string): Promise<ReplayTaskItem> {
  const data = await postData<Schema['ReplayTaskItem']>(`/admin/replay/${taskId}/resume`, {})
  return requiredData(data)
}

/**
 * 取消任务（已写入记录不回滚）。
 *
 * @param taskId 任务主键。
 */
export async function cancelReplayTask(taskId: string): Promise<ReplayTaskItem> {
  const data = await postData<Schema['ReplayTaskItem']>(`/admin/replay/${taskId}/cancel`, {})
  return requiredData(data)
}

/** 重放任务列表投影取数器。 */
export const loadReplayTasks: ReadOnlyLoader<ReplayTaskItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchReplayTasks(params as ReplayQuery)
  return { items: page.items, total: page.total }
}

/** 预估总量的入参（缺省字段不参与过滤；`partition` 只支持单分区）。 */
export interface ReplayEstimateParams {
  /** 命名空间。 */
  namespace: string
  /** 主题名。 */
  topic: string
  /** 分区号（多分区时不传，改为「以创建时计数为准」）。 */
  partition?: number
  /** 起始 offset（含）。 */
  offset_from?: number
  /** 结束 offset（不含）。 */
  offset_to?: number
  /** 分区键精确值。 */
  key?: string
  /** 起始发布时间（含，UTC）。 */
  time_from?: string
  /** 结束发布时间（不含，UTC）。 */
  time_to?: string
}

/**
 * 预估重放总量（复用消息检索的 `total`，只取 1 条）。
 *
 * 估算口径与创建时的精确计数可能不同：只用于确认前给量级，失败一律降级（返回 `null`）。
 *
 * @param params 与创建条件同名的检索参数。
 * @returns 预估条数；取不到返回 `null`。
 */
export async function estimateReplayTotal(params: ReplayEstimateParams): Promise<number | null> {
  try {
    const page = await fetchMessages({ ...params, page: 1, size: 1 } as MessageQuery)
    return page?.total ?? null
  } catch {
    return null
  }
}

/**
 * 取主题下订阅组名（确认弹窗的目标消费组预览）。
 *
 * @param namespace 命名空间。
 * @param topic 主题名。
 * @returns 组名列表；取不到返回 `null`（与「确实没有订阅组」区分开）。
 */
export async function fetchTopicGroups(namespace: string, topic: string): Promise<readonly string[] | null> {
  try {
    const page = await fetchGroups({ namespace, topic, page: 1, size: 100 })
    return page.items.map((item) => item.group)
  } catch {
    return null
  }
}
