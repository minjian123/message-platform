/**
 * 模块文案包（中英两份由同一份目录派生，键集合天然一致）。
 *
 * 页面不硬编码中文：页面标题、字段、四态、错误码、堆积提示全部按键取值。
 * 错误码键形如 `error.<后端业务码>`，未命中回落 `error.fallback`。
 */

/** 文案目录：键 → [中文, 英文]。 */
const CATALOG: Record<string, readonly [string, string]> = {
  // 05-2 既有「消息配额」文案（原样保留，不改字）。
  'msg.quota.title': ['消息配额', 'Message quota'],
  'msg.quota.key': ['配额维度', 'Dimension'],
  'msg.quota.limit': ['硬限', 'Hard limit'],
  'msg.quota.soft': ['软限', 'Soft limit'],
  'msg.quota.usage': ['用量', 'Usage'],
  'msg.quota.state': ['状态', 'State'],
  'msg.quota.updatedAt': ['更新时间', 'Updated at'],
  'msg.quota.source.default': ['缺省', 'Default'],
  'msg.quota.source.configured': ['已配置', 'Configured'],
  'msg.quota.state.unconfigured': ['未配置', 'Unconfigured'],
  'msg.quota.state.ok': ['正常', 'OK'],
  'msg.quota.state.soft_exceeded': ['接近上限', 'Near limit'],
  'msg.quota.state.hard_exceeded': ['已超限', 'Exceeded'],
  'msg.quota.state.unavailable': ['暂不可用', 'Unavailable'],
  'msg.quota.unlimited': ['不限', 'Unlimited'],
  'msg.quota.unavailable': ['暂不可用：无「消息配额」查询权限或消息平台服务未接入', 'Unavailable: missing query permission or service not connected'],
  'msg.quota.empty': ['该租户暂无消息配额数据', 'No message quota data for this tenant'],
  'msg.quota.topic_count': ['主题数', 'Topics'],
  'msg.quota.group_count': ['消费组数', 'Consumer groups'],
  'msg.quota.produce_rate': ['生产速率', 'Produce rate'],
  'msg.quota.storage_bytes': ['消息存储', 'Storage'],
  'msg.quota.unacked_count': ['未确认数', 'Unacked'],
  'msg.quota.connection_count': ['连接数', 'Connections'],

  // 导航与通用动作。
  'msg.admin.group': ['消息平台', 'Message platform'],
  'msg.admin.nav.overview': ['平台概览', 'Overview'],
  'msg.admin.nav.topics': ['主题管理', 'Topics'],
  'msg.admin.nav.groups': ['消费组与积压', 'Consumer groups'],
  'msg.admin.nav.partitions': ['分区', 'Partitions'],
  'msg.admin.nav.messages': ['消息查询', 'Message search'],
  'msg.admin.nav.delayed': ['延迟消息', 'Delayed messages'],
  'msg.admin.nav.dlq': ['死信与重放', 'Dead letters & replay'],
  'msg.admin.common.refresh': ['重新加载', 'Reload'],
  'msg.admin.common.back': ['返回', 'Back'],
  'msg.admin.common.detail': ['查看详情', 'Details'],
  'msg.admin.common.query': ['查询', 'Query'],
  'msg.admin.common.reset': ['重置', 'Reset'],
  'msg.admin.common.grafana': ['查看 Grafana', 'Open Grafana'],
  'msg.admin.common.placeholder': ['—', '—'],
  'msg.admin.common.later.write': ['写操作归 06-2，本页不提供', 'Write operations belong to 06-2'],
  'msg.admin.common.later.dispose': ['处置交互归 06-5，本页不提供', 'Disposition belongs to 06-5'],
  'msg.admin.common.later.replay': ['重放任务控制归 06-6，本页不提供', 'Replay controls belong to 06-6'],

  // 四态与错误码映射（键集合与后端码同源）。
  'state.loading': ['加载中…', 'Loading…'],
  'state.empty': ['暂无数据', 'No data'],
  'state.error': ['查询失败', 'Query failed'],
  'state.denied': ['无「msg:query」查询权限', 'Missing msg:query permission'],
  'state.choose': ['请先选择查询条件', 'Select query filters first'],
  'state.collecting': ['指标采集中', 'Collecting metrics'],
  'error.350001': ['主题不存在或未启用', 'Topic not found or disabled'],
  'error.350009': ['分区号越界', 'Partition out of range'],
  'error.350010': ['命名空间未登记或未启用', 'Namespace not registered or disabled'],
  'error.350011': ['租户停用窗口：只读请求放行', 'Tenant suspended: read-only requests allowed'],
  'error.350012': ['租户注销窗口：只读请求放行', 'Tenant being purged: read-only requests allowed'],
  'error.350013': ['消费组不存在', 'Consumer group not found'],
  'error.350027': ['无主题描述授权', 'Topic describe permission denied'],
  'error.10001': ['查询参数非法', 'Invalid query parameters'],
  'error.30001': ['无权限执行该操作', 'Permission denied'],
  'error.fallback': ['查询失败，请稍后重试', 'Query failed, please retry later'],
  'hint.normal': ['延迟区运行正常', 'Delay queue healthy'],
  'hint.stale': ['存在已到期未转投消息', 'Overdue messages pending delivery'],
  'hint.near_quota': ['延迟区用量接近上限', 'Delay queue near quota'],

  // 平台概览与工作台卡片。
  'msg.admin.overview.title': ['平台概览', 'Platform overview'],
  'msg.admin.overview.card': ['消息平台概览', 'Message platform overview'],
  'msg.admin.overview.throughput': ['入口吞吐', 'Ingress throughput'],
  'msg.admin.overview.lag': ['当前积压', 'Consumer lag'],
  'msg.admin.overview.dlq': ['死信数', 'Dead letters'],
  'msg.admin.overview.disk': ['磁盘使用率', 'Disk usage'],
  'msg.admin.overview.throughputNote': ['采样周期均值', 'Sampling window average'],
  'msg.admin.overview.lagNote': ['当前租户汇总', 'Current tenant total'],
  'msg.admin.overview.dlqNote': ['待处置', 'Pending'],
  'msg.admin.overview.diskNote': ['多卷取最大值', 'Max across volumes'],
  'msg.admin.overview.polling': ['挂载取一次，每 30 秒刷新', 'Load on mount, refresh every 30s'],
  'msg.admin.overview.grafanaHint': ['趋势图不自绘，跳转 Grafana 看板', 'No local trend charts, open Grafana dashboard'],

  // 主题管理。
  'msg.admin.topics.title': ['主题管理', 'Topic management'],
  'msg.admin.topics.namespace': ['命名空间', 'Namespace'],
  'msg.admin.topics.keyword': ['名称关键字', 'Name keyword'],
  'msg.admin.topics.name': ['主题', 'Topic'],
  'msg.admin.topics.partitionCount': ['分区数', 'Partitions'],
  'msg.admin.topics.retention': ['保留期', 'Retention'],
  'msg.admin.topics.status': ['状态', 'Status'],
  'msg.admin.topics.cleanupPolicy': ['清理策略', 'Cleanup policy'],
  'msg.admin.topics.createdAt': ['创建时间', 'Created at'],
  'msg.admin.topics.detail': ['主题详情', 'Topic details'],
  'msg.admin.topics.partitionDetail': ['分区明细', 'Partition details'],
  'msg.admin.topics.laterCreate': ['新建主题归 06-2，本页只读', 'Create topic belongs to 06-2'],

  // 消费组与积压。
  'msg.admin.groups.title': ['消费组与积压', 'Consumer groups & lag'],
  'msg.admin.groups.namespace': ['命名空间', 'Namespace'],
  'msg.admin.groups.topic': ['主题', 'Topic'],
  'msg.admin.groups.group': ['消费组', 'Group'],
  'msg.admin.groups.memberCount': ['成员数', 'Members'],
  'msg.admin.groups.lag': ['积压', 'Lag'],
  'msg.admin.groups.lastCommit': ['最后提交时间', 'Last commit'],
  'msg.admin.groups.state': ['状态', 'State'],
  'msg.admin.groups.detail': ['消费组详情', 'Group details'],
  'msg.admin.groups.generation': ['世代', 'Generation'],
  'msg.admin.groups.members': ['成员与分配', 'Members & assignments'],
  'msg.admin.groups.member': ['成员', 'Member'],
  'msg.admin.groups.assigned': ['分配分区', 'Assigned partitions'],
  'msg.admin.groups.lastSeen': ['最后心跳', 'Last heartbeat'],
  'msg.admin.groups.laterReset': ['位点重置归 06-2，本页只读', 'Offset reset belongs to 06-2'],

  // 分区（列表列名与主题详情共用）。
  'msg.admin.partitions.title': ['分区', 'Partitions'],
  'msg.admin.partitions.choose': ['先选择命名空间与主题，再查看分区明细', 'Select namespace and topic to inspect partitions'],
  'msg.admin.partitions.partition': ['分区', 'Partition'],
  'msg.admin.partitions.leader': ['Leader', 'Leader'],
  'msg.admin.partitions.leo': ['LEO', 'LEO'],
  'msg.admin.partitions.hw': ['HW', 'HW'],
  'msg.admin.partitions.isr': ['ISR', 'ISR'],
  'msg.admin.partitions.committed': ['已提交位点', 'Committed'],
  'msg.admin.partitions.owner': ['Owner', 'Owner'],

  // 消息查询（只展示头与摘要）。
  'msg.admin.messages.title': ['消息查询', 'Message search'],
  'msg.admin.messages.namespace': ['命名空间', 'Namespace'],
  'msg.admin.messages.topic': ['主题', 'Topic'],
  'msg.admin.messages.partition': ['分区', 'Partition'],
  'msg.admin.messages.offsetFrom': ['起始 offset', 'Offset from'],
  'msg.admin.messages.offsetTo': ['结束 offset', 'Offset to'],
  'msg.admin.messages.key': ['分区键', 'Key'],
  'msg.admin.messages.timeFrom': ['起始时间（UTC）', 'Time from (UTC)'],
  'msg.admin.messages.timeTo': ['结束时间（UTC）', 'Time to (UTC)'],
  'msg.admin.messages.logId': ['log_id', 'log_id'],
  'msg.admin.messages.offset': ['offset', 'offset'],
  'msg.admin.messages.publishedAt': ['发布时间（UTC）', 'Published at (UTC)'],
  'msg.admin.messages.payloadLen': ['载荷长度', 'Payload size'],
  'msg.admin.messages.sha256': ['SHA-256 摘要', 'SHA-256 digest'],
  'msg.admin.messages.headers': ['自定义头', 'Headers'],
  'msg.admin.messages.traceId': ['trace_id', 'trace_id'],
  'msg.admin.messages.messageId': ['message_id', 'message_id'],
  'msg.admin.messages.detail': ['消息头', 'Message header'],
  'msg.admin.messages.noBody': ['只展示头与摘要，不展示消息正文', 'Headers and digest only, message body is not shown'],
  'msg.admin.messages.choose': ['命名空间与主题为必填条件', 'Namespace and topic are required'],

  // 延迟消息。
  'msg.admin.delayed.title': ['延迟消息', 'Delayed messages'],
  'msg.admin.delayed.distribution': ['档位分布（九档）', 'Level distribution (9 levels)'],
  'msg.admin.delayed.level': ['档位', 'Level'],
  'msg.admin.delayed.count': ['条数', 'Count'],
  'msg.admin.delayed.minRemaining': ['最小剩余时间', 'Min remaining'],
  'msg.admin.delayed.total': ['待投总数', 'Pending total'],
  'msg.admin.delayed.staleCount': ['已到期未转投', 'Overdue'],
  'msg.admin.delayed.quotaUsedRatio': ['配额用量', 'Quota usage'],
  'msg.admin.delayed.hint': ['堆积提示', 'Backlog hint'],
  'msg.admin.delayed.deliverAt': ['到期时间', 'Deliver at'],
  'msg.admin.delayed.remainingMs': ['剩余时间', 'Remaining'],
  'msg.admin.delayed.readonly': ['只读：不提供改期与取消', 'Read-only: no reschedule or cancel'],

  // 死信与重放（只读入口；无对应只读端点，按占位呈现）。
  'msg.admin.dlq.title': ['死信与重放', 'Dead letters & replay'],
  'msg.admin.dlq.deadLetters': ['死信（只读）', 'Dead letters (read-only)'],
  'msg.admin.dlq.replayTasks': ['重放任务（只读）', 'Replay tasks (read-only)'],
  'msg.admin.dlq.placeholder': ['只读列表数据源由 06-5 / 06-6 提供，本任务只搭入口', 'List endpoints belong to 06-5 / 06-6; this task only builds the entry'],
  'msg.admin.dlq.topic': ['主题', 'Topic'],
  'msg.admin.dlq.status': ['状态', 'Status'],
}

/** 中文文案包（键 `msg:zh-cn`）。 */
export const MSG_MESSAGES_ZH_CN: Record<string, string> = Object.fromEntries(
  Object.entries(CATALOG).map(([key, [zh]]) => [key, zh]),
)

/** 英文文案包（键 `msg:en`）。 */
export const MSG_MESSAGES_EN: Record<string, string> = Object.fromEntries(
  Object.entries(CATALOG).map(([key, [, en]]) => [key, en]),
)

/**
 * 取当前语言文案（缺省中文；未命中键原样返回）。
 *
 * @param key 文案键。
 * @param locale 语言标识。
 * @returns 文案。
 */
export function message(key: string, locale: string = 'zh-cn'): string {
  const pack = locale.toLowerCase().startsWith('en') ? MSG_MESSAGES_EN : MSG_MESSAGES_ZH_CN
  return pack[key] ?? key
}
