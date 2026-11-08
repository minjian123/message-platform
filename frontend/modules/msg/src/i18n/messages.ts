/** 模块文案（中文；平台 i18n 包）。 */

export const MSG_MESSAGES_ZH_CN: Record<string, string> = {
  'msg.quota.title': '消息配额',
  'msg.quota.key': '配额维度',
  'msg.quota.limit': '硬限',
  'msg.quota.soft': '软限',
  'msg.quota.usage': '用量',
  'msg.quota.state': '状态',
  'msg.quota.updatedAt': '更新时间',
  'msg.quota.source.default': '缺省',
  'msg.quota.source.configured': '已配置',
  'msg.quota.state.unconfigured': '未配置',
  'msg.quota.state.ok': '正常',
  'msg.quota.state.soft_exceeded': '接近上限',
  'msg.quota.state.hard_exceeded': '已超限',
  'msg.quota.state.unavailable': '暂不可用',
  'msg.quota.unlimited': '不限',
  'msg.quota.unavailable': '暂不可用：无「消息配额」查询权限或消息平台服务未接入',
  'msg.quota.empty': '该租户暂无消息配额数据',
  'msg.quota.topic_count': '主题数',
  'msg.quota.group_count': '消费组数',
  'msg.quota.produce_rate': '生产速率',
  'msg.quota.storage_bytes': '消息存储',
  'msg.quota.unacked_count': '未确认数',
  'msg.quota.connection_count': '连接数',
}

export const MSG_MESSAGES_EN: Record<string, string> = {
  'msg.quota.title': 'Message quota',
  'msg.quota.key': 'Dimension',
  'msg.quota.limit': 'Hard limit',
  'msg.quota.soft': 'Soft limit',
  'msg.quota.usage': 'Usage',
  'msg.quota.state': 'State',
  'msg.quota.updatedAt': 'Updated at',
  'msg.quota.source.default': 'Default',
  'msg.quota.source.configured': 'Configured',
  'msg.quota.state.unconfigured': 'Unconfigured',
  'msg.quota.state.ok': 'OK',
  'msg.quota.state.soft_exceeded': 'Near limit',
  'msg.quota.state.hard_exceeded': 'Exceeded',
  'msg.quota.state.unavailable': 'Unavailable',
  'msg.quota.unlimited': 'Unlimited',
  'msg.quota.unavailable': 'Unavailable: missing query permission or service not connected',
  'msg.quota.empty': 'No message quota data for this tenant',
  'msg.quota.topic_count': 'Topics',
  'msg.quota.group_count': 'Consumer groups',
  'msg.quota.produce_rate': 'Produce rate',
  'msg.quota.storage_bytes': 'Storage',
  'msg.quota.unacked_count': 'Unacked',
  'msg.quota.connection_count': 'Connections',
}

/** 取当前语言文案（缺省中文；未命中键原样返回）。 */
export function message(key: string, locale: string = 'zh-cn'): string {
  const pack = locale.toLowerCase().startsWith('en') ? MSG_MESSAGES_EN : MSG_MESSAGES_ZH_CN
  return pack[key] ?? key
}
