/** 模块级常量（集中放部署相关地址与刷新口径，页面不写死域名 / 周期）。 */

/** 工作台「消息平台概览」卡片轮询间隔（毫秒）。 */
export const OVERVIEW_POLL_INTERVAL_MS = 30_000

/**
 * Grafana 平台概览看板地址（06-3-2 交付的看板 UID `msg-overview`）。
 *
 * 留空表示当前部署未配置；此时页面与卡片均不渲染跳转按钮。
 */
export const GRAFANA_OVERVIEW_URL = ''

/** 宿主菜单分组名（模块自描述，宿主按 `meta.group` 归组）。 */
export const MSG_MENU_GROUP = '消息平台'

/** 删主题清理进度轮询间隔（毫秒；`deleting` 状态详情页用）。 */
export const CLEANUP_POLL_INTERVAL_MS = 3_000

/** 配额管理页的作用域（租户级 / 命名空间级）。 */
export const QUOTA_SCOPES: readonly { key: string; labelKey: string }[] = [
  { key: '', labelKey: 'msg.admin.quotas.scopeTenant' },
  { key: 'msg', labelKey: 'msg.admin.quotas.scopeNamespace' },
]

/** 06-7「主题授权」预留的菜单位次（本任务不预注册路由）。 */
export const ORDER_RESERVED_TOPIC_ACL = 80

/** 06-8「租户用量」预留的菜单位次（本任务不预注册路由）。 */
export const ORDER_RESERVED_TENANT_USAGE = 90
