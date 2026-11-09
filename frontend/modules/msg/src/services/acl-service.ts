/**
 * 主题授权取数（05-3 管理端点；产品域寻址，经宿主注入能力）。
 *
 * - 契约类型取自 `@msg/api-types`（唯一事实源 `deploy/contracts/msg.json`）；
 * - 请求体不接收租户参数；页面不自拼 `/api` 前缀；
 * - 授予 / 更新整批提交，`changed` 表达幂等是否真正发生变更。
 */

import type { msg } from '@msg/api-types'

import type { ReadOnlyQueryParams } from '../base/BaseMsgReadOnlyView'
import type { ReadOnlyLoader } from '../composables/useMsgReadOnlyView'
import { delData, getData, putData, requiredData } from './product-api'

type Schema = msg.components['schemas']
type Ops = msg.operations

/** ACL 列表响应。 */
export type AclPage = Schema['AclPage']
/** ACL 列表行。 */
export type AclItem = Schema['AclItem']
/** 授予 / 更新请求项。 */
export type AclRuleInput = Schema['AclRuleRequest']
/** 批量授予 / 更新结果。 */
export type AclBatchResult = Schema['AclBatchResult']
/** 收回结果。 */
export type AclRevokeResult = Schema['AclRevokeResult']
/** ACL 列表查询参数（命名空间 / 主题 / 主体 / 动作 / 状态 + 分页）。 */
export type AclQuery = NonNullable<Ops['list_acls_api_v1_msg_admin_acls_get']['parameters']['query']>

/** 动作选项（值 + 模块 i18n 键）。 */
export const ACL_ACTION_OPTIONS: readonly { value: string; labelKey: string }[] = [
  { value: 'publish', labelKey: 'msg.admin.acls.action.publish' },
  { value: 'subscribe', labelKey: 'msg.admin.acls.action.subscribe' },
  { value: 'describe', labelKey: 'msg.admin.acls.action.describe' },
  { value: 'manage', labelKey: 'msg.admin.acls.action.manage' },
]

/** 效果选项。 */
export const ACL_EFFECT_OPTIONS: readonly { value: string; labelKey: string }[] = [
  { value: 'allow', labelKey: 'msg.admin.acls.effect.allow' },
  { value: 'deny', labelKey: 'msg.admin.acls.effect.deny' },
]

/** 主体类型选项。 */
export const ACL_PRINCIPAL_TYPE_OPTIONS: readonly { value: string; labelKey: string }[] = [
  { value: 'user', labelKey: 'msg.admin.acls.principalType.user' },
  { value: 'role', labelKey: 'msg.admin.acls.principalType.role' },
]

/** 状态选项。 */
export const ACL_STATUS_OPTIONS: readonly { value: string; labelKey: string }[] = [
  { value: 'enabled', labelKey: 'msg.admin.acls.status.enabled' },
  { value: 'disabled', labelKey: 'msg.admin.acls.status.disabled' },
]

/** 空 ACL 页（接口未返回数据时的结构占位，不伪造行）。 */
export const EMPTY_ACL_PAGE: AclPage = { items: [], page: 1, size: 20, total: 0 }

/**
 * ACL 规则列表（分页 + 过滤）。
 *
 * @param params 查询参数。
 * @returns 当页规则与总数。
 */
export async function fetchAcls(params: AclQuery = {}): Promise<AclPage> {
  const page = await getData<AclPage>('/admin/acls', params)
  return page ?? { ...EMPTY_ACL_PAGE, page: params.page ?? 1, size: params.size ?? 20 }
}

/**
 * 批量授予 / 更新 ACL（整批成功 / 失败）。
 *
 * @param items 规则清单。
 * @returns 写入后的规则与变更 ID。
 */
export async function grantAcls(items: readonly AclRuleInput[]): Promise<AclBatchResult> {
  const body: Schema['AclBatchRequest'] = { items: [...items] }
  return requiredData(await putData<AclBatchResult>('/admin/acls', body))
}

/**
 * 收回一条 ACL（软停用；幂等）。
 *
 * @param aclId 规则 ID（雪花字符串）。
 * @returns 收回结果（`changed=false` 表示已停用 / 不存在）。
 */
export async function revokeAcl(aclId: string): Promise<AclRevokeResult> {
  return requiredData(await delData<AclRevokeResult>(`/admin/acls/${aclId}`))
}

/** ACL 列表投影取数器。 */
export const loadAcls: ReadOnlyLoader<AclItem> = async (params: ReadOnlyQueryParams) => {
  const page = await fetchAcls(params as AclQuery)
  return { items: page.items ?? [], total: page.total }
}
