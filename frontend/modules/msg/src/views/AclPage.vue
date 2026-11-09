<script setup lang="ts">
/**
 * 主题授权页（`/msg/acls`）：列表 / 过滤 / 分页 / 授予与更新 / 收回 / 主体名称解析。
 *
 * - 页面、菜单与按钮挂 `msg:manage`（与 05-3 列表端点同码）；
 * - 过滤条件与页码写入路由查询参数，支持深链与刷新保持；
 * - 主体名称按主键批量解析；无 `user:query` / `role:query` 或接口失败降级手填。
 */

import {
  ElAlert,
  ElButton,
  ElOption,
  ElSelect,
  ElTable,
  ElTableColumn,
  ElTag,
} from 'element-plus'
import { computed, ref, watch } from 'vue'

import type { ImpactRow } from '../base/BaseMsgWriteAction'
import AclGrantDrawer, { type AclGrantMode, type AclGrantPrefill } from '../components/AclGrantDrawer.vue'
import MsgConfirmDialog from '../components/MsgConfirmDialog.vue'
import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { usePrincipalDirectory } from '../composables/usePrincipalDirectory'
import { message } from '../i18n/messages'
import { hostPermissions, MSG_MANAGE_PERMISSION } from '../runtime'
import {
  ACL_ACTION_OPTIONS,
  ACL_EFFECT_OPTIONS,
  ACL_PRINCIPAL_TYPE_OPTIONS,
  ACL_STATUS_OPTIONS,
  loadAcls,
  revokeAcl,
  type AclItem,
  type AclRevokeResult,
} from '../services/acl-service'
import type { ReadOnlyFilters } from '../base/BaseMsgReadOnlyView'

/** URL 中允许回填的筛选键（与 `AclQuery` 同名）。 */
const FILTER_KEYS = ['namespace', 'topic', 'principal_type', 'principal_key', 'action', 'status'] as const

const adminRoute = useAdminRoute()

/**
 * 从路由查询参数取白名单筛选条件。
 *
 * @param query 路由查询参数。
 * @returns 筛选条件。
 */
function pickFilters(query: Record<string, string>): ReadOnlyFilters {
  const filters: ReadOnlyFilters = {}
  for (const key of FILTER_KEYS) {
    const value = query[key]
    if (value !== undefined && value !== '') {
      filters[key] = value
    }
  }
  return filters
}

/**
 * 解析正整数（非法回落缺省）。
 *
 * @param raw 原始字符串。
 * @param fallback 缺省值。
 * @returns 正整数。
 */
function positiveInt(raw: string | undefined, fallback: number): number {
  if (raw === undefined || !/^\d+$/.test(raw)) {
    return fallback
  }
  const value = Number(raw)
  return value > 0 ? value : fallback
}

const initialQuery = adminRoute.query
const initialFilters = pickFilters(initialQuery)

const view = useMsgReadOnlyView<AclItem>({
  loader: loadAcls,
  initialFilters,
  initialPage: positiveInt(initialQuery.page, 1),
  initialPageSize: positiveInt(initialQuery.size, 20),
  permission: MSG_MANAGE_PERMISSION,
})

const directory = usePrincipalDirectory()

const namespace = ref(String(initialFilters.namespace ?? ''))
const topic = ref(String(initialFilters.topic ?? ''))
const principalType = ref(String(initialFilters.principal_type ?? ''))
const principalKey = ref(String(initialFilters.principal_key ?? ''))
const action = ref(String(initialFilters.action ?? ''))
const status = ref(String(initialFilters.status ?? ''))

/** 反馈文案（成功 / 无变化；下一次操作前清空）。 */
const feedback = ref('')
/** 授予抽屉。 */
const grantVisible = ref(false)
const grantMode = ref<AclGrantMode>('create')
const grantPrefill = ref<AclGrantPrefill | null>(null)
/** 收回确认。 */
const revokeVisible = ref(false)
const revokeRow = ref<AclItem | null>(null)

/** 收回动作。 */
const revokeAction = useMsgWriteAction<AclRevokeResult>({
  runner: () => revokeAcl((revokeRow.value as AclItem).acl_id),
  requiresConfirm: true,
})

/** 收回错误文案（350027 用页面级键）。 */
const revokeErrorText = computed(() => {
  const code = revokeAction.errorCode.value
  if (code === undefined) {
    return ''
  }
  if (code === 350027) {
    return message('msg.admin.acls.error.350027')
  }
  return message(revokeAction.errorKey.value)
})

/** 收回影响范围。 */
const revokeImpact = computed<ImpactRow[]>(() => {
  const row = revokeRow.value
  if (row === null) {
    return []
  }
  return [
    { label: message('msg.admin.acls.namespace'), value: row.namespace },
    { label: message('msg.admin.acls.topic'), value: row.topic },
    {
      label: message('msg.admin.acls.principalKey'),
      value: `${principalCell(row) || message('msg.admin.acls.principalUnavailable')}（${row.principal_key}）`,
    },
    { label: message('msg.admin.acls.action'), value: actionText(row.action) },
  ]
})

/** 列表态文案（无 msg:manage 时用页面级键）。 */
const listTextKey = computed(() => (view.state.value === 'denied' ? 'state.denied.manage' : view.textKey.value))

/** 是否可管理（页面已由宿主守卫拦截，此处只做展示层兜底）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes(MSG_MANAGE_PERMISSION) === true)

/** 当页行变化后批量解析主体名称。 */
watch(
  () => view.rows.value,
  (rows) => {
    void directory.resolve(rows, hostPermissions())
  },
)

/**
 * 应用筛选（一次刷新）。
 */
function apply(): void {
  feedback.value = ''
  view.setFilters({
    namespace: namespace.value,
    topic: topic.value,
    principal_type: principalType.value,
    principal_key: principalKey.value,
    action: action.value,
    status: status.value,
  })
  syncQuery()
}

/** 重置筛选并清空 URL。 */
function reset(): void {
  namespace.value = ''
  topic.value = ''
  principalType.value = ''
  principalKey.value = ''
  action.value = ''
  status.value = ''
  feedback.value = ''
  view.reset()
  syncQuery()
}

/**
 * 翻页并同步 URL。
 *
 * @param page 目标页码。
 */
function onPage(page: number): void {
  view.setPage(page)
  syncQuery()
}

/** 把当前筛选与分页写入路由查询参数。 */
function syncQuery(): void {
  const query: Record<string, string> = {}
  if (namespace.value !== '') {
    query.namespace = namespace.value
  }
  if (topic.value !== '') {
    query.topic = topic.value
  }
  if (principalType.value !== '') {
    query.principal_type = principalType.value
  }
  if (principalKey.value !== '') {
    query.principal_key = principalKey.value
  }
  if (action.value !== '') {
    query.action = action.value
  }
  if (status.value !== '') {
    query.status = status.value
  }
  if (view.page.value > 1) {
    query.page = String(view.page.value)
  }
  if (view.pageSize.value !== 20) {
    query.size = String(view.pageSize.value)
  }
  adminRoute.replaceQuery(query)
}

/** 打开新建授予抽屉。 */
function openCreate(): void {
  feedback.value = ''
  grantMode.value = 'create'
  grantPrefill.value = null
  grantVisible.value = true
}

/**
 * 打开更新 / 重新授予抽屉。
 *
 * @param row 列表行。
 * @param mode 模式。
 */
function openEdit(row: unknown, mode: AclGrantMode): void {
  const item = row as AclItem
  feedback.value = ''
  grantMode.value = mode
  grantPrefill.value = {
    namespace: item.namespace,
    topic: item.topic,
    principalType: item.principal_type,
    principalKey: item.principal_key,
    principalLabel: directory.label(item.principal_type, item.principal_key),
    action: item.action,
    effect: item.effect,
  }
  grantVisible.value = true
}

/**
 * 授予 / 更新成功：反馈并刷新列表。
 *
 * @param payload 变更条数。
 */
function onGrantSaved(payload: { changed: number }): void {
  feedback.value = message(
    payload.changed > 0 ? 'msg.admin.acls.grant.saved' : 'msg.admin.acls.grant.noChange',
  )
  void view.refresh()
}

/**
 * 打开收回确认。
 *
 * @param row 列表行。
 */
function openRevoke(row: unknown): void {
  feedback.value = ''
  revokeRow.value = row as AclItem
  revokeAction.reset()
  revokeVisible.value = true
}

/** 确认收回。 */
async function confirmRevoke(): Promise<void> {
  revokeAction.acknowledge()
  await revokeAction.run()
  if (revokeAction.status.value !== 'done') {
    return
  }
  const result = revokeAction.result.value as AclRevokeResult
  feedback.value = message(result.changed ? 'msg.admin.acls.revoked' : 'msg.admin.acls.noChange')
  revokeVisible.value = false
  void view.refresh()
}

/**
 * 主体名称（未解析返回空串，由调用方决定占位）。
 *
 * @param row 列表行。
 * @returns 名称。
 */
function principalCell(row: unknown): string {
  const item = row as AclItem
  return directory.label(item.principal_type, item.principal_key)
}

/**
 * 是否名称不可用。
 *
 * @param row 列表行。
 * @returns 是否不可用。
 */
function nameUnavailable(row: unknown): boolean {
  const item = row as AclItem
  return directory.label(item.principal_type, item.principal_key) === ''
}

/**
 * 主体类型文案。
 *
 * @param value 值。
 */
function principalTypeText(value: string): string {
  return message(ACL_PRINCIPAL_TYPE_OPTIONS.find((item) => item.value === value)?.labelKey ?? 'msg.admin.acls.principalType')
}

/**
 * 动作文案。
 *
 * @param value 值。
 */
function actionText(value: string): string {
  return message(ACL_ACTION_OPTIONS.find((item) => item.value === value)?.labelKey ?? 'msg.admin.acls.action')
}

/**
 * 效果文案。
 *
 * @param value 值。
 */
function effectText(value: string): string {
  return message(ACL_EFFECT_OPTIONS.find((item) => item.value === value)?.labelKey ?? 'msg.admin.acls.effect')
}

/**
 * 状态文案。
 *
 * @param value 值。
 */
function statusText(value: string): string {
  return message(ACL_STATUS_OPTIONS.find((item) => item.value === value)?.labelKey ?? 'msg.admin.acls.status')
}

/**
 * 效果标签色。
 *
 * @param value 值。
 */
function effectTag(value: string): 'success' | 'danger' {
  return value === 'allow' ? 'success' : 'danger'
}

/**
 * 状态标签色。
 *
 * @param value 值。
 */
function statusTag(value: string): 'success' | 'info' {
  return value === 'enabled' ? 'success' : 'info'
}
</script>

<template>
  <section class="admin-page" data-test="page-acls">
    <div class="admin-page__head">
      <div>
        <h1>{{ message('msg.admin.acls.title') }}</h1>
        <p class="admin-page__hint">
          页面、菜单与按钮挂 <code>msg:manage</code>；过滤条件写入路由查询参数，支持深链与刷新保持
        </p>
      </div>
      <div class="admin-page__actions">
        <el-button v-if="canManage" type="primary" data-test="acl-grant" @click="openCreate">
          {{ message('msg.admin.acls.grant') }}
        </el-button>
      </div>
    </div>

    <el-alert
      v-if="feedback !== ''"
      type="success"
      :closable="false"
      show-icon
      :title="feedback"
      data-test="acl-feedback"
    />

    <read-only-list
      :title="message('msg.admin.acls.title')"
      :state="view.state.value"
      :text-key="listTextKey"
      :page="view.page.value"
      :page-size="view.pageSize.value"
      :total="view.total.value"
      @refresh="view.refresh"
      @update:page="onPage"
    >
      <template #filters>
        <div class="admin-page__filters">
          <el-input v-model="namespace" :placeholder="message('msg.admin.acls.namespace')" clearable style="width: 150px" data-test="acl-namespace" />
          <el-input v-model="topic" :placeholder="message('msg.admin.acls.topic')" clearable style="width: 160px" data-test="acl-topic" />
          <el-select v-model="principalType" :placeholder="message('msg.admin.acls.principalType')" clearable style="width: 130px" data-test="acl-principal-type">
            <el-option v-for="item in ACL_PRINCIPAL_TYPE_OPTIONS" :key="item.value" :label="message(item.labelKey)" :value="item.value" />
          </el-select>
          <el-input v-model="principalKey" :placeholder="message('msg.admin.acls.principalKey')" clearable style="width: 160px" data-test="acl-principal-key" />
          <el-select v-model="action" :placeholder="message('msg.admin.acls.action')" clearable style="width: 120px" data-test="acl-action">
            <el-option v-for="item in ACL_ACTION_OPTIONS" :key="item.value" :label="message(item.labelKey)" :value="item.value" />
          </el-select>
          <el-select v-model="status" :placeholder="message('msg.admin.acls.status')" clearable style="width: 120px" data-test="acl-status">
            <el-option v-for="item in ACL_STATUS_OPTIONS" :key="item.value" :label="message(item.labelKey)" :value="item.value" />
          </el-select>
          <el-button type="primary" data-test="acl-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="acl-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>

      <el-table :data="view.rows.value" size="small" border data-test="acl-table">
        <el-table-column prop="namespace" :label="message('msg.admin.acls.namespace')" min-width="110" />
        <el-table-column prop="topic" :label="message('msg.admin.acls.topic')" min-width="150" />
        <el-table-column :label="message('msg.admin.acls.principalType')" width="100">
          <template #default="{ row }">{{ principalTypeText(row.principal_type) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.acls.principalKey')" min-width="210">
          <template #default="{ row }">
            <div class="acl-principal-cell">
              <span v-if="!nameUnavailable(row)" data-test="acl-principal-name">{{ principalCell(row) }}</span>
              <span v-else class="acl-principal-cell__missing" data-test="acl-principal-unavailable">
                {{ message('msg.admin.acls.principalUnavailable') }}
              </span>
              <span class="acl-principal-cell__key">{{ row.principal_key }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.acls.action')" width="100">
          <template #default="{ row }">{{ actionText(row.action) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.acls.effect')" width="90">
          <template #default="{ row }">
            <el-tag :type="effectTag(row.effect)" size="small" data-test="acl-effect-tag">{{ effectText(row.effect) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.acls.status')" width="100">
          <template #default="{ row }">
            <el-tag :type="statusTag(row.status)" size="small" data-test="acl-status-tag">{{ statusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="updated_at" :label="message('msg.admin.acls.updatedAt')" min-width="170" />
        <el-table-column v-if="canManage" :label="message('msg.admin.common.actions')" width="190" fixed="right">
          <template #default="{ row }">
            <el-button
              v-if="row.status === 'enabled'"
              link
              type="primary"
              data-test="acl-update"
              @click="openEdit(row, 'update')"
            >
              {{ message('msg.admin.acls.update') }}
            </el-button>
            <el-button v-else link type="primary" data-test="acl-regrant" @click="openEdit(row, 'regrant')">
              {{ message('msg.admin.acls.regrant') }}
            </el-button>
            <el-button
              v-if="row.status === 'enabled'"
              link
              type="danger"
              data-test="acl-revoke"
              @click="openRevoke(row)"
            >
              {{ message('msg.admin.acls.revoke') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </read-only-list>

    <acl-grant-drawer
      v-model="grantVisible"
      :mode="grantMode"
      :prefill="grantPrefill"
      :permission-codes="hostPermissions()"
      @saved="onGrantSaved"
    />

    <msg-confirm-dialog
      v-model="revokeVisible"
      :title="message('msg.admin.acls.revoke.title')"
      :rows="revokeImpact"
      :warning="message('msg.admin.acls.revoke.warning')"
      danger
      :loading="revokeAction.status.value === 'running'"
      @confirm="confirmRevoke"
    >
      <p v-if="revokeErrorText !== ''" class="admin-page__error" data-test="acl-revoke-error">{{ revokeErrorText }}</p>
    </msg-confirm-dialog>
  </section>
</template>

<style scoped>
.admin-page {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.admin-page__head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--bms-space-2);
}
.admin-page__hint {
  margin: 4px 0 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.admin-page__actions {
  display: flex;
  align-items: center;
  gap: var(--bms-space-1);
}
.admin-page__error {
  margin: var(--bms-space-2) 0 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
.admin-page__filters {
  display: flex;
  gap: var(--bms-space-2);
  flex-wrap: wrap;
  align-items: center;
}
.acl-principal-cell {
  display: flex;
  flex-direction: column;
  gap: 1px;
}
.acl-principal-cell__missing {
  color: var(--bms-color-warning);
  font-size: 11px;
}
.acl-principal-cell__key {
  color: var(--bms-color-text-secondary);
  font-family: Consolas, Monaco, monospace;
  font-size: 11px;
}
</style>
