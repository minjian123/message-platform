<script setup lang="ts">
/**
 * 配额管理页（`/msg/quotas`）：六维配额的硬限 / 软限调整。
 *
 * 与租户记录页的只读「消息配额」页签分工：本页是写入口，只改限额；平台租户页保持只读。
 * 保存整批提交 `PUT /admin/quotas`，成功后重新取数（以服务端生效值为准，不在前端推算）。
 */

import { ElAlert, ElButton, ElInput, ElMessage, ElOption, ElSelect, ElTable, ElTableColumn, ElTag } from 'element-plus'
import { computed, onMounted, ref } from 'vue'

import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { QUOTA_SCOPES } from '../constants'
import { message } from '../i18n/messages'
import { hostPermissions } from '../runtime'
import { fetchQuotas, updateQuotas } from '../services/quota-service'
import type { msg } from '@msg/api-types'

type Schema = msg.components['schemas']
type QuotaItem = Schema['QuotaItem']

/** 可编辑行（硬限 / 软限以字符串承载输入，提交前统一归一）。 */
interface EditableRow {
  quota_key: string
  unit: string
  state: string
  usage: number | null
  source: string
  hard: string
  soft: string
}

const STATE_TAG: Record<string, 'success' | 'warning' | 'danger' | 'info'> = {
  ok: 'success',
  unconfigured: 'info',
  soft_exceeded: 'warning',
  hard_exceeded: 'danger',
  unavailable: 'info',
}

const scope = ref('')
const rows = ref<EditableRow[]>([])
const loading = ref(false)
const loadError = ref('')
/** 是否具备写权限（无 `msg:manage` 时只展示、不提供保存）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)

/** 保存动作。 */
const saveAction = useMsgWriteAction({
  runner: () => updateQuotas(buildItems()),
})

/**
 * 行 → 提交项（硬限 0 表示不限；软限空串表示按硬限派生）。
 *
 * @returns 批量调整项。
 */
function buildItems(): Schema['QuotaUpdateItem'][] {
  return rows.value.map((row) => ({
    quota_key: row.quota_key,
    namespace: scope.value,
    limit: Number(row.hard === '' ? 0 : row.hard),
    soft_limit: row.soft === '' ? null : Number(row.soft),
  }))
}

/** 软限超过硬限的行（硬限 0 = 不限，不参与比较）。 */
const softViolations = computed(() =>
  rows.value.filter((row) => {
    if (row.soft === '' || row.hard === '') {
      return false
    }
    const hard = Number(row.hard)
    return hard !== 0 && Number(row.soft) > hard
  }),
)

/**
 * 硬限 / 软限是否为非负整数（空串放行：硬限空按 0、软限空按不限）。
 *
 * @param row 可编辑行。
 * @returns 是否非法。
 */
function invalidValue(row: EditableRow): boolean {
  for (const raw of [row.hard, row.soft]) {
    if (raw.trim() === '') {
      continue
    }
    const value = Number(raw)
    if (!Number.isInteger(value) || value < 0) {
      return true
    }
  }
  return false
}

/** 含非法数值的行。 */
const invalidRows = computed(() => rows.value.filter(invalidValue))

/** 是否可保存（有权限、无越界、无非法值、无提交中）。 */
const canSave = computed(
  () =>
    canManage.value &&
    softViolations.value.length === 0 &&
    invalidRows.value.length === 0 &&
    saveAction.status.value !== 'running',
)

/**
 * 清单行 → 可编辑行。
 *
 * @param item 服务端配额行。
 * @returns 可编辑行。
 */
function toEditable(item: QuotaItem): EditableRow {
  return {
    quota_key: item.quota_key,
    unit: item.unit,
    state: item.state,
    usage: item.usage,
    source: item.source,
    hard: item.hard_limit === null ? '0' : String(item.hard_limit),
    soft: item.soft_limit === null ? '' : String(item.soft_limit),
  }
}

/** 状态标签类型。 */
function stateTag(state: string): 'success' | 'warning' | 'danger' | 'info' {
  return STATE_TAG[state] ?? 'info'
}

/** 维度展示名。 */
function keyLabel(key: string): string {
  return message(`msg.quota.${key}`)
}

/** 载入配额（失败展示错误，不阻断页面）。 */
async function load(): Promise<void> {
  loading.value = true
  loadError.value = ''
  try {
    const view = await fetchQuotas(scope.value)
    rows.value = (view.items ?? []).map(toEditable)
  } catch {
    rows.value = []
    loadError.value = message('error.fallback')
  } finally {
    loading.value = false
  }
}

/** 切换作用域并重新取数。 */
async function onScopeChange(): Promise<void> {
  saveAction.reset()
  await load()
}

/** 保存调整：成功后提示并以服务端值为准刷新（越界 / 非法 / 无权限时按钮已禁用）。 */
async function save(): Promise<void> {
  await saveAction.run()
  if (saveAction.status.value === 'done') {
    ElMessage.success(message('msg.admin.quotas.saved'))
    await load()
  }
}

onMounted(load)
</script>

<template>
  <section class="admin-page" data-test="page-quotas">
    <div class="admin-page__head">
      <span />
      <div class="admin-page__actions">
        <span class="admin-page__hint">{{ message('msg.admin.quotas.scope') }}</span>
        <el-select v-model="scope" style="width: 180px" data-test="quotas-scope" @change="onScopeChange">
          <el-option
            v-for="item in QUOTA_SCOPES"
            :key="item.key"
            :label="message(item.labelKey)"
            :value="item.key"
          />
        </el-select>
        <el-button
          v-if="canManage"
          type="primary"
          :disabled="!canSave"
          :loading="saveAction.status.value === 'running'"
          data-test="quotas-save"
          @click="save"
        >
          {{ message('msg.admin.common.save') }}
        </el-button>
      </div>
    </div>

    <el-alert v-if="loadError !== ''" type="error" :closable="false" show-icon :title="loadError" data-test="quotas-error" />
    <el-alert
      v-else-if="!canManage"
      type="info"
      :closable="false"
      show-icon
      :title="message('msg.admin.quotas.readonly')"
      data-test="quotas-readonly"
    />
    <el-alert
      v-if="invalidRows.length > 0"
      type="error"
      :closable="false"
      show-icon
      :title="message('msg.admin.quotas.integer')"
      data-test="quotas-invalid-warning"
    />
    <el-alert
      v-else-if="softViolations.length > 0"
      type="warning"
      :closable="false"
      show-icon
      :title="message('msg.admin.quotas.softExceedsHard')"
      data-test="quotas-soft-warning"
    />
    <p v-if="saveAction.errorKey.value !== ''" class="admin-page__error" data-test="quotas-save-error">
      {{ message(saveAction.errorKey.value) }}
    </p>

    <el-table
      v-loading="loading"
      :data="rows"
      size="small"
      border
      :empty-text="message('msg.admin.quotas.empty')"
      data-test="quotas-table"
    >
      <el-table-column :label="message('msg.quota.key')" min-width="140">
        <template #default="{ row }">{{ keyLabel(row.quota_key) }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.quotas.hard')" min-width="150">
        <template #default="{ row }">
          <el-input v-model="row.hard" size="small" :disabled="!canManage" data-test="quota-hard" />
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.quotas.soft')" min-width="150">
        <template #default="{ row }">
          <el-input v-model="row.soft" size="small" :disabled="!canManage" data-test="quota-soft" />
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.quotas.usage')" min-width="120" align="right">
        <template #default="{ row }">{{ row.usage === null ? message('msg.quota.state.unavailable') : row.usage }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.quota.state')" min-width="110">
        <template #default="{ row }">
          <el-tag :type="stateTag(row.state)" size="small">{{ message(`msg.quota.state.${row.state}`) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.quotas.origin')" min-width="110">
        <template #default="{ row }">{{ message(`msg.quota.source.${row.source}`) }}</template>
      </el-table-column>
    </el-table>
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
  align-items: center;
  justify-content: space-between;
  gap: var(--bms-space-2);
}
.admin-page__actions {
  display: flex;
  align-items: center;
  gap: var(--bms-space-1);
}
.admin-page__hint {
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.admin-page__error {
  margin: 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
</style>
