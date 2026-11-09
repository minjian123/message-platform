<script setup lang="ts">
// 插件：消息配额（租户记录页只读页签；`sys.record.detail.tabs`）。
//
// **只读**：不注册提交器、不提供编辑 / 保存；数据来自 msg 管理面 `GET /admin/quotas`。
// 宿主页经显式上下文注入 `formKey` / `recordId`（缺省由 `DictRecordLayout` 从系统信息派生）；
// 非租户表单不渲染（返回空容器），请求失败降级为「暂不可用」，不影响宿主页。
import { ElAlert, ElTable, ElTableColumn, ElTag } from 'element-plus'
import { useModuleSlotField } from '@bms/ui_ep'
import { computed, onMounted, ref } from 'vue'

import type { msg } from '@msg/api-types'
import { message } from '../i18n/messages'
import { fetchQuotas } from '../services/quota-service'

/** 宿主上下文：表单标识与记录主键。 */
const formKey = useModuleSlotField<string>('formKey')
const recordId = useModuleSlotField<string>('recordId')

/** 是否租户记录页（非租户不渲染）。 */
const visible = computed(() => formKey.value === 'tenant')
/** 当前租户主键（仅展示 / 排障用；后端以租户上下文为准）。 */
const tenantId = computed(() => recordId.value ?? '')

/** 是否载入中。 */
const loading = ref(false)
/** 是否不可用（无请求能力 / 请求失败）。 */
const unavailable = ref(false)
/** 配额行（契约类型取自生成物）。 */
type QuotaItem = msg.components['schemas']['QuotaItem']
const items = ref<QuotaItem[]>([])

const STATE_TAG: Record<string, 'success' | 'warning' | 'danger' | 'info'> = {
  ok: 'success',
  unconfigured: 'info',
  soft_exceeded: 'warning',
  hard_exceeded: 'danger',
  unavailable: 'info',
}

/** 状态文案键。 */
function stateLabel(state: string): string {
  return message(`msg.quota.state.${state}`)
}

/** 状态标签类型。 */
function stateTag(state: string): 'success' | 'warning' | 'danger' | 'info' {
  return STATE_TAG[state] ?? 'info'
}

/** 维度展示名。 */
function keyLabel(key: string): string {
  return message(`msg.quota.${key}`)
}

/** 字节数人性化（B / KiB / MiB / GiB / TiB）。 */
function humanBytes(value: number): string {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  let amount = value
  let index = 0
  while (amount >= 1024 && index < units.length - 1) {
    amount /= 1024
    index += 1
  }
  return `${Number.isInteger(amount) ? amount : amount.toFixed(1)} ${units[index]}`
}

/**
 * 配额值格式化。
 *
 * @param value 数值（null 表示不限 / 不可用）。
 * @param unit 单位。
 * @param usage 是否用量列（null 显示暂不可用）。
 */
function formatValue(value: number | null, unit: string, usage = false): string {
  if (value === null) {
    return usage ? message('msg.quota.state.unavailable') : message('msg.quota.unlimited')
  }
  if (unit === 'bytes') {
    return humanBytes(value)
  }
  if (unit === 'qps') {
    return `${value}/s`
  }
  return String(value)
}

/** 时间格式化（UTC → 本地可读；空值显示 `-`）。 */
function formatTime(value: string | null): string {
  if (value === null || value === '') {
    return '-'
  }
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

/** 载入配额（失败静默降级）。 */
async function load(): Promise<void> {
  if (!visible.value) {
    return
  }
  loading.value = true
  unavailable.value = false
  try {
    const view = await fetchQuotas()
    items.value = view.items ?? []
  } catch {
    unavailable.value = true
    items.value = []
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>

<template>
  <div v-if="visible" class="msg-quota-panel" data-test="msg-quota-panel" :data-tenant="tenantId" :data-loading="loading">
    <el-alert
      v-if="unavailable"
      type="info"
      :closable="false"
      show-icon
      :title="message('msg.quota.unavailable')"
      data-test="msg-quota-unavailable"
    />
    <el-table
      v-else
      :data="items"
      size="small"
      border
      data-test="msg-quota-table"
      :empty-text="message('msg.quota.empty')"
    >
      <el-table-column :label="message('msg.quota.key')" min-width="140">
        <template #default="{ row }">{{ keyLabel(row.quota_key) }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.quota.limit')" min-width="120">
        <template #default="{ row }">{{ formatValue(row.hard_limit, row.unit) }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.quota.soft')" min-width="120">
        <template #default="{ row }">{{ formatValue(row.soft_limit, row.unit) }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.quota.usage')" min-width="120">
        <template #default="{ row }">{{ formatValue(row.usage, row.unit, true) }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.quota.state')" min-width="110">
        <template #default="{ row }">
          <el-tag :type="stateTag(row.state)" size="small">{{ stateLabel(row.state) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.quota.updatedAt')" min-width="180">
        <template #default="{ row }">{{ formatTime(row.updated_at) }}</template>
      </el-table-column>
    </el-table>
  </div>
</template>

<style scoped>
.msg-quota-panel {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
  min-height: 160px;
}
</style>
