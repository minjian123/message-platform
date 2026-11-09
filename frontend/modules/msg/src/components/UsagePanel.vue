<script setup lang="ts">
/**
 * 消息用量（租户记录页只读页签；区域 `sys.record.detail.tabs`）。
 *
 * **只读**：不注册提交器、不提供写入；四列总量来自 `GET /admin/tenant-usage`。
 * 与 05-2「消息配额」页签分工固定：本页签看四列总量，配额页签看限额 / 软硬限状态。
 *
 * 宿主页经显式上下文注入 `formKey` / `recordId`（缺省由 `DictRecordLayout` 从系统信息派生）；
 * 非租户表单不渲染（返回空容器）；请求失败降级为「暂不可用」，不影响宿主页。
 *
 * **统计租户口径**：请求头 `X-Tenant-ID` 来自登录态，不随记录页切换，
 * 故页签显式提示「按当前登录租户统计」并给出记录页租户标识供对照。
 */
import { ElAlert, ElLink } from 'element-plus'
import { useModuleSlotField } from '@bms/ui_ep'
import { computed, watch } from 'vue'

import UsageCards from './UsageCards.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadUsageCards, type UsageCardRow } from '../services/usage-service'

/** 宿主上下文：表单标识与记录主键。 */
const formKey = useModuleSlotField<string>('formKey')
const recordId = useModuleSlotField<string>('recordId')
const { go } = useAdminRoute()

/** 是否租户记录页（非租户不渲染）。 */
const visible = computed(() => formKey.value === 'tenant')
/** 记录页租户标识（仅对照展示；取数以登录态租户上下文为准）。 */
const targetTenant = computed(() => recordId.value ?? '')

const view = useMsgReadOnlyView<UsageCardRow>({ loader: loadUsageCards, active: visible.value })

watch(visible, (next) => view.setActive(next))

/** 是否降级（无权限 / 请求失败）：不渲染四卡与跳转。 */
const degraded = computed(() => view.state.value === 'denied' || view.state.value === 'error')

/** 跳转独立页（宿主路由由模块 `routes` 注册，不替换宿主路由表）。 */
function openPage(): void {
  go('/msg/usage')
}
</script>

<template>
  <div v-if="visible" class="usage-panel" data-test="usage-panel" :data-state="view.state.value">
    <el-alert
      v-if="degraded"
      type="info"
      :closable="false"
      show-icon
      :title="message('msg.usage.unavailable')"
      data-test="usage-panel-degraded"
    />
    <template v-else>
      <p class="usage-panel__hint" data-test="usage-panel-hint">
        <span>{{ message('msg.usage.tenantHint') }}</span>
        <template v-if="targetTenant !== ''">
          <span class="usage-panel__sep">·</span>
          <span>{{ message('msg.usage.tenantTarget') }} {{ targetTenant }}</span>
        </template>
      </p>

      <usage-cards :rows="view.rows.value" :state="view.state.value" :layout="2" />

      <div class="usage-panel__actions">
        <el-link type="primary" data-test="usage-panel-open" @click="openPage">
          {{ message('msg.usage.openPage') }}
        </el-link>
      </div>
    </template>
  </div>
</template>

<style scoped>
.usage-panel {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
  min-height: 160px;
}
.usage-panel__hint {
  display: flex;
  align-items: center;
  gap: 4px;
  margin: 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.usage-panel__sep {
  opacity: 0.6;
}
.usage-panel__actions {
  display: flex;
  justify-content: flex-end;
}
</style>
