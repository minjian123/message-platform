<script setup lang="ts">
/**
 * 租户用量页（`/msg/usage`）：四张数字卡 + 口径说明。
 *
 * **只读**：只取 `GET /admin/tenant-usage`，不写数据、不重算用量、不自绘趋势；
 * 刷新由用户手动触发（本页为低频查看，页签内嵌时也不在宿主页挂常驻轮询）。
 * 状态机与竞态取自 `BaseMsgReadOnlyView` 投影，页面只做渲染与降级展示。
 */
import { ElAlert, ElButton } from 'element-plus'
import { computed } from 'vue'

import UsageCards from '../components/UsageCards.vue'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadUsageCards, type UsageCardRow } from '../services/usage-service'

const view = useMsgReadOnlyView<UsageCardRow>({ loader: loadUsageCards })

/** 是否整页降级（无权限 / 请求失败）：此时不渲染四卡，避免半截数据被误读。 */
const blocked = computed(() => view.state.value === 'denied' || view.state.value === 'error')
/** 降级提示文案键（无权限用通用键，失败按错误码映射）。 */
const blockedKey = computed(() => (view.state.value === 'denied' ? 'state.denied' : view.textKey.value))
/** 降级提示条类型。 */
const blockedType = computed(() => (view.state.value === 'denied' ? 'warning' : 'error'))
</script>

<template>
  <section class="admin-page" data-test="page-usage">
    <div class="admin-page__head">
      <div>
        <h1>{{ message('msg.admin.usage.title') }}</h1>
        <p class="admin-page__hint" data-test="usage-subtitle">{{ message('msg.admin.usage.subtitle') }}</p>
      </div>
      <el-button :loading="view.state.value === 'loading'" data-test="usage-refresh" @click="view.refresh">
        {{ message('msg.admin.common.refresh') }}
      </el-button>
    </div>

    <el-alert
      v-if="blocked"
      :type="blockedType"
      :closable="false"
      show-icon
      :title="message(blockedKey)"
      data-test="usage-blocked"
    >
      <template v-if="view.state.value === 'error'" #default>
        <el-button size="small" type="primary" data-test="usage-retry" @click="view.refresh">
          {{ message('msg.admin.common.refresh') }}
        </el-button>
      </template>
    </el-alert>

    <template v-else>
      <usage-cards :rows="view.rows.value" :state="view.state.value" :layout="4" />

      <section class="usage-note" data-test="usage-note">
        <h2 class="usage-note__title">{{ message('msg.admin.usage.note') }}</h2>
        <ul class="usage-note__list">
          <li>{{ message('msg.usage.note.topicCount') }}</li>
          <li>{{ message('msg.usage.note.partitionCount') }}</li>
          <li>{{ message('msg.usage.note.storage') }}</li>
          <li>{{ message('msg.usage.note.deliveredToday') }}</li>
          <li>{{ message('msg.usage.note.source') }}</li>
          <li>{{ message('msg.usage.note.grafana') }}</li>
        </ul>
      </section>
    </template>
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
  margin: 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.usage-note {
  margin-top: var(--bms-space-2);
  padding: var(--bms-space-2);
  border-radius: var(--bms-radius-sm);
  background: var(--bms-color-fill-light);
}
.usage-note__title {
  margin: 0 0 var(--bms-space-1);
  font-size: 13px;
  font-weight: 600;
}
.usage-note__list {
  margin: 0;
  padding-left: 18px;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
  line-height: 1.7;
}
</style>
