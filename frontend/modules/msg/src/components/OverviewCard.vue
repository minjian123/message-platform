<script setup lang="ts">
/** 工作台「消息平台概览」卡片（只读；与概览页共用同一份投影与数字卡）。 */

import OverviewMetrics from './OverviewMetrics.vue'
import { useOverviewSnapshot } from '../composables/useOverviewSnapshot'
import { message } from '../i18n/messages'

const { overview, state, textKey, firstLoad, lastUpdated, refresh } = useOverviewSnapshot()
</script>

<template>
  <section class="overview-card" data-test="msg-overview-card">
    <h3 class="overview-card__title">{{ message('msg.admin.overview.card') }}</h3>
    <overview-metrics
      :overview="overview"
      :state="state"
      :text-key="textKey"
      :first-load="firstLoad"
      :last-updated="lastUpdated"
      :show-grafana="false"
      :show-polling-hint="false"
      @refresh="refresh"
    />
  </section>
</template>

<style scoped>
.overview-card {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.overview-card__title {
  margin: 0;
  font-size: 15px;
}
.overview-card :deep(.overview-metrics__grid) {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}
</style>
