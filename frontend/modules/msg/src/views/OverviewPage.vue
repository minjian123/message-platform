<script setup lang="ts">
/** 平台概览页（四值数字卡 + Grafana 入口；与工作台卡片共用取数投影）。 */

import OverviewMetrics from '../components/OverviewMetrics.vue'
import { useOverviewSnapshot } from '../composables/useOverviewSnapshot'
import { message } from '../i18n/messages'

const { overview, state, textKey, firstLoad, lastUpdated, refresh } = useOverviewSnapshot()
</script>

<template>
  <section class="admin-page" data-test="page-overview">
    <p class="admin-page__hint" data-test="overview-grafana-hint">
      {{ message('msg.admin.overview.grafanaHint') }}
    </p>
    <overview-metrics
      :overview="overview"
      :state="state"
      :text-key="textKey"
      :first-load="firstLoad"
      :last-updated="lastUpdated"
      @refresh="refresh"
    />
  </section>
</template>

<style scoped>
.admin-page {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.admin-page__hint {
  margin: 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
</style>
