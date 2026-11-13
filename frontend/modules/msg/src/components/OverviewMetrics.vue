<script setup lang="ts">
/**
 * 平台概览四值数字卡（概览页与工作台卡片共用）。
 *
 * 数值 / 冷启动占位 / 错误态全部键化；趋势图不自绘，Grafana 地址来自模块常量，缺配置不渲染入口。
 */
import { MetricCard } from '@bms/ui_ep'
import { ElAlert, ElButton, ElLink } from 'element-plus'
import { computed } from 'vue'

import type { ReadOnlyViewState } from '../base/BaseMsgReadOnlyView'
import type { Overview } from '../services/admin-service'
import { GRAFANA_OVERVIEW_URL } from '../constants'
import { message } from '../i18n/messages'

interface Props {
  /** 概览快照（未取到为 null）。 */
  overview: Overview | null
  /** 展示态。 */
  state: ReadOnlyViewState
  /** 错误态文案键。 */
  textKey?: string
  /** 是否显示 Grafana 入口（工作台卡片为 false）。 */
  showGrafana?: boolean
  /** 是否显示轮询说明（工作台卡片为 false）。 */
  showPollingHint?: boolean
  /** 是否首次加载（区分「加载中」与「指标采集中」）。 */
  firstLoad?: boolean
  /** 最近一次成功刷新时刻（展示「上次更新」，失败时说明当前是旧值）。 */
  lastUpdated?: Date | null
}

const props = withDefaults(defineProps<Props>(), {
  textKey: '',
  showGrafana: true,
  showPollingHint: true,
  firstLoad: true,
  lastUpdated: null,
})

const emit = defineEmits<{ refresh: [] }>()

/** 是否已有可用快照（失败时保留上次成功值）。 */
const hasData = computed(() => props.overview !== null && props.overview.snapshot_available === true)
/** 数据通路是否就绪（轮询刷新期间沿用上次成功值，不闪空）。 */
const ready = computed(
  () => hasData.value && (props.state === 'ready' || props.state === 'error' || (props.state === 'loading' && !props.firstLoad)),
)
/** 降级文案（错误态按码映射；首次加载与冷启动分别给占位）。 */
const degradeText = computed(() => {
  if (props.state === 'error' && props.textKey !== '') {
    return message(props.textKey)
  }
  if (props.state === 'loading' && props.firstLoad) {
    return message('state.loading')
  }
  return message('state.collecting')
})
/** 上次成功刷新文案（未成功过不渲染）。 */
const lastUpdatedText = computed(() =>
  props.lastUpdated === null ? '' : message('msg.admin.common.lastUpdated').replace('{t}', props.lastUpdated.toLocaleString()),
)
/** 是否渲染 Grafana 入口。 */
const grafanaVisible = computed(() => props.showGrafana && GRAFANA_OVERVIEW_URL !== '' && props.state === 'ready')

/** 四张卡展示值（单位与口径说明与原型一致）。 */
const cards = computed(() => {
  const data = props.overview
  return [
    {
      key: 'throughput',
      title: message('msg.admin.overview.throughput'),
      value: (data?.messages_in_per_second ?? 0).toFixed(1),
      unit: '/s',
      caption: message('msg.admin.overview.throughputNote'),
    },
    {
      key: 'lag',
      title: message('msg.admin.overview.lag'),
      value: data?.consumer_lag ?? 0,
      unit: '',
      caption: message('msg.admin.overview.lagNote'),
    },
    {
      key: 'dlq',
      title: message('msg.admin.overview.dlq'),
      value: data?.dlq_total ?? 0,
      unit: '',
      caption: message('msg.admin.overview.dlqNote'),
    },
    {
      key: 'disk',
      title: message('msg.admin.overview.disk'),
      value: ((data?.disk_used_ratio ?? 0) * 100).toFixed(1),
      unit: '%',
      caption: message('msg.admin.overview.diskNote'),
    },
  ]
})
</script>

<template>
  <section class="overview-metrics" data-test="overview-metrics" :data-state="state">
    <div class="overview-metrics__head">
      <el-link
        v-if="grafanaVisible"
        type="primary"
        :href="GRAFANA_OVERVIEW_URL"
        target="_blank"
        rel="noopener"
        data-test="overview-grafana"
      >{{ message('msg.admin.common.grafana') }}</el-link>
      <el-button size="small" data-test="overview-refresh" @click="emit('refresh')">
        {{ message('msg.admin.common.refresh') }}
      </el-button>
    </div>
    <p v-if="showPollingHint" class="overview-metrics__hint" data-test="overview-hint">
      {{ message('msg.admin.overview.polling') }}
    </p>
    <p v-if="lastUpdatedText !== ''" class="overview-metrics__hint" data-test="overview-updated">
      {{ lastUpdatedText }}
    </p>
    <el-alert
      v-if="state === 'denied'"
      type="warning"
      :closable="false"
      show-icon
      :title="message('state.denied')"
      data-test="overview-denied"
    />
    <el-alert
      v-else-if="state === 'error'"
      type="error"
      :closable="false"
      show-icon
      :title="degradeText"
      data-test="overview-error"
    />
    <div v-if="state !== 'denied' && !(state === 'error' && !hasData)" class="overview-metrics__grid">
      <metric-card
        v-for="card in cards"
        :key="card.key"
        :ready="ready"
        :degrade-text="degradeText"
        :title="card.title"
        :value="card.value"
        :unit="card.unit"
        :caption="card.caption"
        :animate="false"
        :status-color="false"
        :data-test="`overview-card-${card.key}`"
      />
    </div>
  </section>
</template>

<style scoped>
.overview-metrics {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.overview-metrics__head {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--bms-space-2);
}
.overview-metrics__grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: var(--bms-space-2);
}
.overview-metrics__hint {
  margin: 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
</style>
