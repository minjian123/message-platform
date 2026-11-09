<script setup lang="ts">
/**
 * 租户用量四卡（独立页与租户记录页页签共用）。
 *
 * 只渲染：列元数据由端点给定（标题取 `label`），数值交给平台指标卡按 `number` 口径千分位，
 * 字节列先拆成「数值 + 单位」再交指标卡（自适应单位由 `splitBytes` 决定）。
 * 状态由调用方传入——本组件不取数、不判权限。
 *
 * 两种未就绪形态：**首次加载**（无数据）出四块骨架；**刷新中**（已有数据）保留旧值，
 * 避免把已看到的数字换成「加载中」造成闪烁。单列不可用（`value=null`）走指标卡的占位态，
 * 而不是把字符串当值传进去（指标卡把非数字值判为空值并渲染 `—`）。
 */
import { MetricCard } from '@bms/ui_ep'
import { ElSkeleton } from 'element-plus'
import { computed } from 'vue'

import type { ReadOnlyViewState } from '../base/BaseMsgReadOnlyView'
import { message } from '../i18n/messages'
import { STORAGE_BYTES_COLUMN, splitBytes } from '../services/usage-format'
import { captionKeyOf, type UsageCardRow } from '../services/usage-service'

interface Props {
  /** 卡片行（按端点列序）。 */
  rows: UsageCardRow[]
  /** 展示态（用于首次加载骨架；刷新中保留旧值）。 */
  state: ReadOnlyViewState
  /** 栅格列数：独立页 4（一行四列）、页签 2（2×2）。 */
  layout?: 4 | 2
}

const props = withDefaults(defineProps<Props>(), { layout: 4 })

/** 骨架卡数量（列数未知时给四块，与四列对齐）。 */
const SKELETON_COUNT = 4

/** 是否首次加载（无数据且加载中）→ 出骨架。 */
const skeleton = computed(() => props.state === 'loading' && props.rows.length === 0)
/** 不可用占位文案（单列值为 null 时）。 */
const degradeText = computed(() => message('msg.quota.state.unavailable'))

/**
 * 卡片数值（字节列换算为自适应单位后的数值，其余原样）。
 *
 * @param row 卡片行。
 * @returns 数值（`null` 走占位态）。
 */
function cardValue(row: UsageCardRow): number | null {
  if (row.value === null) {
    return null
  }
  return row.key === STORAGE_BYTES_COLUMN ? splitBytes(row.value).amount : row.value
}

/**
 * 卡片单位（仅字节列有；非字节列为空串）。
 *
 * @param row 卡片行。
 * @returns 单位文本。
 */
function cardUnit(row: UsageCardRow): string {
  if (row.key !== STORAGE_BYTES_COLUMN || row.value === null) {
    return ''
  }
  return splitBytes(row.value).unit
}
</script>

<template>
  <div class="usage-cards" :class="`usage-cards--${layout}`" data-test="usage-cards" :data-layout="layout">
    <template v-if="skeleton">
      <el-skeleton v-for="n in SKELETON_COUNT" :key="n" :rows="2" animated data-test="usage-skeleton" />
    </template>
    <template v-else>
      <metric-card
        v-for="row in rows"
        :key="row.key"
        :ready="row.value !== null"
        :degrade-text="degradeText"
        :title="row.label"
        :value="cardValue(row)"
        :unit="cardUnit(row)"
        :format="'number'"
        :animate="false"
        :status-color="false"
        :caption="message(captionKeyOf(row.key))"
        :data-test="`usage-card-${row.key}`"
      >
        <!-- 单列不可用：指标卡占位态默认只渲染一句降级文案，会丢掉列名与口径，
             与其余三卡结构不一致；用退化插槽补齐标题与脚注。 -->
        <template #degrade>
          <div class="usage-card__head" data-test="usage-card-head">{{ row.label }}</div>
          <div class="usage-card__degraded" data-test="usage-card-degraded">{{ degradeText }}</div>
          <div class="usage-card__note">{{ message(captionKeyOf(row.key)) }}</div>
        </template>
      </metric-card>
    </template>
  </div>
</template>

<style scoped>
.usage-cards {
  display: grid;
  gap: var(--bms-space-2);
}
.usage-cards--4 {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}
.usage-cards--2 {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}
/* 平台指标卡自带「刷新」按钮；本场景刷新是整页 / 整页签的统一入口，
   四张卡各带一个刷新既重复又会在宿主页签里挤占宽度，故隐藏卡内入口。 */
.usage-cards :deep(.bms-metric-card__refresh) {
  display: none;
}
/* 单列不可用的退化插槽：补齐标题与脚注，保持与其余三卡同构。 */
.usage-card__head {
  color: var(--bms-color-text-secondary);
  font-size: 13px;
}
.usage-card__degraded {
  color: var(--bms-color-warning);
  font-size: 16px;
}
.usage-card__note {
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
</style>
