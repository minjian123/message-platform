<script setup lang="ts">
/** 延迟消息（待投列表 + 九档分布 + 堆积提示；只读，不提供改期与取消）。 */

import {
  ElAlert,
  ElButton,
  ElDescriptions,
  ElDescriptionsItem,
  ElInput,
  ElOption,
  ElSelect,
  ElTable,
  ElTableColumn,
} from 'element-plus'
import { onMounted, ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { errorCodeOf, errorKeyOf } from '../base/BaseMsgReadOnlyView'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { DELAY_LEVELS } from '../constants'
import { message } from '../i18n/messages'
import {
  fetchDelaySummary,
  loadDelayed,
  type DelaySummary,
  type DelayedItem,
} from '../services/admin-service'
import { durationText, toLocalText } from '../services/time-format'

const view = useMsgReadOnlyView<DelayedItem>({ loader: loadDelayed })
const summary = ref<DelaySummary | null>(null)
const summaryError = ref('')
const namespace = ref('')
const topic = ref('')
const partition = ref('')
const level = ref('')

/** 当前筛选条件（空值不落参）。 */
function currentFilters(): Record<string, string> {
  return {
    namespace: namespace.value.trim(),
    topic: topic.value.trim(),
    partition: partition.value.trim(),
    level: level.value,
  }
}

/** 取档位分布（失败只降级汇总区，不阻断待投列表）。 */
async function loadSummary(): Promise<void> {
  summaryError.value = ''
  const filters = currentFilters()
  try {
    summary.value = await fetchDelaySummary({
      namespace: filters.namespace === '' ? undefined : filters.namespace,
      topic: filters.topic === '' ? undefined : filters.topic,
    })
  } catch (error) {
    summaryError.value = message(errorKeyOf(errorCodeOf(error)))
  }
}

/** 应用筛选（一次提交全部条件，只发一个列表请求）。 */
function apply(): void {
  view.setFilters(currentFilters())
  void loadSummary()
}

/** 重置筛选（先清表单，再一次性提交空条件）。 */
function reset(): void {
  namespace.value = ''
  topic.value = ''
  partition.value = ''
  level.value = ''
  view.setFilters({})
  void loadSummary()
}

/**
 * 档位占比（当档条数 / 待投总数，保留一位小数）。
 *
 * @param count 当档条数。
 * @param total 待投总数。
 * @returns 占比文本。
 */
function percentText(count: number, total: number): string {
  if (total <= 0) {
    return '0.0%'
  }
  return `${((count / total) * 100).toFixed(1)}%`
}

onMounted(loadSummary)
</script>

<template>
  <section class="admin-page" data-test="page-delayed">
    <p class="admin-page__hint" data-test="delayed-readonly">{{ message('msg.admin.delayed.readonly') }}</p>
    <el-alert
      v-if="summaryError !== ''"
      type="error"
      :closable="false"
      show-icon
      :title="summaryError"
      data-test="delayed-summary-error"
    />
    <section v-else data-test="delayed-summary">
      <el-descriptions :column="3" border size="small">
        <el-descriptions-item :label="message('msg.admin.delayed.total')">{{ summary?.total ?? 0 }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.delayed.staleCount')">{{ summary?.stale_count ?? 0 }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.delayed.quotaUsedRatio')">{{ summary?.quota_used_ratio ?? 0 }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.delayed.hint')">
          <span data-test="delayed-hint">{{ message(`hint.${summary?.hint ?? 'normal'}`) }}</span>
        </el-descriptions-item>
      </el-descriptions>
      <h2>{{ message('msg.admin.delayed.distribution') }}</h2>
      <el-table :data="summary?.levels ?? []" size="small" border data-test="delayed-levels-table">
        <el-table-column prop="level" :label="message('msg.admin.delayed.level')" min-width="100" />
        <el-table-column prop="count" :label="message('msg.admin.delayed.count')" min-width="100" />
        <el-table-column :label="message('msg.admin.delayed.percent')" min-width="100">
          <template #default="{ row }">{{ percentText((row as { count: number }).count, summary?.total ?? 0) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.delayed.minRemaining')" min-width="140">
          <template #default="{ row }">{{ durationText((row as { min_remaining_ms: number }).min_remaining_ms) }}</template>
        </el-table-column>
      </el-table>
    </section>
    <read-only-list
      :title="message('msg.admin.delayed.title')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :page="view.page.value"
      :page-size="view.pageSize.value"
      :total="view.total.value"
      :skeleton="view.firstLoad.value"
      @refresh="view.refresh"
      @update:page="view.setPage"
    >
      <template #filters>
        <div class="admin-page__filters">
          <el-input v-model="namespace" :placeholder="message('msg.admin.topics.namespace')" data-test="delayed-namespace" />
          <el-input v-model="topic" :placeholder="message('msg.admin.delayed.topicFilter')" data-test="delayed-topic" />
          <el-input v-model="partition" :placeholder="message('msg.admin.delayed.partitionFilter')" data-test="delayed-partition" />
          <el-select v-model="level" :placeholder="message('msg.admin.delayed.levelFilter')" clearable data-test="delayed-level">
            <el-option v-for="item in DELAY_LEVELS" :key="item" :label="item" :value="item" />
          </el-select>
          <el-button type="primary" data-test="delayed-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="delayed-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="delayed-table">
        <el-table-column prop="log_id" :label="message('msg.admin.messages.logId')" min-width="180" />
        <el-table-column prop="topic" :label="message('msg.admin.groups.topic')" min-width="140" />
        <el-table-column prop="partition" :label="message('msg.admin.partitions.partition')" min-width="80" />
        <el-table-column prop="level" :label="message('msg.admin.delayed.level')" min-width="90" />
        <el-table-column :label="message('msg.admin.delayed.deliverAt')" min-width="180">
          <template #default="{ row }">{{ toLocalText((row as DelayedItem).deliver_at) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.delayed.remainingMs')" min-width="120">
          <template #default="{ row }">{{ durationText((row as DelayedItem).remaining_ms) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.messages.traceId')" min-width="120">
          <template #default>{{ message('msg.admin.common.placeholder') }}</template>
        </el-table-column>
      </el-table>
    </read-only-list>
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
.admin-page__filters {
  display: flex;
  gap: var(--bms-space-2);
  flex-wrap: wrap;
}
.admin-page__filters :deep(.el-input) {
  max-width: 200px;
}
.admin-page h2 {
  margin: 0;
  font-size: 15px;
}
</style>
