<script setup lang="ts">
/**
 * 分区查询页（分区接口必填 `namespace` + `topic`，故先选主题再展示明细）。
 *
 * 未选主题为「待选条件」态，不发起请求；从主题详情深链带入参数时直接取数。
 */

import { ElButton, ElInput, ElTable, ElTableColumn } from 'element-plus'
import { ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadPartitions, type PartitionItem } from '../services/admin-service'

const { query } = useAdminRoute()
const namespace = ref(query.namespace ?? '')
const topic = ref(query.topic ?? '')
const ready = namespace.value !== '' && topic.value !== ''

const view = useMsgReadOnlyView<PartitionItem>({
  loader: loadPartitions,
  initialFilters: ready ? { namespace: namespace.value, topic: topic.value } : {},
  active: ready,
  autoLoad: ready,
})

/** 应用必填条件并取数；缺参退回「待选条件」态。 */
function apply(): void {
  if (namespace.value === '' || topic.value === '') {
    view.setActive(false)
    return
  }
  view.setFilter('namespace', namespace.value)
  view.setFilter('topic', topic.value)
  view.setActive(true)
}

/** 重置条件。 */
function reset(): void {
  namespace.value = ''
  topic.value = ''
  view.reset()
  view.setActive(false)
}

/**
 * 可空字段展示。
 *
 * @param value 字段值。
 * @returns 展示文本。
 */
function display(value: unknown): string {
  if (value === null || value === undefined || value === '') {
    return message('msg.admin.common.placeholder')
  }
  if (Array.isArray(value) && value.length === 0) {
    return message('msg.admin.common.placeholder')
  }
  return String(value)
}
</script>

<template>
  <section class="admin-page" data-test="page-partitions">
    <h1>{{ message('msg.admin.partitions.title') }}</h1>
    <p class="admin-page__hint" data-test="partitions-choose-hint">{{ message('msg.admin.partitions.choose') }}</p>
    <read-only-list
      :title="message('msg.admin.partitions.title')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :paginated="false"
      @refresh="view.refresh"
    >
      <template #filters>
        <div class="admin-page__filters">
          <el-input v-model="namespace" :placeholder="message('msg.admin.topics.namespace')" data-test="partitions-namespace" />
          <el-input v-model="topic" :placeholder="message('msg.admin.topics.name')" data-test="partitions-topic" />
          <el-button type="primary" data-test="partitions-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="partitions-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="partitions-table">
        <el-table-column prop="partition" :label="message('msg.admin.partitions.partition')" min-width="80" />
        <el-table-column prop="leader" :label="message('msg.admin.partitions.leader')" min-width="110">
          <template #default="{ row }">{{ display(row.leader) }}</template>
        </el-table-column>
        <el-table-column prop="leo" :label="message('msg.admin.partitions.leo')" min-width="90" />
        <el-table-column :label="message('msg.admin.partitions.hw')" min-width="90">
          <template #default="{ row }">{{ display(row.hw) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.partitions.isr')" min-width="90">
          <template #default="{ row }">{{ display(row.isr) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.partitions.committed')" min-width="110">
          <template #default="{ row }">{{ display(row.committed) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.partitions.lag')" min-width="90">
          <template #default="{ row }">{{ display(row.lag) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.partitions.owner')" min-width="110">
          <template #default="{ row }">{{ display(row.owner) }}</template>
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
</style>
