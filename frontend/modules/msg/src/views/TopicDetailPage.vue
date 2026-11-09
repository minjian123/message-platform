<script setup lang="ts">
/** 主题详情（元数据 + 分区明细；HW / ISR 为可空占位，显示 `—` 不伪造数值）。 */

import { ElAlert, ElDescriptions, ElDescriptionsItem, ElTable, ElTableColumn } from 'element-plus'
import { onMounted, ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { errorCodeOf, errorKeyOf } from '../base/BaseMsgReadOnlyView'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import {
  fetchTopicDetail,
  loadPartitions,
  type PartitionItem,
  type TopicDetail,
} from '../services/admin-service'

const { params, go } = useAdminRoute()
const detail = ref<TopicDetail | null>(null)
const detailError = ref('')
const view = useMsgReadOnlyView<PartitionItem>({ loader: loadPartitions, active: false, autoLoad: false })

/**
 * 可空字段展示（含空数组），统一显示占位符。
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

/** 载入主题元数据，拿到命名空间与主题名后接线分区明细。 */
async function load(): Promise<void> {
  detailError.value = ''
  try {
    const data = await fetchTopicDetail(params.topicId ?? '')
    detail.value = data
    if (data === null) {
      view.setActive(false)
      return
    }
    view.setFilter('namespace', data.namespace)
    view.setFilter('topic', data.name)
    view.setActive(true)
  } catch (error) {
    detailError.value = message(errorKeyOf(errorCodeOf(error)))
  }
}

onMounted(load)
</script>

<template>
  <section class="admin-page" data-test="page-topic-detail">
    <h1>{{ detail?.name ?? message('msg.admin.topics.detail') }}</h1>
    <el-alert
      v-if="detailError !== ''"
      type="error"
      :closable="false"
      show-icon
      :title="detailError"
      data-test="topic-detail-error"
    />
    <el-descriptions v-if="detail !== null" :column="3" border size="small" data-test="topic-detail-meta">
      <el-descriptions-item :label="message('msg.admin.topics.namespace')">{{ detail.namespace }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.partitionCount')">{{ detail.partition_count }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.status')">{{ detail.status }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.retention')">{{ detail.retention_ms }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.cleanupPolicy')">{{ detail.cleanup_policy }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.createdAt')">{{ detail.created_at }}</el-descriptions-item>
    </el-descriptions>
    <read-only-list
      :title="message('msg.admin.topics.partitionDetail')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :paginated="false"
      :back="true"
      @refresh="load"
      @back="go('/msg/topics')"
    >
      <el-table :data="view.rows.value" size="small" border data-test="topic-partitions-table">
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
</style>
