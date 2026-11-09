<script setup lang="ts">
/** 消费组详情（状态 / 世代 / 成员与分配 / 各分区位点与积压）。 */

import { ElDescriptions, ElDescriptionsItem, ElTable, ElTableColumn } from 'element-plus'
import { ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import type { msg } from '@msg/api-types'
import { fetchGroupDetail } from '../services/admin-service'

type Schema = msg.components['schemas']
type Member = Schema['GroupMemberItem']
type PartitionRow = Schema['GroupPartitionItem']

const { params, go } = useAdminRoute()
const meta = ref<Schema['GroupDetail'] | null>(null)
const members = ref<Member[]>([])
const namespace = params.namespace ?? ''
const topic = params.topic ?? ''
const group = params.group ?? ''

const view = useMsgReadOnlyView<PartitionRow>({
  loader: async () => {
    const data = await fetchGroupDetail(namespace, topic, group)
    meta.value = data
    members.value = data?.members ?? []
    const rows = data?.partitions ?? []
    return { items: rows, total: rows.length }
  },
})

/**
 * 成员分配分区展示。
 *
 * @param row 成员行。
 * @returns 分区清单文本。
 */
function assigned(row: unknown): string {
  return (row as Member).partitions.map((value) => String(value)).join(', ')
}
</script>

<template>
  <section class="admin-page" data-test="page-group-detail">
    <h1>{{ meta?.group ?? group }}</h1>
    <el-descriptions v-if="meta !== null" :column="3" border size="small" data-test="group-detail-meta">
      <el-descriptions-item :label="message('msg.admin.groups.namespace')">{{ meta.namespace }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.groups.topic')">{{ meta.topic }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.groups.state')">{{ meta.state }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.groups.generation')">{{ meta.generation }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.partitionCount')">{{ meta.partition_count }}</el-descriptions-item>
    </el-descriptions>
    <h2>{{ message('msg.admin.groups.members') }}</h2>
    <el-table :data="members" size="small" border data-test="group-members-table">
      <el-table-column prop="consumer_id" :label="message('msg.admin.groups.member')" min-width="160" />
      <el-table-column :label="message('msg.admin.groups.assigned')" min-width="180">
        <template #default="{ row }">{{ assigned(row) }}</template>
      </el-table-column>
      <el-table-column prop="last_seen" :label="message('msg.admin.groups.lastSeen')" min-width="180" />
    </el-table>
    <read-only-list
      :title="message('msg.admin.groups.detail')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :paginated="false"
      :back="true"
      @refresh="view.refresh"
      @back="go('/msg/groups')"
    >
      <el-table :data="view.rows.value" size="small" border data-test="group-partitions-table">
        <el-table-column prop="partition" :label="message('msg.admin.partitions.partition')" min-width="80" />
        <el-table-column prop="leo" :label="message('msg.admin.partitions.leo')" min-width="100" />
        <el-table-column prop="committed" :label="message('msg.admin.partitions.committed')" min-width="120" />
        <el-table-column prop="lag" :label="message('msg.admin.groups.lag')" min-width="100" />
        <el-table-column prop="owner" :label="message('msg.admin.partitions.owner')" min-width="120" />
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
.admin-page h2 {
  margin: 0;
  font-size: 15px;
}
</style>
