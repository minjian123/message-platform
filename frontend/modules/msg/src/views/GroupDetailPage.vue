<script setup lang="ts">
/** 消费组详情（状态 / 世代 / 成员与分配 / 各分区位点与积压；分区行可重置位点）。 */

import { ElButton, ElDescriptions, ElDescriptionsItem, ElInputNumber, ElTable, ElTableColumn } from 'element-plus'
import { computed, reactive, ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import MsgConfirmDialog from '../components/MsgConfirmDialog.vue'
import type { ImpactRow } from '../base/BaseMsgWriteAction'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { message } from '../i18n/messages'
import { hostPermissions } from '../runtime'
import { fetchGroupDetail, resetOffset } from '../services/admin-service'
import type { msg } from '@msg/api-types'

type Schema = msg.components['schemas']
type Member = Schema['GroupMemberItem']
type PartitionRow = Schema['GroupPartitionItem']

const { params, go } = useAdminRoute()
const meta = ref<Schema['GroupDetail'] | null>(null)
const members = ref<Member[]>([])
const namespace = params.namespace ?? ''
const topic = params.topic ?? ''
const group = params.group ?? ''

/** 是否具备写权限（无 `msg:manage` 时不渲染重置入口）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)

const resetVisible = ref(false)
const resetForm = reactive({ partition: 0, offset: 0 })
const resetTarget = ref<PartitionRow | null>(null)
/** 位点重置确认的影响范围行。 */
const resetRows = computed<ImpactRow[]>(() => {
  const row = resetTarget.value
  if (row === null) {
    return []
  }
  return [
    { label: message('msg.admin.groups.group'), value: group },
    { label: message('msg.admin.groups.topic'), value: topic },
    { label: message('msg.admin.partitions.partition'), value: String(row.partition) },
    {
      label: message('msg.admin.groups.reset.current'),
      value: row.committed === null || row.committed === undefined ? message('msg.admin.common.placeholder') : String(row.committed),
    },
    { label: message('msg.admin.groups.reset.target'), value: String(resetForm.offset), tone: 'danger' },
  ]
})

const view = useMsgReadOnlyView<PartitionRow>({
  loader: async () => {
    const data = await fetchGroupDetail(namespace, topic, group)
    meta.value = data
    members.value = data?.members ?? []
    const rows = data?.partitions ?? []
    return { items: rows, total: rows.length }
  },
})

/** 位点重置动作（`confirm` 只在这里显式传 true）。 */
const resetAction = useMsgWriteAction({
  requiresConfirm: true,
  runner: () =>
    resetOffset({
      namespace,
      topic,
      group,
      partition: resetForm.partition,
      offset: resetForm.offset,
      confirm: true,
    }),
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

/**
 * 打开某分区的位点重置确认（目标缺省为当前位点）。
 *
 * @param row 分区行。
 */
function openReset(row: unknown): void {
  const item = row as PartitionRow
  resetAction.reset()
  resetTarget.value = item
  resetForm.partition = item.partition
  resetForm.offset = item.committed ?? 0
  resetVisible.value = true
}

/** 确认重置：成功后收起并刷新。 */
async function confirmReset(): Promise<void> {
  resetAction.acknowledge()
  await resetAction.run()
  if (resetAction.status.value === 'done') {
    resetVisible.value = false
    await view.refresh()
  }
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
        <el-table-column v-if="canManage" :label="message('msg.admin.groups.reset.action')" min-width="130">
          <template #default="{ row }">
            <el-button link type="danger" data-test="group-reset" @click="openReset(row)">
              {{ message('msg.admin.groups.reset.action') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </read-only-list>

    <msg-confirm-dialog
      v-model="resetVisible"
      :title="message('msg.admin.groups.reset.title')"
      :rows="resetRows"
      :warning="message('msg.admin.groups.reset.warning')"
      :danger="true"
      :loading="resetAction.status.value === 'running'"
      :confirm-text="message('msg.admin.groups.reset.action')"
      data-test="group-reset-dialog"
      @confirm="confirmReset"
    >
      <el-input-number v-model="resetForm.offset" :min="0" data-test="reset-offset" />
    </msg-confirm-dialog>
    <p v-if="resetAction.errorKey.value !== ''" class="admin-page__error" data-test="group-reset-error">
      {{ message(resetAction.errorKey.value) }}
    </p>
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
.admin-page__error {
  margin: 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
</style>
