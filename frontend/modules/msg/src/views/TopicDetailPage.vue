<script setup lang="ts">
/** 主题详情（元数据 + 分区明细；写操作：扩分区 / 改保留期 / 删除；`deleting` 展示清理进度）。 */

import {
  ElAlert,
  ElButton,
  ElDescriptions,
  ElDescriptionsItem,
  ElDialog,
  ElForm,
  ElFormItem,
  ElInputNumber,
  ElTable,
  ElTableColumn,
} from 'element-plus'
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import MsgConfirmDialog from '../components/MsgConfirmDialog.vue'
import { errorCodeOf, errorKeyOf } from '../base/BaseMsgReadOnlyView'
import type { ImpactRow } from '../base/BaseMsgWriteAction'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { CLEANUP_POLL_INTERVAL_MS } from '../constants'
import { message } from '../i18n/messages'
import { hostPermissions } from '../runtime'
import {
  deleteTopic,
  expandPartitions,
  fetchDeleteImpact,
  fetchTopicDetail,
  loadPartitions,
  updateRetention,
  type DeleteImpact,
  type PartitionItem,
  type TopicDetail,
} from '../services/admin-service'

/** 清理步骤键（与后端 `CLEANUP_STEPS` 同序）。 */
const CLEANUP_STEPS: readonly string[] = ['messages', 'delay', 'offsets', 'groups', 'dlq', 'runtime', 'metadata']

const { params, go } = useAdminRoute()
const detail = ref<TopicDetail | null>(null)
const detailError = ref('')
const deleted = ref(false)
const view = useMsgReadOnlyView<PartitionItem>({ loader: loadPartitions, active: false, autoLoad: false })

/** 是否具备写权限（无 `msg:manage` 时写入口不渲染）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)
/** 是否处于清理中（此时不可读写，写入口全部不渲染）。 */
const deleting = computed(() => detail.value?.status === 'deleting')
/** 清理进度行（步骤 → 状态）。 */
const cleanupRows = computed<{ step: string; state: string }[]>(() => {
  const done = detail.value?.cleanup?.done_steps ?? []
  return CLEANUP_STEPS.map((step) => ({
    step: message(`msg.admin.topics.cleanup.step.${step}`),
    state: done.includes(step) ? message('msg.admin.topics.cleanup.done') : message('msg.admin.topics.cleanup.pending'),
  }))
})

const expandVisible = ref(false)
const retentionVisible = ref(false)
const deleteVisible = ref(false)
const impact = ref<DeleteImpact | null>(null)
const retentionForm = reactive({ retentionMs: 0, retentionBytes: 0 })
const expandForm = reactive({ target: 1 })

/** 扩分区动作。 */
const expandAction = useMsgWriteAction({
  runner: () => expandPartitions(params.topicId ?? '', expandForm.target),
})
/** 改保留期动作。 */
const retentionAction = useMsgWriteAction({
  runner: () =>
    updateRetention(params.topicId ?? '', {
      retention_ms: retentionForm.retentionMs,
      retention_bytes: retentionForm.retentionBytes,
    }),
})
/** 删主题动作（影响范围行由预检结果构造）。 */
const deleteAction = useMsgWriteAction({
  requiresConfirm: true,
  runner: () => deleteTopic(params.topicId ?? ''),
})
/** 删除确认的影响范围行。 */
const deleteRows = computed<ImpactRow[]>(() => {
  const data = impact.value
  if (data === null) {
    return []
  }
  return [
    { label: message('msg.admin.impact.partitions'), value: String(data.partition_count) },
    { label: message('msg.admin.impact.messages'), value: String(data.message_total) },
    { label: message('msg.admin.impact.groups'), value: String(data.group_count) },
    { label: message('msg.admin.impact.delayed'), value: String(data.delayed_total) },
    { label: message('msg.admin.impact.dlq'), value: String(data.dlq_total), tone: 'danger' },
  ]
})

let timer: ReturnType<typeof setInterval> | undefined

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

/** 停止进度轮询（幂等）。 */
function stopPolling(): void {
  if (timer !== undefined) {
    clearInterval(timer)
    timer = undefined
  }
}

/**
 * 按一次取数结果更新页面与轮询状态。
 *
 * @param data 主题详情（null 表示主题行已物理删除）。
 */
function applyDetail(data: TopicDetail | null): void {
  if (data === null) {
    detail.value = null
    deleted.value = true
    stopPolling()
    view.setActive(false)
    return
  }
  detail.value = data
  deleted.value = false
  if (data.status === 'deleting') {
    startPolling()
    return
  }
  stopPolling()
  view.setFilter('namespace', data.namespace)
  view.setFilter('topic', data.name)
  view.setActive(true)
}

/** 启动进度轮询（`deleting` 状态；主题行消失或状态变化即停）。 */
function startPolling(): void {
  if (timer !== undefined) {
    return
  }
  timer = setInterval(() => {
    void pollOnce()
  }, CLEANUP_POLL_INTERVAL_MS)
}

/** 轮询一次清理进度。 */
async function pollOnce(): Promise<void> {
  try {
    const data = await fetchTopicDetail(params.topicId ?? '')
    applyDetail(data)
  } catch (error) {
    if (errorCodeOf(error) === 350001) {
      applyDetail(null)
      return
    }
    detailError.value = message(errorKeyOf(errorCodeOf(error)))
  }
}

/** 载入主题元数据与分区明细。 */
async function load(): Promise<void> {
  detailError.value = ''
  try {
    const data = await fetchTopicDetail(params.topicId ?? '')
    applyDetail(data)
  } catch (error) {
    detailError.value = message(errorKeyOf(errorCodeOf(error)))
  }
}

/** 打开扩分区对话框（目标值由 `detail` 监听回填，这里只管清态与显隐）。 */
function openExpand(): void {
  expandAction.reset()
  expandVisible.value = true
}

/** 打开改保留期对话框（回填由 `detail` 监听承担）。 */
function openRetention(): void {
  retentionAction.reset()
  retentionVisible.value = true
}

/** 打开删除确认（先取影响范围，失败也允许继续）。 */
async function openDelete(): Promise<void> {
  deleteAction.reset()
  impact.value = null
  deleteVisible.value = true
  try {
    impact.value = await fetchDeleteImpact(params.topicId ?? '')
  } catch {
    impact.value = null
  }
}

/** 提交扩分区：成功后收起并刷新。 */
async function submitExpand(): Promise<void> {
  await expandAction.run()
  if (expandAction.status.value === 'done') {
    expandVisible.value = false
    await load()
  }
}

/** 提交改保留期：成功后收起并刷新。 */
async function submitRetention(): Promise<void> {
  await retentionAction.run()
  if (retentionAction.status.value === 'done') {
    retentionVisible.value = false
    await load()
  }
}

/** 确认删除：受理成功后收起弹窗并转为清理中。 */
async function confirmDelete(): Promise<void> {
  deleteAction.acknowledge()
  await deleteAction.run()
  if (deleteAction.status.value === 'done') {
    deleteVisible.value = false
    await load()
  }
}

// 详情到手即回填两个表单的初值：不在 `open*` 里重复判空，避免出现「永远走不到」的兜底分支。
watch(
  detail,
  (value) => {
    if (value === null) {
      return
    }
    expandForm.target = value.partition_count + 1
    retentionForm.retentionMs = value.retention_ms
    retentionForm.retentionBytes = value.retention_bytes
  },
  { immediate: true },
)

onMounted(load)
onUnmounted(stopPolling)
</script>

<template>
  <section class="admin-page" data-test="page-topic-detail">
    <div class="admin-page__head">
      <h1>{{ detail?.name ?? message('msg.admin.topics.detail') }}</h1>
      <div v-if="canManage && detail !== null && !deleting" class="admin-page__actions">
        <el-button data-test="topic-expand" @click="openExpand">{{ message('msg.admin.topics.expand.title') }}</el-button>
        <el-button data-test="topic-retention" @click="openRetention">
          {{ message('msg.admin.topics.retention.title') }}
        </el-button>
        <el-button type="danger" plain data-test="topic-delete" @click="openDelete">
          {{ message('msg.admin.topics.delete.title') }}
        </el-button>
      </div>
    </div>
    <el-alert
      v-if="detailError !== ''"
      type="error"
      :closable="false"
      show-icon
      :title="detailError"
      data-test="topic-detail-error"
    />
    <el-alert
      v-if="deleted"
      type="success"
      :closable="false"
      show-icon
      :title="message('msg.admin.topics.delete.completed')"
      data-test="topic-deleted"
    />
    <el-alert
      v-if="deleting"
      type="warning"
      :closable="false"
      show-icon
      :title="message('msg.admin.topics.delete.accepted')"
      data-test="topic-deleting"
    />
    <el-descriptions v-if="detail !== null" :column="3" border size="small" data-test="topic-detail-meta">
      <el-descriptions-item :label="message('msg.admin.topics.namespace')">{{ detail.namespace }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.partitionCount')">{{ detail.partition_count }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.status')">{{ detail.status }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.retention')">{{ detail.retention_ms }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.cleanupPolicy')">{{ detail.cleanup_policy }}</el-descriptions-item>
      <el-descriptions-item :label="message('msg.admin.topics.createdAt')">{{ detail.created_at }}</el-descriptions-item>
    </el-descriptions>

    <el-table
      v-if="deleting"
      :data="cleanupRows"
      size="small"
      border
      :empty-text="message('msg.admin.topics.cleanup.title')"
      data-test="topic-cleanup-table"
    >
      <el-table-column prop="step" :label="message('msg.admin.topics.cleanup.step')" min-width="200" />
      <el-table-column prop="state" :label="message('msg.admin.topics.cleanup.state')" min-width="120" />
    </el-table>

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

    <el-dialog
      v-if="detail !== null"
      v-model="expandVisible"
      :title="message('msg.admin.topics.expand.title')"
      width="440px"
      data-test="topic-expand-dialog"
    >
      <el-form label-width="120px">
        <el-form-item :label="message('msg.admin.topics.expand.current')">{{ detail.partition_count }}</el-form-item>
        <el-form-item :label="message('msg.admin.topics.expand.target')">
          <el-input-number
            v-model="expandForm.target"
            :min="detail.partition_count + 1"
            :max="1024"
            data-test="expand-target"
          />
        </el-form-item>
      </el-form>
      <p class="admin-page__hint">{{ message('msg.admin.topics.expand.notIncrease') }}</p>
      <p v-if="expandAction.errorKey.value !== ''" class="admin-page__error" data-test="expand-error">
        {{ message(expandAction.errorKey.value) }}
      </p>
      <template #footer>
        <el-button
          :disabled="expandAction.status.value === 'running'"
          data-test="expand-cancel"
          @click="expandVisible = false"
        >
          {{ message('msg.admin.common.cancel') }}
        </el-button>
        <el-button
          type="primary"
          :loading="expandAction.status.value === 'running'"
          :disabled="expandAction.status.value === 'running'"
          data-test="expand-submit"
          @click="submitExpand"
        >
          {{ message('msg.admin.common.confirm') }}
        </el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="retentionVisible"
      :title="message('msg.admin.topics.retention.title')"
      width="480px"
      data-test="topic-retention-dialog"
    >
      <el-form label-width="140px">
        <el-form-item :label="message('msg.admin.topics.create.retentionMs')">
          <el-input-number v-model="retentionForm.retentionMs" :min="1" data-test="retention-ms" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.retentionBytes')">
          <el-input-number v-model="retentionForm.retentionBytes" :min="0" data-test="retention-bytes" />
        </el-form-item>
      </el-form>
      <el-alert type="info" :closable="false" show-icon :title="message('msg.admin.topics.retention.effect')" />
      <p v-if="retentionAction.errorKey.value !== ''" class="admin-page__error" data-test="retention-error">
        {{ message(retentionAction.errorKey.value) }}
      </p>
      <template #footer>
        <el-button
          :disabled="retentionAction.status.value === 'running'"
          data-test="retention-cancel"
          @click="retentionVisible = false"
        >
          {{ message('msg.admin.common.cancel') }}
        </el-button>
        <el-button
          type="primary"
          :loading="retentionAction.status.value === 'running'"
          :disabled="retentionAction.status.value === 'running'"
          data-test="retention-submit"
          @click="submitRetention"
        >
          {{ message('msg.admin.common.save') }}
        </el-button>
      </template>
    </el-dialog>

    <msg-confirm-dialog
      v-model="deleteVisible"
      :title="message('msg.admin.topics.delete.title')"
      :rows="deleteRows"
      :warning="message('msg.admin.topics.delete.warning')"
      :danger="true"
      :loading="deleteAction.status.value === 'running'"
      :confirm-text="message('msg.admin.topics.delete.title')"
      data-test="topic-delete-dialog"
      @confirm="confirmDelete"
    />
    <p v-if="deleteAction.errorKey.value !== ''" class="admin-page__error" data-test="delete-error">
      {{ message(deleteAction.errorKey.value) }}
    </p>
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
  align-items: center;
  justify-content: space-between;
  gap: var(--bms-space-2);
}
.admin-page__actions {
  display: flex;
  gap: var(--bms-space-1);
}
.admin-page__hint {
  margin: 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.admin-page__error {
  margin: 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
</style>
