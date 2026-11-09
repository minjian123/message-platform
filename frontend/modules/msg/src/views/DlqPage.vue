<script setup lang="ts">
/**
 * 死信与重放页（`/msg/dlq`）：死信查询 + 详情 + 重放 / 忽略 / 归档。
 *
 * 契约：`GET /admin/dlq`、`GET /admin/dlq/{id}`、`POST .../replay|ignore|archive`（06-5-2）。
 * 重放任务列表只做展示，任务创建 / 暂停 / 取消与进度联动归 06-6。
 */

import {
  ElButton,
  ElDatePicker,
  ElDescriptions,
  ElDescriptionsItem,
  ElDrawer,
  ElInput,
  ElInputNumber,
  ElOption,
  ElSelect,
  ElTable,
  ElTableColumn,
  ElTag,
} from 'element-plus'
import { computed, reactive, ref } from 'vue'

import type { ImpactRow } from '../base/BaseMsgWriteAction'
import MsgConfirmDialog from '../components/MsgConfirmDialog.vue'
import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { message } from '../i18n/messages'
import { hostPermissions } from '../runtime'
import {
  archiveDeadLetter,
  fetchDeadLetter,
  ignoreDeadLetter,
  loadDeadLetters,
  replayDeadLetter,
  type DlqDetail,
  type DlqDisposeResult,
  type DlqItem,
} from '../services/admin-service'

const view = useMsgReadOnlyView<DlqItem>({ loader: loadDeadLetters })

const status = ref('')
const reason = ref('')
const topic = ref('')
const partition = ref<number | undefined>(undefined)
const failedFrom = ref('')
const failedTo = ref('')
const keyword = ref('')

/** 是否有处置权限（无 `msg:manage` 时操作列隐藏）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)

/** 状态 → 标签色。 */
const STATUS_TAG: Record<string, 'success' | 'warning' | 'danger' | 'info'> = {
  pending: 'warning',
  replaying: 'info',
  resolved: 'success',
  ignored: 'info',
  archived: 'danger',
}

/** 状态中文文案键。 */
const STATUS_TEXT: Record<string, string> = {
  pending: 'msg.admin.dlq.state.pending',
  replaying: 'msg.admin.dlq.state.replaying',
  resolved: 'msg.admin.dlq.state.resolved',
  ignored: 'msg.admin.dlq.state.ignored',
  archived: 'msg.admin.dlq.state.archived',
}

/** 原因中文文案键。 */
const REASON_TEXT: Record<string, string> = {
  delivery_exhausted: 'msg.admin.dlq.reason.delivery_exhausted',
  schema_invalid: 'msg.admin.dlq.reason.schema_invalid',
  processing_failed: 'msg.admin.dlq.reason.processing_failed',
}

/** 应用筛选。 */
function apply(): void {
  view.setFilter('status', status.value)
  view.setFilter('reason', reason.value)
  view.setFilter('topic', topic.value)
  view.setFilter('partition', partition.value)
  view.setFilter('failed_from', failedFrom.value)
  view.setFilter('failed_to', failedTo.value)
  view.setFilter('keyword', keyword.value)
}

/** 重置筛选。 */
function reset(): void {
  status.value = ''
  reason.value = ''
  topic.value = ''
  partition.value = undefined
  failedFrom.value = ''
  failedTo.value = ''
  keyword.value = ''
  view.reset()
}

/** 详情抽屉。 */
const detail = reactive<{ visible: boolean; loading: boolean; row: DlqDetail | null; error: string }>({
  visible: false,
  loading: false,
  row: null,
  error: '',
})

/**
 * 打开详情。
 *
 * @param row 列表行。
 */
async function openDetail(row: unknown): Promise<void> {
  const item = row as DlqItem
  detail.visible = true
  detail.loading = true
  detail.error = ''
  detail.row = null
  try {
    detail.row = await fetchDeadLetter(item.id)
  } catch {
    detail.error = message('msg.admin.dlq.detailFailed')
  } finally {
    detail.loading = false
  }
}

/** 处置对话框。 */
const dialog = reactive<{ visible: boolean; row: DlqItem | null; action: 'replay' | 'ignore' | 'archive'; rate: number; reason: string; error: string }>({
  visible: false,
  row: null,
  action: 'replay',
  rate: 100,
  reason: '',
  error: '',
})

/** 处置动作。 */
const write = useMsgWriteAction<DlqDisposeResult>({
  runner: () => submitDispose(),
  requiresConfirm: true,
})

/** 处置标题。 */
const dialogTitle = computed(() => {
  const key = { replay: 'msg.admin.dlq.action.replay', ignore: 'msg.admin.dlq.action.ignore', archive: 'msg.admin.dlq.action.archive' }[dialog.action]
  return message(key)
})

/** 影响范围行。 */
const impactRows = computed<ImpactRow[]>(() => {
  const row = dialog.row
  if (row === null) {
    return []
  }
  return [
    { label: message('msg.admin.dlq.topic'), value: row.topic },
    { label: message('msg.admin.dlq.partition'), value: String(row.partition_no) },
    { label: message('msg.admin.messages.offset'), value: String(row.offset) },
    { label: 'log_id', value: row.log_id },
  ]
})

/**
 * 打开处置对话框。
 *
 * @param row 列表行。
 * @param action 动作。
 */
function openDispose(row: unknown, action: 'replay' | 'ignore' | 'archive'): void {
  const item = row as DlqItem
  dialog.row = item
  dialog.action = action
  dialog.rate = 100
  dialog.reason = ''
  dialog.error = ''
  dialog.visible = true
  write.reset()
}

/**
 * 提交处置（由确认弹窗触发）。
 */
async function submitDispose(): Promise<DlqDisposeResult> {
  const row = dialog.row
  if (row === null) {
    throw new Error('no dead letter selected')
  }
  if (dialog.action === 'replay') {
    return replayDeadLetter(row.id, dialog.rate)
  }
  if (dialog.action === 'ignore') {
    return ignoreDeadLetter(row.id, dialog.reason)
  }
  return archiveDeadLetter(row.id, dialog.reason)
}

/** 确认处置。 */
async function confirmDispose(): Promise<void> {
  if (dialog.action === 'ignore' && dialog.reason.trim() === '') {
    dialog.error = message('msg.admin.dlq.reasonRequired')
    return
  }
  dialog.error = ''
  write.acknowledge()
  await write.run()
  if (write.status.value === 'done') {
    dialog.visible = false
    await view.refresh()
  } else if (write.errorKey.value !== '') {
    dialog.error = message(write.errorKey.value)
  }
}

/**
 * 状态文案。
 *
 * @param value 状态值。
 */
function statusText(value: string): string {
  return message(STATUS_TEXT[value] ?? 'msg.admin.dlq.status')
}

/**
 * 原因文案。
 *
 * @param value 原因值。
 */
function reasonText(value: string): string {
  return message(REASON_TEXT[value] ?? 'msg.admin.dlq.reason')
}
</script>

<template>
  <section class="admin-page" data-test="page-dlq">
    <h1>{{ message('msg.admin.dlq.title') }}</h1>
    <read-only-list
      :title="message('msg.admin.dlq.deadLetters')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :page="view.page.value"
      :page-size="view.pageSize.value"
      :total="view.total.value"
      @refresh="view.refresh"
      @update:page="view.setPage"
    >
      <template #filters>
        <div class="admin-page__filters">
          <el-select v-model="status" :placeholder="message('msg.admin.dlq.status')" clearable style="width: 150px" data-test="dlq-status">
            <el-option :label="message('msg.admin.dlq.state.pending')" value="pending" />
            <el-option :label="message('msg.admin.dlq.state.replaying')" value="replaying" />
            <el-option :label="message('msg.admin.dlq.state.resolved')" value="resolved" />
            <el-option :label="message('msg.admin.dlq.state.ignored')" value="ignored" />
            <el-option :label="message('msg.admin.dlq.state.archived')" value="archived" />
          </el-select>
          <el-select v-model="reason" :placeholder="message('msg.admin.dlq.reason')" clearable style="width: 160px" data-test="dlq-reason">
            <el-option :label="message('msg.admin.dlq.reason.delivery_exhausted')" value="delivery_exhausted" />
            <el-option :label="message('msg.admin.dlq.reason.schema_invalid')" value="schema_invalid" />
            <el-option :label="message('msg.admin.dlq.reason.processing_failed')" value="processing_failed" />
          </el-select>
          <el-input v-model="topic" :placeholder="message('msg.admin.dlq.topic')" clearable style="width: 160px" data-test="dlq-topic" />
          <el-input-number v-model="partition" :min="0" :controls="false" :placeholder="message('msg.admin.dlq.partition')" style="width: 110px" data-test="dlq-partition" />
          <el-date-picker v-model="failedFrom" type="datetime" value-format="YYYY-MM-DDTHH:mm:ss" :placeholder="message('msg.admin.dlq.failedFrom')" style="width: 190px" />
          <el-date-picker v-model="failedTo" type="datetime" value-format="YYYY-MM-DDTHH:mm:ss" :placeholder="message('msg.admin.dlq.failedTo')" style="width: 190px" />
          <el-input v-model="keyword" :placeholder="message('msg.admin.dlq.keyword')" clearable style="width: 200px" data-test="dlq-keyword" />
          <el-button type="primary" data-test="dlq-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="dlq-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="dlq-dead-table">
        <el-table-column :label="'log_id'" min-width="150">
          <template #default="{ row }">
            <el-button link type="primary" data-test="dlq-detail" @click="openDetail(row)">{{ row.log_id }}</el-button>
          </template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.dlq.topic')" min-width="150">
          <template #default="{ row }">{{ row.topic }} / {{ row.partition_no }}</template>
        </el-table-column>
        <el-table-column prop="offset" :label="message('msg.admin.messages.offset')" min-width="100" align="right" />
        <el-table-column :label="message('msg.admin.dlq.reason')" min-width="130">
          <template #default="{ row }">{{ reasonText(row.reason) }}</template>
        </el-table-column>
        <el-table-column prop="attempts" :label="message('msg.admin.dlq.attempts')" min-width="90" align="right" />
        <el-table-column :label="message('msg.admin.dlq.status')" min-width="100">
          <template #default="{ row }">
            <el-tag :type="STATUS_TAG[row.status] ?? 'info'" size="small">{{ statusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="failed_at" :label="message('msg.admin.dlq.failedAt')" min-width="170" />
        <el-table-column prop="trace_id" :label="'trace_id'" min-width="130" />
        <el-table-column v-if="canManage" :label="message('msg.admin.common.actions')" min-width="210" fixed="right">
          <template #default="{ row }">
            <el-button v-if="row.status === 'pending'" link type="primary" data-test="dlq-replay" @click="openDispose(row, 'replay')">
              {{ message('msg.admin.dlq.action.replay') }}
            </el-button>
            <el-button v-if="row.status === 'pending'" link type="warning" data-test="dlq-ignore" @click="openDispose(row, 'ignore')">
              {{ message('msg.admin.dlq.action.ignore') }}
            </el-button>
            <el-button
              v-if="row.status === 'pending' || row.status === 'ignored' || row.status === 'resolved'"
              link
              type="info"
              data-test="dlq-archive"
              @click="openDispose(row, 'archive')"
            >
              {{ message('msg.admin.dlq.action.archive') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </read-only-list>

    <read-only-list
      :title="message('msg.admin.dlq.replayTasks')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :paginated="false"
      :skeleton="false"
    >
      <el-table :data="[]" size="small" border data-test="dlq-replay-table" :empty-text="message('msg.admin.dlq.replayLater')">
        <el-table-column prop="task_id" :label="message('msg.admin.dlq.task')" min-width="140" />
        <el-table-column prop="topic" :label="message('msg.admin.dlq.topic')" min-width="140" />
        <el-table-column prop="progress" :label="message('msg.admin.dlq.progress')" min-width="100" />
        <el-table-column prop="status" :label="message('msg.admin.dlq.status')" min-width="100" />
      </el-table>
    </read-only-list>

    <!-- 详情抽屉 -->
    <el-drawer v-model="detail.visible" :title="message('msg.admin.dlq.detailTitle')" size="480px" data-test="dlq-detail-drawer">
      <div v-if="detail.loading" data-test="dlq-detail-loading">{{ message('state.loading') }}</div>
      <el-alert v-else-if="detail.error !== ''" type="error" :closable="false" :title="detail.error" />
      <template v-else-if="detail.row">
        <el-descriptions :column="1" border size="small">
          <el-descriptions-item :label="'log_id'">{{ detail.row.log_id }}</el-descriptions-item>
          <el-descriptions-item :label="'message_id'">{{ detail.row.message_id }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.topic')">{{ detail.row.topic }} / {{ detail.row.partition_no }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.messages.offset')">{{ detail.row.offset }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.reason')">{{ reasonText(detail.row.reason) }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.attempts')">{{ detail.row.attempts }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.failedAt')">{{ detail.row.failed_at }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.status')">{{ statusText(detail.row.status) }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.replayTask')">{{ detail.row.replay_task_id ?? '—' }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.dispositionReason')">{{ detail.row.disposition_reason ?? '—' }}</el-descriptions-item>
          <el-descriptions-item :label="message('msg.admin.dlq.disposedAt')">{{ detail.row.disposed_at ?? '—' }}</el-descriptions-item>
        </el-descriptions>
        <h4>{{ message('msg.admin.dlq.failureDetail') }}</h4>
        <pre class="dlq-detail__json">{{ JSON.stringify(detail.row.detail, null, 2) }}</pre>
      </template>
    </el-drawer>

    <!-- 处置确认 -->
    <msg-confirm-dialog
      v-model="dialog.visible"
      :title="dialogTitle"
      :rows="impactRows"
      :warning="dialog.action === 'archive' ? message('msg.admin.dlq.archiveWarning') : ''"
      :danger="dialog.action === 'archive'"
      :loading="write.status.value === 'running'"
      @confirm="confirmDispose"
    >
      <div v-if="dialog.action === 'replay'" class="dlq-dialog__field">
        <span>{{ message('msg.admin.dlq.rate') }}</span>
        <el-input-number v-model="dialog.rate" :min="1" :max="1000" :step="100" data-test="dlq-rate" />
      </div>
      <div v-if="dialog.action === 'ignore' || dialog.action === 'archive'" class="dlq-dialog__field">
        <span>{{ message('msg.admin.dlq.dispositionReason') }}</span>
        <el-input
          v-model="dialog.reason"
          type="textarea"
          :rows="3"
          maxlength="256"
          show-word-limit
          :placeholder="dialog.action === 'ignore' ? message('msg.admin.dlq.reasonRequired') : message('msg.admin.dlq.reasonOptional')"
          data-test="dlq-disposition-reason"
        />
      </div>
      <p v-if="dialog.error !== ''" class="dlq-dialog__error" data-test="dlq-dispose-error">{{ dialog.error }}</p>
    </msg-confirm-dialog>
  </section>
</template>

<style scoped>
.admin-page {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.admin-page__filters {
  display: flex;
  gap: var(--bms-space-2);
  flex-wrap: wrap;
  align-items: center;
}
.dlq-detail__json {
  background: var(--bms-color-fill-light);
  border-radius: 6px;
  padding: var(--bms-space-2);
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-all;
}
.dlq-dialog__field {
  display: flex;
  gap: var(--bms-space-2);
  align-items: center;
  margin-top: var(--bms-space-2);
}
.dlq-dialog__error {
  margin: var(--bms-space-2) 0 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
</style>
