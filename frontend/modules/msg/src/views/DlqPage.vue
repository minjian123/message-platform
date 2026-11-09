<script setup lang="ts">
/**
 * 死信与重放页（`/msg/dlq`）：页签一「死信」查询 + 详情 + 重放 / 忽略 / 归档；
 * 页签二「重放任务」由 `ReplayTaskPanel` 承载（06-6-2：创建 / 列表 / 详情 / 暂停 / 恢复 / 取消）。
 *
 * 契约：`GET /admin/dlq`、`GET /admin/dlq/{id}`、`POST .../replay|ignore|archive`（06-5-2）；
 * 重放任务五个端点见 `admin-service.ts`（06-6-1）。死信重放成功后切到重放页签并高亮新任务。
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
  ElTabPane,
  ElTable,
  ElTableColumn,
  ElTabs,
  ElTag,
} from 'element-plus'
import { computed, onMounted, reactive, ref } from 'vue'

import type { ImpactRow } from '../base/BaseMsgWriteAction'
import MsgConfirmDialog from '../components/MsgConfirmDialog.vue'
import ReadOnlyList from '../components/ReadOnlyList.vue'
import ReplayTaskPanel from '../components/ReplayTaskPanel.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { message } from '../i18n/messages'
import { hostPermissions } from '../runtime'
import {
  archiveDeadLetter,
  fetchDeadLetter,
  fetchReplayTask,
  ignoreDeadLetter,
  loadDeadLetters,
  replayDeadLetter,
  type DlqDetail,
  type DlqDisposeResult,
  type DlqItem,
  type ReplayTaskItem,
} from '../services/admin-service'

const view = useMsgReadOnlyView<DlqItem>({ loader: loadDeadLetters })
const adminRoute = useAdminRoute()

const status = ref('')
const reason = ref('')
const topic = ref('')
const partition = ref<number | undefined>(undefined)
const failedFrom = ref('')
const failedTo = ref('')
const keyword = ref('')

/** 是否有处置权限（无 `msg:manage` 时操作列隐藏）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)

/** 当前页签（死信 / 重放任务）。 */
const tab = ref<'dlq' | 'replay'>('dlq')
/** 需要在重放页签高亮的任务 ID（死信重放成功 / 新建成功后）。 */
const highlightTask = ref('')
/** 刷新令牌：自增一次即让重放页签重新取数。 */
const replayRefreshToken = ref(0)
/** 需要打开详情的任务 ID（死信详情跳转 / `?task=` 深链）。 */
const replayFocusTaskId = ref('')
/** 死信关联的重放任务（详情抽屉里展示状态与进度）。 */
const related = reactive<{ loading: boolean; task: ReplayTaskItem | null }>({ loading: false, task: null })

/**
 * 取死信关联的重放任务（失败降级为「—」，不阻断详情）。
 *
 * @param taskId 重放任务 ID。
 */
async function loadRelated(taskId: string): Promise<void> {
  related.loading = true
  try {
    related.task = await fetchReplayTask(taskId)
  } catch {
    related.task = null
  } finally {
    related.loading = false
  }
}

/**
 * 跳到重放页签并打开任务详情。
 *
 * @param taskId 重放任务 ID。
 */
function goReplayTask(taskId: string): void {
  detail.visible = false
  tab.value = 'replay'
  highlightTask.value = taskId
  replayFocusTaskId.value = taskId
}

/**
 * 新建重放任务成功：切页签并高亮（列表刷新由页签自身完成）。
 *
 * @param taskId 新任务 ID。
 */
function onReplayCreated(taskId: string): void {
  tab.value = 'replay'
  highlightTask.value = taskId
}

onMounted(() => {
  const deepLink = adminRoute.query.task ?? ''
  if (deepLink !== '') {
    tab.value = 'replay'
    highlightTask.value = deepLink
    replayFocusTaskId.value = deepLink
  }
})

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

/** 当前死信关联的重放任务 ID（空串 = 未关联）。 */
const relatedTaskId = computed(() => detail.row?.replay_task_id ?? '')

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
  related.task = null
  try {
    const row = await fetchDeadLetter(item.id)
    detail.row = row
    // 详情主体先出（关联任务可能还要再取一次），避免整抽屉被关联取数拖住
    detail.loading = false
    const taskId = row?.replay_task_id ?? ''
    if (taskId !== '') {
      await loadRelated(taskId)
    }
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
  // 弹窗只经 openDispose 打开，打开即带行；此处断言类型而非再判一次空（分支不可达）
  const row = dialog.row as DlqItem
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
  if (write.status.value !== 'done') {
    dialog.error = message(write.errorKey.value)
    return
  }
  const action = dialog.action
  dialog.visible = false
  await view.refresh()
  if (action === 'replay') {
    // 重放处置成功后后端必带任务 ID（契约 `DlqDisposeResult.replay_task_id`）
    const created = write.result.value as DlqDisposeResult
    tab.value = 'replay'
    highlightTask.value = created.replay_task_id as string
    replayRefreshToken.value += 1
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
    <el-tabs v-model="tab" data-test="dlq-tabs">
      <el-tab-pane :label="message('msg.admin.dlq.deadLetters')" name="dlq">
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

      </el-tab-pane>

      <el-tab-pane :label="message('msg.admin.replay.tab')" name="replay">
        <replay-task-panel
          :highlight-task="highlightTask"
          :refresh-token="replayRefreshToken"
          :focus-task-id="replayFocusTaskId"
          @created="onReplayCreated"
        />
      </el-tab-pane>
    </el-tabs>

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

        <template v-if="relatedTaskId !== ''">
          <h4>{{ message('msg.admin.dlq.relatedTask') }}</h4>
          <p v-if="related.loading" class="dlq-detail__hint" data-test="dlq-related-loading">{{ message('state.loading') }}</p>
          <el-descriptions v-else-if="related.task" :column="1" border size="small" data-test="dlq-related-task">
            <el-descriptions-item :label="message('msg.admin.replay.task')">{{ related.task.task_id }}</el-descriptions-item>
            <el-descriptions-item :label="message('msg.admin.replay.status')">
              {{ message(`msg.admin.replay.status.${related.task.status}`) }}
            </el-descriptions-item>
            <el-descriptions-item :label="message('msg.admin.replay.progress')">
              {{ related.task.replayed }} / {{ related.task.total }}
            </el-descriptions-item>
          </el-descriptions>
          <p v-else class="dlq-detail__hint">{{ message('msg.admin.common.placeholder') }}</p>
          <el-button link type="primary" data-test="dlq-related-open" @click="goReplayTask(relatedTaskId)">
            {{ message('msg.admin.dlq.viewTask') }}
          </el-button>
        </template>
        <p v-else class="dlq-detail__hint" data-test="dlq-related-none">{{ message('msg.admin.dlq.relatedTaskNone') }}</p>

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
.dlq-detail__hint {
  margin: var(--bms-space-2) 0 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
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
