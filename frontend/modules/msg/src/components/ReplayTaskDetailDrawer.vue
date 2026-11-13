<script setup lang="ts">
/**
 * 重放任务详情抽屉：进度 / 条件 / 首末 log_id / 影响消费组 / 暂停·恢复·取消。
 *
 * 契约：`GET /admin/replay/{id}` 与三个状态端点（06-6-1）。进度与积压**全部取后端值**，前端只做展示；
 * 非终态每 `REPLAY_POLL_INTERVAL_MS` 轮询一次，转终态 / 请求失败 / 关闭抽屉即停（口径见详细设计 §8）。
 */

import {
  ElAlert,
  ElButton,
  ElDescriptions,
  ElDescriptionsItem,
  ElDrawer,
  ElProgress,
  ElTable,
  ElTableColumn,
  ElTag,
} from 'element-plus'
import { computed, onScopeDispose, ref, watch } from 'vue'

import type { ImpactRow } from '../base/BaseMsgWriteAction'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { message } from '../i18n/messages'
import { REPLAY_POLL_INTERVAL_MS } from '../constants'
import { hostPermissions } from '../runtime'
import {
  cancelReplayTask,
  fetchReplayTask,
  pauseReplayTask,
  resumeReplayTask,
  type ReplayTaskItem,
} from '../services/admin-service'
import { describeReplayScope } from '../services/replay-format'
import { toLocalText } from '../services/time-format'
import MsgConfirmDialog from './MsgConfirmDialog.vue'

/** 状态操作。 */
export type ReplayAction = 'pause' | 'resume' | 'cancel'

interface Props {
  /** 任务 ID；`null` 表示抽屉关闭。 */
  taskId: string | null
}

const props = defineProps<Props>()

const emit = defineEmits<{
  /** 关闭抽屉（父页清空 taskId）。 */
  close: []
  /** 任务状态变化（父页刷新列表）。 */
  changed: []
}>()

/** 终态集合（只读）。 */
const TERMINAL: readonly string[] = ['completed', 'failed', 'cancelled']

/** 状态 → 标签色。 */
const STATUS_TAG: Record<string, 'success' | 'warning' | 'danger' | 'info'> = {
  pending: 'info',
  running: 'success',
  paused: 'warning',
  completed: 'success',
  failed: 'danger',
  cancelled: 'info',
}

/** 当前任务快照。 */
const row = ref<ReplayTaskItem | null>(null)
/** 首次加载中。 */
const loading = ref(false)
/** 最近一次取数失败（保留已有数据，仅提示）。 */
const failed = ref(false)
/** 操作确认弹窗。 */
const action = ref<{ visible: boolean; kind: ReplayAction; taskId: string; errorKey: string }>({
  visible: false,
  kind: 'pause',
  taskId: '',
  errorKey: '',
})

/** 轮询定时器（`setTimeout` 自递归，避免慢响应堆叠）。 */
let timer: ReturnType<typeof setTimeout> | undefined

/** 是否有写权限（无 `msg:manage` 时操作按钮不渲染）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)

/** 抽屉显隐（关闭时回到父页）。 */
const visible = computed<boolean>({
  get: () => props.taskId !== null,
  set: (value: boolean) => {
    if (!value) {
      emit('close')
    }
  },
})

/** 进度百分比（`total = 0` 记 0，避免 NaN）。 */
const percentage = computed(() => {
  const current = row.value
  if (current === null || current.total <= 0) {
    return 0
  }
  return Math.min(100, Math.round((current.replayed / current.total) * 100))
})

/** 进度条状态（完成绿、失败红、其余默认色）。 */
const progressStatus = computed<'success' | 'exception' | ''>(() => {
  if (row.value?.status === 'completed') {
    return 'success'
  }
  if (row.value?.status === 'failed') {
    return 'exception'
  }
  return ''
})

/**
 * 条件摘要（与列表同一格式化）。
 *
 * @param item 任务快照。
 * @returns 摘要文本。
 */
function scopeTextOf(item: ReplayTaskItem): string {
  return describeReplayScope({
    partitions: item.partitions,
    offset_from: item.offset_from,
    offset_to: item.offset_to,
    time_from: item.time_from,
    time_to: item.time_to,
    key: item.key,
  })
}

/** 影响消费组（契约里列表端点为 `null`，详情端点为数组）。 */
const affected = computed(() => row.value?.affected_groups ?? [])
/** 基线是否被截断。 */
const truncated = computed(() => row.value?.affected_truncated === true)

/** 操作确认弹窗的影响范围行。 */
const actionRows = computed<ImpactRow[]>(() => {
  const current = row.value
  if (current === null) {
    return []
  }
  return [
    { label: message('msg.admin.replay.task'), value: current.task_id },
    { label: message('msg.admin.replay.topic'), value: `${current.namespace} / ${current.topic}` },
    { label: message('msg.admin.replay.currentProgress'), value: `${String(current.replayed)} / ${String(current.total)}` },
  ]
})

/** 操作确认弹窗标题。 */
const actionTitle = computed(() => {
  const keys: Record<ReplayAction, string> = {
    pause: 'msg.admin.replay.pauseTitle',
    resume: 'msg.admin.replay.resumeTitle',
    cancel: 'msg.admin.replay.cancelTitle',
  }
  return message(keys[action.value.kind])
})

/** 操作确认弹窗风险提示（仅取消给危险提示）。 */
const actionWarning = computed(() => {
  if (action.value.kind !== 'cancel') {
    return ''
  }
  return message('msg.admin.replay.cancelWarning')
})

/** 操作确认弹窗补充说明（取消已有风险提示，这里只补暂停 / 恢复的语义）。 */
const actionHint = computed(() => {
  if (action.value.kind === 'cancel') {
    return ''
  }
  return action.value.kind === 'pause' ? message('msg.admin.replay.pauseHint') : message('msg.admin.replay.resumeHint')
})


/** 写操作（三个动作共用，确认回调后按 `kind` 分派）。 */
const write = useMsgWriteAction<ReplayTaskItem>({
  runner: () => runAction(),
  requiresConfirm: true,
})

/**
 * 是否终态。
 *
 * @param status 任务状态。
 * @returns 是否终态。
 */
function isTerminal(status: string): boolean {
  return TERMINAL.includes(status)
}

/** 停掉轮询（幂等）。 */
function stopPolling(): void {
  if (timer !== undefined) {
    clearTimeout(timer)
    timer = undefined
  }
}

/** 当前任务 ID（watch 写入；重试按钮用它，避免在模板里判空）。 */
const currentTaskId = ref('')

/**
 * 安排下一次取数。
 *
 * @param taskId 任务 ID。
 */
function schedule(taskId: string): void {
  stopPolling()
  timer = setTimeout(() => {
    void load(taskId)
  }, REPLAY_POLL_INTERVAL_MS)
}

/**
 * 取一次任务快照（失败即停轮询并提示，保留已有数据）。
 *
 * @param taskId 任务 ID。
 */
async function load(taskId: string): Promise<void> {
  loading.value = row.value === null
  try {
    const data = await fetchReplayTask(taskId)
    if (data === null) {
      failed.value = true
      stopPolling()
      return
    }
    row.value = data
    failed.value = false
    if (isTerminal(data.status)) {
      stopPolling()
    } else {
      schedule(taskId)
    }
  } catch {
    failed.value = true
    stopPolling()
  } finally {
    loading.value = false
  }
}

/** 手动重试（失败提示上的按钮）。 */
function retry(): void {
  void load(currentTaskId.value)
}

/**
 * 打开操作确认弹窗。
 *
 * @param kind 动作。
 * @param taskId 任务 ID。
 */
function openAction(kind: ReplayAction, taskId: string): void {
  action.value.kind = kind
  action.value.taskId = taskId
  action.value.errorKey = ''
  action.value.visible = true
  write.reset()
}

/** 按 `kind` 调对应端点。 */
async function runAction(): Promise<ReplayTaskItem> {
  const taskId = action.value.taskId
  if (action.value.kind === 'pause') {
    return pauseReplayTask(taskId)
  }
  if (action.value.kind === 'resume') {
    return resumeReplayTask(taskId)
  }
  return cancelReplayTask(taskId)
}

/** 确认执行状态操作。 */
async function confirmAction(): Promise<void> {
  write.acknowledge()
  await write.run()
  if (write.status.value !== 'done') {
    action.value.errorKey = write.errorKey.value
    return
  }
  action.value.visible = false
  await load(currentTaskId.value)
  emit('changed')
}

watch(
  () => props.taskId,
  (taskId) => {
    stopPolling()
    row.value = null
    failed.value = false
    currentTaskId.value = taskId ?? ''
    if (taskId !== null) {
      void load(taskId)
    }
  },
  { immediate: true },
)

onScopeDispose(stopPolling)
</script>

<template>
  <el-drawer v-model="visible" :title="message('msg.admin.replay.detailTitle')" size="620px" data-test="replay-detail-drawer">
    <div v-if="loading" data-test="replay-detail-loading">{{ message('state.loading') }}</div>
    <el-alert
      v-if="failed"
      type="error"
      :closable="false"
      show-icon
      data-test="replay-detail-error"
    >
      <template #title>
        {{ message('msg.admin.replay.detailFailed') }}
        <el-button link type="primary" data-test="replay-detail-retry" @click="retry">
          {{ message('msg.admin.common.refresh') }}
        </el-button>
      </template>
    </el-alert>

    <template v-if="row">
      <el-progress
        :percentage="percentage"
        :status="progressStatus"
        :stroke-width="14"
        data-test="replay-detail-progress"
      />
      <p class="replay-detail__progress" data-test="replay-detail-progress-text">
        {{ message('msg.admin.replay.replayedOf') }} {{ row.replayed }} / {{ row.total }}
      </p>

      <el-descriptions :column="1" border size="small">
        <el-descriptions-item :label="message('msg.admin.replay.task')">{{ row.task_id }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.topic')">
          {{ row.namespace }} / {{ row.topic }}
        </el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.condition')">{{ scopeTextOf(row) }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.rate')">{{ row.rate_per_second }} 条/秒</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.status')">
          <el-tag :type="STATUS_TAG[row.status]" size="small" data-test="replay-detail-status">
            {{ message(`msg.admin.replay.status.${row.status}`) }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.logIdFirst')">
          {{ row.log_id_first ?? message('msg.admin.common.placeholder') }}
        </el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.logIdLast')">
          {{ row.log_id_last ?? message('msg.admin.common.placeholder') }}
        </el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.createdAt')">{{ toLocalText(row.created_at) }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.replay.finishedAt')">
          {{ toLocalText(row.finished_at ?? null) }}
        </el-descriptions-item>
      </el-descriptions>

      <el-alert
        v-if="row.error_msg"
        type="error"
        :closable="false"
        show-icon
        :title="`${message('msg.admin.replay.errorMsg')}：${row.error_msg}`"
        class="replay-detail__error-msg"
        data-test="replay-detail-error-msg"
      />

      <h4 class="replay-detail__section">{{ message('msg.admin.replay.affected') }}</h4>
      <el-alert
        v-if="truncated"
        type="warning"
        :closable="false"
        show-icon
        :title="message('msg.admin.replay.truncated')"
        class="replay-detail__truncated"
        data-test="replay-detail-truncated"
      />
      <el-table :data="[...affected]" border size="small" :empty-text="message('msg.admin.replay.affectedEmpty')" data-test="replay-affected-table">
        <el-table-column prop="group" :label="message('msg.admin.replay.group')" min-width="130" />
        <el-table-column prop="baseline_lag" :label="message('msg.admin.replay.baseline')" width="100" align="right" />
        <el-table-column prop="current_lag" :label="message('msg.admin.replay.current')" width="100" align="right" />
        <el-table-column :label="message('msg.admin.replay.delta')" width="90" align="right">
          <template #default="{ row: item }">
            <span :class="item.delta > 0 ? 'replay-detail__delta' : ''">{{ item.delta > 0 ? `+${String(item.delta)}` : item.delta }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.replay.capturedAt')" width="160">
          <template #default="{ row: item }">{{ toLocalText((item as { captured_at: string | null }).captured_at) }}</template>
        </el-table-column>
      </el-table>
      <p class="replay-detail__note">{{ message('msg.admin.replay.affectedNote') }}</p>

      <p v-if="isTerminal(row.status)" class="replay-detail__note" data-test="replay-detail-terminal">
        {{ message('msg.admin.replay.terminal') }}
      </p>
    </template>

    <template #footer>
      <el-button data-test="replay-detail-close" @click="visible = false">
        {{ message('msg.admin.replay.off') }}
      </el-button>
      <el-button
        v-if="canManage && row && (row.status === 'pending' || row.status === 'running')"
        data-test="replay-pause"
        @click="openAction('pause', row.task_id)"
      >
        {{ message('msg.admin.replay.pause') }}
      </el-button>
      <el-button
        v-if="canManage && row && row.status === 'paused'"
        type="primary"
        data-test="replay-resume"
        @click="openAction('resume', row.task_id)"
      >
        {{ message('msg.admin.replay.resume') }}
      </el-button>
      <el-button
        v-if="canManage && row && !isTerminal(row.status)"
        type="danger"
        data-test="replay-cancel"
        @click="openAction('cancel', row.task_id)"
      >
        {{ message('msg.admin.replay.cancel') }}
      </el-button>
    </template>
  </el-drawer>

  <msg-confirm-dialog
    v-model="action.visible"
    :title="actionTitle"
    :rows="actionRows"
    :warning="actionWarning"
    :danger="action.kind === 'cancel'"
    :loading="write.status.value === 'running'"
    @confirm="confirmAction"
  >
    <p v-if="actionHint !== ''" class="replay-detail__note">{{ actionHint }}</p>
    <p v-if="action.errorKey !== ''" class="replay-detail__error" data-test="replay-action-error">
      {{ message(action.errorKey) }}
    </p>
  </msg-confirm-dialog>
</template>

<style scoped>
.replay-detail__progress {
  margin: var(--bms-space-1) 0 var(--bms-space-3);
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.replay-detail__section {
  margin: var(--bms-space-3) 0 var(--bms-space-2);
  font-size: 14px;
}
.replay-detail__note {
  margin: var(--bms-space-1) 0 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
  line-height: 1.7;
}
.replay-detail__error-msg,
.replay-detail__truncated {
  margin-top: var(--bms-space-2);
}
.replay-detail__delta {
  color: var(--bms-color-warning);
  font-weight: 600;
}
.replay-detail__error {
  margin: var(--bms-space-2) 0 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
</style>
