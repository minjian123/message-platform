<script setup lang="ts">
/**
 * 重放任务页签：筛选 / 列表 / 新建入口 / 任务详情。
 *
 * 契约：`GET /admin/replay`（06-6-1）；列表项 `affected_groups` 固定为 `null`（影响只在详情返回），
 * 因此本页签不渲染影响列，也不自行计算积压与进度真值。
 */

import {
  ElButton,
  ElDatePicker,
  ElInput,
  ElOption,
  ElSelect,
  ElTable,
  ElTableColumn,
  ElTag,
} from 'element-plus'
import { computed, ref, watch } from 'vue'

import ReadOnlyList from './ReadOnlyList.vue'
import ReplayTaskCreateDialog from './ReplayTaskCreateDialog.vue'
import ReplayTaskDetailDrawer from './ReplayTaskDetailDrawer.vue'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { hostPermissions } from '../runtime'
import { loadReplayTasks, type ReplayTaskItem } from '../services/admin-service'
import { describeReplayScope } from '../services/replay-format'

interface Props {
  /** 需要高亮的任务 ID（死信重放联动 / 新建成功后）。 */
  highlightTask?: string
  /** 刷新令牌：父页自增即触发一次重新取数（死信重放 / 新建成功后）。 */
  refreshToken?: number
  /** 需要直接打开详情的任务 ID（死信详情跳转 / `?task=` 深链）。 */
  focusTaskId?: string
  /** 页签是否处于激活态（切走后关闭详情抽屉，停止进度轮询）。 */
  active?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  highlightTask: '',
  refreshToken: 0,
  focusTaskId: '',
  active: true,
})

const emit = defineEmits<{
  /** 新建成功（父页据此切页签并高亮）。 */
  created: [taskId: string]
}>()

/** 状态 → 标签色。 */
const STATUS_TAG: Record<string, 'success' | 'warning' | 'danger' | 'info'> = {
  pending: 'info',
  running: 'success',
  paused: 'warning',
  completed: 'success',
  failed: 'danger',
  cancelled: 'info',
}

/** 状态筛选项（六态，与契约取值一致）。 */
const STATUS_OPTIONS: readonly string[] = ['pending', 'running', 'paused', 'completed', 'failed', 'cancelled']

const view = useMsgReadOnlyView<ReplayTaskItem>({ loader: loadReplayTasks })

const status = ref('')
const namespace = ref('')
const topic = ref('')
const createdFrom = ref('')
const createdTo = ref('')
const createVisible = ref(false)
const detailTaskId = ref<string | null>(null)

/** 当页行 + 条件摘要（表格列直接取值，避免在模板里调类型化函数）。 */
const rows = computed(() => view.rows.value.map((item) => ({ ...item, scope: scopeOf(item) })))

/** 是否有写权限（`msg:manage` 才渲染「新建重放任务」）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)

/** 应用筛选。 */
function apply(): void {
  view.setFilter('status', status.value)
  view.setFilter('namespace', namespace.value)
  view.setFilter('topic', topic.value)
  view.setFilter('created_from', createdFrom.value)
  view.setFilter('created_to', createdTo.value)
}

/** 重置筛选。 */
function reset(): void {
  status.value = ''
  namespace.value = ''
  topic.value = ''
  createdFrom.value = ''
  createdTo.value = ''
  view.reset()
}

/**
 * 条件摘要（分区 / offset / 时间 / key）。
 *
 * @param item 列表行。
 * @returns 摘要文本。
 */
function scopeOf(item: ReplayTaskItem): string {
  return describeReplayScope({
    partitions: item.partitions,
    offset_from: item.offset_from,
    offset_to: item.offset_to,
    time_from: item.time_from,
    time_to: item.time_to,
    key: item.key,
  })
}

/**
 * 行高亮（命中父页传入的任务 ID）。
 *
 * @param param 行上下文。
 * @returns 行类名。
 */
function rowClass({ row }: { row: ReplayTaskItem }): string {
  return props.highlightTask !== '' && row.task_id === props.highlightTask ? 'replay-row--active' : ''
}

/**
 * 新建成功：刷新列表并通知父页高亮。
 *
 * @param taskId 新任务 ID。
 */
function onCreated(taskId: string): void {
  createVisible.value = false
  emit('created', taskId)
  void view.refresh()
}

// 父页联动：刷新令牌变化 → 重新取数；指定任务 ID 变化 → 打开详情
watch(
  () => props.refreshToken,
  () => {
    void view.refresh()
  },
)
watch(
  () => props.focusTaskId,
  (taskId) => {
    detailTaskId.value = taskId
  },
)
// 切走页签即关闭详情抽屉（抽屉停止轮询，不在后台空转）。
watch(
  () => props.active,
  (active) => {
    if (!active) {
      detailTaskId.value = null
    }
  },
)
</script>

<template>
  <read-only-list
    :title="message('msg.admin.replay.tab')"
    :state="view.state.value"
    :text-key="view.textKey.value"
    :page="view.page.value"
    :page-size="view.pageSize.value"
    :total="view.total.value"
    data-test="replay-list"
    @refresh="view.refresh"
    @update:page="view.setPage"
  >
    <template #filters>
      <div class="replay-filters">
        <div class="replay-filters__row">
          <el-select v-model="status" :placeholder="message('msg.admin.replay.status')" clearable style="width: 140px" data-test="replay-status">
            <el-option v-for="item in STATUS_OPTIONS" :key="item" :label="message(`msg.admin.replay.status.${item}`)" :value="item" />
          </el-select>
          <el-input v-model="namespace" :placeholder="message('msg.admin.replay.namespace')" clearable style="width: 120px" data-test="replay-namespace" />
          <el-input v-model="topic" :placeholder="message('msg.admin.replay.topic')" clearable style="width: 170px" data-test="replay-topic" />
        </div>
        <div class="replay-filters__row">
          <el-date-picker
            v-model="createdFrom"
            type="datetime"
            value-format="YYYY-MM-DDTHH:mm:ss"
            :placeholder="message('msg.admin.replay.createdAt')"
            style="width: 190px"
            data-test="replay-created-from"
          />
          <el-date-picker
            v-model="createdTo"
            type="datetime"
            value-format="YYYY-MM-DDTHH:mm:ss"
            :placeholder="message('msg.admin.replay.createdAt')"
            style="width: 190px"
            data-test="replay-created-to"
          />
          <el-button type="primary" data-test="replay-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="replay-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
          <el-button v-if="canManage" type="success" plain data-test="replay-create" @click="createVisible = true">
            {{ message('msg.admin.replay.create') }}
          </el-button>
        </div>
      </div>
    </template>

    <el-table :data="rows" size="small" border :row-class-name="rowClass" data-test="replay-tasks">
      <el-table-column :label="message('msg.admin.replay.task')" min-width="150">
        <template #default="{ row }">
          <el-button link type="primary" data-test="replay-detail" @click="detailTaskId = row.task_id">
            {{ row.task_id }}
          </el-button>
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.replay.topic')" min-width="180">
        <template #default="{ row }">{{ row.namespace }} / {{ row.topic }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.replay.condition')" min-width="230">
        <template #default="{ row }">{{ row.scope }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.replay.rate')" width="110" align="right">
        <template #default="{ row }">{{ row.rate_per_second }} 条/秒</template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.replay.progress')" width="130" align="right">
        <template #default="{ row }">{{ row.replayed }} / {{ row.total }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.replay.status')" width="110">
        <template #default="{ row }">
          <el-tag :type="STATUS_TAG[row.status]" size="small" data-test="replay-status-tag">
            {{ message(`msg.admin.replay.status.${row.status}`) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="created_at" :label="message('msg.admin.replay.createdAt')" min-width="170" />
    </el-table>
  </read-only-list>

  <replay-task-create-dialog v-model="createVisible" @created="onCreated" />
  <replay-task-detail-drawer :task-id="detailTaskId" @close="detailTaskId = null" @changed="view.refresh" />
</template>

<style scoped>
.replay-filters {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.replay-filters__row {
  display: flex;
  gap: var(--bms-space-2);
  flex-wrap: wrap;
  align-items: center;
}
:deep(.replay-row--active) td {
  background: var(--bms-color-success-light-9);
}
</style>
