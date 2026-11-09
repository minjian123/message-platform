<script setup lang="ts">
/**
 * 新建重放任务（两段式）：填条件 → 二次确认（范围 / 速率 / 目标消费组）→ 提交。
 *
 * 契约：`POST /admin/replay`（06-6-1），`confirm` 由取数层固定并入，只在确认回调后调用；
 * 预估总量与目标组是**确认前的辅助信息**，失败一律降级、不阻断提交（口径见详细设计 §6.3）。
 */

import {
  ElAlert,
  ElButton,
  ElDatePicker,
  ElDialog,
  ElForm,
  ElFormItem,
  ElInput,
  ElInputNumber,
} from 'element-plus'
import { computed, reactive, ref } from 'vue'

import type { ImpactRow } from '../base/BaseMsgWriteAction'
import MsgConfirmDialog from './MsgConfirmDialog.vue'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { message } from '../i18n/messages'
import {
  createReplayTask,
  estimateReplayTotal,
  fetchTopicGroups,
  type ReplayTaskItem,
} from '../services/admin-service'
import {
  describeReplayScope,
  toReplayBody,
  validateReplayForm,
  type ReplayBody,
  type ReplayForm,
} from '../services/replay-format'

interface Props {
  /** 显隐（双向）。 */
  modelValue: boolean
}

const props = defineProps<Props>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  /** 创建成功（携带新任务 ID，供父页高亮）。 */
  created: [taskId: string]
}>()

/** 表单初值（命名空间 / 主题留空，避免误重放他人主题）。 */
function emptyForm(): ReplayForm {
  return {
    namespace: '',
    topic: '',
    offsetFrom: undefined,
    offsetTo: undefined,
    timeFrom: '',
    timeTo: '',
    key: '',
    partitions: '',
    rate: 100,
  }
}

const form = reactive<ReplayForm>(emptyForm())
const errorKey = ref('')
const confirmVisible = ref(false)
const confirmErrorKey = ref('')
const estimate = reactive<{ resolved: boolean; multi: boolean; total: number | null }>({
  resolved: false,
  multi: false,
  total: null,
})
const groups = reactive<{ failed: boolean; names: string[] }>({
  failed: false,
  names: [],
})

/** 弹窗显隐（双向绑定）。 */
const visible = computed<boolean>({
  get: () => props.modelValue,
  set: (value: boolean) => {
    emit('update:modelValue', value)
  },
})

/** 范围摘要（未填条件时也给出「全部分区」）。 */
const scopeText = computed(() =>
  describeReplayScope({
    partitions: toReplayBody(form).partitions,
    offset_from: form.offsetFrom ?? null,
    offset_to: form.offsetTo ?? null,
    time_from: form.timeFrom === '' ? null : form.timeFrom,
    time_to: form.timeTo === '' ? null : form.timeTo,
    key: form.key.trim() === '' ? null : form.key.trim(),
  }),
)

/** 预计总量文案（未估算 / 多分区 / 取不到 / 具体条数）。 */
const estimateText = computed(() => {
  if (!estimate.resolved) {
    return message('msg.admin.common.placeholder')
  }
  if (estimate.multi) {
    return message('msg.admin.replay.estimateMulti')
  }
  if (estimate.total === null) {
    return message('msg.admin.replay.estimateUnavailable')
  }
  return `${String(estimate.total)} ${message('msg.admin.replay.estimateSuffix')}`
})

/** 确认弹窗的影响范围行。 */
const confirmRows = computed<ImpactRow[]>(() => {
  const body = toReplayBody(form)
  return [
    { label: message('msg.admin.replay.topic'), value: `${body.namespace} / ${body.topic}` },
    { label: message('msg.admin.replay.scope'), value: scopeText.value },
    { label: message('msg.admin.replay.rate'), value: `${String(body.rate_per_second)} 条/秒` },
    { label: message('msg.admin.replay.estimate'), value: estimateText.value },
    { label: message('msg.admin.replay.targetGroups'), value: `${String(groups.names.length)}` },
  ]
})

/** 写操作（确认回调后才提交）。 */
const write = useMsgWriteAction<ReplayTaskItem>({
  runner: () => createReplayTask(toReplayBody(form)),
  requiresConfirm: true,
})

/** 关闭并复位（保留用户填的条件，下次打开可改）。 */
function close(): void {
  confirmVisible.value = false
  write.reset()
  visible.value = false
}

/** 取预估总量与目标组（多分区不预估，失败降级）。 */
async function prepare(body: ReplayBody): Promise<void> {
  const multi = body.partitions !== null && body.partitions.length > 1
  estimate.multi = multi
  estimate.total = null
  estimate.resolved = false
  groups.names = []
  groups.failed = false

  if (!multi) {
    const partition = body.partitions === null ? undefined : body.partitions[0]
    const params = {
      namespace: body.namespace,
      topic: body.topic,
      offset_from: body.offset_from ?? undefined,
      offset_to: body.offset_to ?? undefined,
      key: body.key ?? undefined,
      time_from: body.time_from ?? undefined,
      time_to: body.time_to ?? undefined,
    }
    estimate.total = await estimateReplayTotal(partition === undefined ? params : { ...params, partition })
  }
  estimate.resolved = true

  const names = await fetchTopicGroups(body.namespace, body.topic)
  groups.names = names === null ? [] : [...names]
  groups.failed = names === null
}

/** 第一步：校验并进入二次确认。 */
async function next(): Promise<void> {
  const key = validateReplayForm(form)
  if (key !== '') {
    errorKey.value = key
    return
  }
  errorKey.value = ''
  confirmErrorKey.value = ''
  write.reset()
  await prepare(toReplayBody(form))
  confirmVisible.value = true
}

/** 第二步：确认后提交。 */
async function submit(): Promise<void> {
  write.acknowledge()
  await write.run()
  const created = write.result.value
  if (created === undefined) {
    confirmErrorKey.value = write.errorKey.value
    return
  }
  confirmVisible.value = false
  visible.value = false
  const taskId = created.task_id
  form.namespace = ''
  form.topic = ''
  form.offsetFrom = undefined
  form.offsetTo = undefined
  form.timeFrom = ''
  form.timeTo = ''
  form.key = ''
  form.partitions = ''
  form.rate = 100
  errorKey.value = ''
  emit('created', taskId)
}
</script>

<template>
  <el-dialog
    v-model="visible"
    :title="message('msg.admin.replay.create')"
    width="600px"
    :close-on-click-modal="false"
    data-test="replay-create-dialog"
  >
    <el-form label-width="120px" size="small">
      <el-form-item :label="message('msg.admin.replay.namespace')" required>
        <el-input v-model="form.namespace" placeholder="msg" style="width: 220px" data-test="replay-form-namespace" />
      </el-form-item>
      <el-form-item :label="message('msg.admin.replay.topic')" required>
        <el-input v-model="form.topic" placeholder="order.created" style="width: 280px" data-test="replay-form-topic" />
      </el-form-item>
      <el-form-item :label="message('msg.admin.replay.offsetFrom')">
        <div class="replay-range">
          <el-input-number
            v-model="form.offsetFrom"
            :min="0"
            :controls="false"
            :placeholder="message('msg.admin.replay.offsetFrom')"
            data-test="replay-form-offset-from"
          />
          <span class="replay-range__sep">~</span>
          <el-input-number
            v-model="form.offsetTo"
            :min="0"
            :controls="false"
            :placeholder="message('msg.admin.replay.offsetTo')"
            data-test="replay-form-offset-to"
          />
        </div>
      </el-form-item>
      <el-form-item :label="message('msg.admin.replay.timeFrom')">
        <div class="replay-range">
          <el-date-picker
            v-model="form.timeFrom"
            type="datetime"
            value-format="YYYY-MM-DDTHH:mm:ss"
            :placeholder="message('msg.admin.replay.timeFrom')"
            data-test="replay-form-time-from"
          />
          <span class="replay-range__sep">~</span>
          <el-date-picker
            v-model="form.timeTo"
            type="datetime"
            value-format="YYYY-MM-DDTHH:mm:ss"
            :placeholder="message('msg.admin.replay.timeTo')"
            data-test="replay-form-time-to"
          />
        </div>
      </el-form-item>
      <el-form-item :label="message('msg.admin.replay.key')">
        <el-input
          v-model="form.key"
          :placeholder="message('msg.admin.replay.keyHint')"
          maxlength="256"
          clearable
          style="width: 280px"
          data-test="replay-form-key"
        />
      </el-form-item>
      <el-form-item :label="message('msg.admin.replay.partitions')">
        <el-input
          v-model="form.partitions"
          :placeholder="message('msg.admin.replay.partitionsHint')"
          clearable
          style="width: 320px"
          data-test="replay-form-partitions"
        />
      </el-form-item>
      <el-form-item :label="message('msg.admin.replay.rate')">
        <el-input-number v-model="form.rate" :min="1" :max="1000" :step="100" data-test="replay-form-rate" />
      </el-form-item>
    </el-form>

    <el-alert type="info" :closable="false" show-icon :title="message('msg.admin.replay.scope')">
      <p class="replay-scope" data-test="replay-scope-text">
        {{ message('msg.admin.replay.scopeCondition') }}：{{ scopeText }}
      </p>
      <p class="replay-scope" data-test="replay-estimate-text">
        {{ message('msg.admin.replay.estimate') }}：{{ estimateText }}
      </p>
    </el-alert>

    <p v-if="errorKey !== ''" class="replay-error" data-test="replay-create-error">{{ message(errorKey) }}</p>

    <template #footer>
      <el-button data-test="replay-create-cancel" @click="close">{{ message('msg.admin.common.cancel') }}</el-button>
      <el-button type="primary" data-test="replay-create-next" @click="next">
        {{ message('msg.admin.replay.nextStep') }}
      </el-button>
    </template>
  </el-dialog>

  <msg-confirm-dialog
    v-model="confirmVisible"
    :title="message('msg.admin.replay.confirmTitle')"
    :rows="confirmRows"
    :warning="message('msg.admin.replay.confirmWarning')"
    :loading="write.status.value === 'running'"
    :confirm-text="message('msg.admin.common.confirm')"
    @confirm="submit"
  >
    <div class="replay-groups">
      <span class="replay-groups__title">
        {{ message('msg.admin.replay.targetGroups') }}（{{ message('msg.admin.replay.targetGroupsNote') }}）
      </span>
      <div v-if="groups.names.length > 0" data-test="replay-target-groups">
        <span v-for="group in groups.names" :key="group" class="replay-groups__chip">{{ group }}</span>
        <p class="replay-groups__note">
          {{ message('msg.admin.replay.targetGroupsCount') }}
        </p>
      </div>
      <p v-else-if="groups.failed" class="replay-groups__note" data-test="replay-target-groups-failed">
        {{ message('msg.admin.replay.targetGroupsUnavailable') }}
      </p>
      <p v-else class="replay-groups__note" data-test="replay-target-groups-empty">
        {{ message('msg.admin.replay.targetGroupsEmpty') }}
      </p>
    </div>
    <p v-if="confirmErrorKey !== ''" class="replay-error" data-test="replay-confirm-error">
      {{ message(confirmErrorKey) }}
    </p>
  </msg-confirm-dialog>
</template>

<style scoped>
.replay-range {
  display: flex;
  align-items: center;
  gap: var(--bms-space-2);
}
.replay-range__sep {
  color: var(--bms-color-text-secondary);
}
.replay-scope {
  margin: 0;
  line-height: 1.7;
}
.replay-error {
  margin: var(--bms-space-2) 0 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
.replay-groups {
  margin-top: var(--bms-space-2);
}
.replay-groups__title {
  font-size: 12px;
  color: var(--bms-color-text-secondary);
}
.replay-groups__chip {
  display: inline-block;
  margin: 6px 6px 0 0;
  padding: 1px 8px;
  border-radius: 10px;
  background: var(--bms-color-primary-light-9);
  color: var(--bms-color-primary);
  font-size: 12px;
  line-height: 18px;
}
.replay-groups__note {
  margin: 6px 0 0;
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
</style>
