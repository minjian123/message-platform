<script setup lang="ts">
/**
 * 二次确认弹窗（共享件）：展示影响范围 + 风险提示，提交中禁用双按钮。
 *
 * **不发请求**：只渲染与收集意图（`confirm` / `cancel`），请求由页面经 `useMsgWriteAction()` 发起；
 * 因此 `loading` 由页面传入，弹窗不自行猜提交状态。06-6-2 / 06-5-2 直接复用本组件。
 */
import { ElAlert, ElButton, ElDialog, ElTable, ElTableColumn } from 'element-plus'
import { computed } from 'vue'

import type { ImpactRow } from '../base/BaseMsgWriteAction'
import { message } from '../i18n/messages'

interface Props {
  /** 显隐（双向）。 */
  modelValue: boolean
  /** 标题（已解析文案）。 */
  title: string
  /** 影响范围行（空表示只展示风险提示）。 */
  rows?: readonly ImpactRow[]
  /** 风险提示（已解析文案）。 */
  warning?: string
  /** 确认按钮是否危险色。 */
  danger?: boolean
  /** 提交中：确认按钮 loading、双按钮禁用。 */
  loading?: boolean
  /** 确认按钮文案（缺省取通用「确认」）。 */
  confirmText?: string
  /** 取消按钮文案（缺省取通用「取消」）。 */
  cancelText?: string
}

const props = withDefaults(defineProps<Props>(), {
  rows: () => [],
  warning: '',
  danger: false,
  loading: false,
  confirmText: '',
  cancelText: '',
})

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  confirm: []
  cancel: []
}>()

/** 确认按钮文案。 */
const confirmLabel = computed(() => (props.confirmText === '' ? message('msg.admin.common.confirm') : props.confirmText))
/** 取消按钮文案。 */
const cancelLabel = computed(() => (props.cancelText === '' ? message('msg.admin.common.cancel') : props.cancelText))

/** 关闭弹窗（取消语义）。 */
function close(): void {
  emit('update:modelValue', false)
  emit('cancel')
}

/** 确认（由页面决定真实提交）。 */
function confirm(): void {
  emit('confirm')
}
</script>

<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    width="520px"
    :close-on-click-modal="false"
    :close-on-press-escape="!loading"
    data-test="confirm-dialog"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-alert
      v-if="warning !== ''"
      type="error"
      :closable="false"
      show-icon
      :title="warning"
      data-test="confirm-warning"
    />
    <el-table v-if="rows.length > 0" :data="[...rows]" size="small" border data-test="confirm-impact">
      <el-table-column :label="message('msg.admin.confirm.impactItem')" min-width="150">
        <template #default="{ row }">{{ row.label }}</template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.confirm.impactValue')" min-width="150" align="right">
        <template #default="{ row }">
          <span :class="row.tone === 'danger' ? 'confirm-danger' : ''">{{ row.value }}</span>
        </template>
      </el-table-column>
    </el-table>
    <!-- 调用方追加的输入区（如「目标位点」）；弹窗本身不承载表单状态。 -->
    <div class="confirm-extra" data-test="confirm-extra"><slot /></div>
    <template #footer>
      <el-button :disabled="loading" data-test="confirm-cancel" @click="close">{{ cancelLabel }}</el-button>
      <el-button
        :type="danger ? 'danger' : 'primary'"
        :loading="loading"
        :disabled="loading"
        data-test="confirm-ok"
        @click="confirm"
      >
        {{ confirmLabel }}
      </el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.confirm-danger {
  color: var(--bms-color-danger);
  font-weight: 600;
}
.confirm-extra {
  margin-top: var(--bms-space-2);
}
</style>
