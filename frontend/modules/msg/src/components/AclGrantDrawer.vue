<script setup lang="ts">
/**
 * 主题授权授予 / 更新抽屉（规则行编辑器）。
 *
 * - `create`：一行起、可增删，单行即单条、多行即批量；
 * - `update` / `regrant`：固定一行，五字段锁定、仅效果可改；
 * - 整批校验后一次 `PUT /admin/acls`，按 `changed` 区分「已保存 / 无变化」。
 */

import { ElAlert, ElButton, ElDialog, ElOption, ElSelect, ElTable, ElTableColumn } from 'element-plus'
import { computed, ref, watch } from 'vue'

import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { message } from '../i18n/messages'
import {
  ACL_ACTION_OPTIONS,
  ACL_EFFECT_OPTIONS,
  ACL_PRINCIPAL_TYPE_OPTIONS,
  grantAcls,
  type AclBatchResult,
  type AclRuleInput,
} from '../services/acl-service'
import AclNamespaceSelect from './AclNamespaceSelect.vue'
import AclPrincipalSelect from './AclPrincipalSelect.vue'
import AclTopicSelect from './AclTopicSelect.vue'

/** 授予模式。 */
export type AclGrantMode = 'create' | 'update' | 'regrant'

/** 编辑 / 重新授予的预填行。 */
export interface AclGrantPrefill {
  /** 命名空间。 */
  namespace: string
  /** 主题名。 */
  topic: string
  /** 主体类型。 */
  principalType: string
  /** 主体标识。 */
  principalKey: string
  /** 主体名称（已知时）。 */
  principalLabel: string
  /** 动作。 */
  action: string
  /** 效果。 */
  effect: string
}

interface Props {
  /** 显隐（双向）。 */
  modelValue: boolean
  /** 模式。 */
  mode: AclGrantMode
  /** 编辑 / 重新授予的预填行（create 为空）。 */
  prefill?: AclGrantPrefill | null
  /** 已持有权限码（缺省取宿主注入快照）。 */
  permissionCodes?: readonly string[] | undefined
}

const props = withDefaults(defineProps<Props>(), {
  prefill: null,
  permissionCodes: undefined,
})

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  saved: [payload: { changed: number }]
}>()

/** 单批规则上限（防超大表单；后端无硬上限）。 */
const MAX_ROWS = 50
/** 行自增键。 */
let nextUid = 1

/** 规则行。 */
interface GrantRow {
  uid: number
  namespace: string
  topic: string
  principalType: string
  principalKey: string
  principalLabel: string
  action: string
  effect: string
  error: string
}

/** 空行。 */
function emptyRow(): GrantRow {
  return {
    uid: nextUid++,
    namespace: 'msg',
    topic: '',
    principalType: 'user',
    principalKey: '',
    principalLabel: '',
    action: 'publish',
    effect: 'allow',
    error: '',
  }
}

/**
 * 预填行 → 规则行。
 *
 * @param prefill 预填内容。
 * @returns 规则行。
 */
function rowFromPrefill(prefill: AclGrantPrefill): GrantRow {
  return {
    uid: nextUid++,
    namespace: prefill.namespace,
    topic: prefill.topic,
    principalType: prefill.principalType,
    principalKey: prefill.principalKey,
    principalLabel: prefill.principalLabel,
    action: prefill.action,
    effect: prefill.effect,
    error: '',
  }
}

const rows = ref<GrantRow[]>([emptyRow()])
/** 锁定五字段（更新 / 重新授予）。 */
const locked = computed(() => props.mode !== 'create')
/** 标题。 */
const title = computed(() => {
  const key =
    props.mode === 'update'
      ? 'msg.admin.acls.grant.updateTitle'
      : props.mode === 'regrant'
        ? 'msg.admin.acls.grant.regrantTitle'
        : 'msg.admin.acls.grant.title'
  return message(key)
})

/** 授予动作。 */
const grant = useMsgWriteAction<AclBatchResult>({
  runner: () => submitPayload(),
  requiresConfirm: false,
})

/** 错误文案（350027 用页面级键）。 */
const errorText = computed(() => {
  const code = grant.errorCode.value
  if (code === undefined) {
    return ''
  }
  if (code === 350027) {
    return message('msg.admin.acls.error.350027')
  }
  return message(grant.errorKey.value)
})

/** 打开时重置行与状态。 */
watch(
  () => props.modelValue,
  (visible) => {
    if (!visible) {
      return
    }
    grant.reset()
    rows.value = props.prefill === null ? [emptyRow()] : [rowFromPrefill(props.prefill)]
  },
  { immediate: true },
)

/** 添加一行。 */
function addRow(): void {
  if (rows.value.length >= MAX_ROWS) {
    rows.value[0] = { ...rows.value[0], error: message('msg.admin.acls.grant.tooMany') }
    return
  }
  rows.value.push(emptyRow())
}

/**
 * 删除一行（至少保留一条）。
 *
 * @param index 行下标。
 */
function removeRow(index: number): void {
  if (rows.value.length <= 1) {
    return
  }
  rows.value.splice(index, 1)
}

/**
 * 主题选中：自动带出命名空间。
 *
 * @param row 规则行。
 * @param payload 主题名与命名空间。
 */
function onTopicSelect(row: unknown, payload: { name: string; namespace: string }): void {
  const target = row as GrantRow
  target.topic = payload.name
  target.namespace = payload.namespace
}

/**
 * 主体类型切换：清空主体标识与名称。
 *
 * @param row 规则行。
 */
function onPrincipalTypeChange(row: unknown): void {
  const target = row as GrantRow
  target.principalKey = ''
  target.principalLabel = ''
}

/**
 * 校验单行。
 *
 * @param row 规则行。
 * @returns 是否通过。
 */
function validateRow(row: GrantRow): boolean {
  row.error = ''
  if (row.namespace === '' || row.topic === '' || row.principalKey === '' || row.action === '' || row.effect === '') {
    row.error = message('msg.admin.acls.grant.required')
    return false
  }
  if (!/^\d+$/.test(row.principalKey) || row.principalKey.length > 64) {
    row.error = message('msg.admin.acls.grant.principalInvalid')
    return false
  }
  return true
}

/**
 * 校验整批（逐行 + 去重）。
 *
 * @returns 是否通过。
 */
function validate(): boolean {
  let valid = true
  const keys = new Set<string>()
  for (const row of rows.value) {
    if (!validateRow(row)) {
      valid = false
      continue
    }
    const key = [row.namespace, row.topic, row.principalType, row.principalKey, row.action].join('\u0000')
    if (keys.has(key)) {
      row.error = message('msg.admin.acls.grant.duplicate')
      valid = false
    }
    keys.add(key)
  }
  return valid
}

/**
 * 组装提交载荷。
 *
 * @returns 规则清单。
 */
function buildItems(): AclRuleInput[] {
  return rows.value.map((row) => ({
    namespace: row.namespace,
    topic: row.topic,
    principal_type: row.principalType,
    principal_key: row.principalKey,
    action: row.action,
    effect: row.effect,
  }))
}

/**
 * 提交（由保存按钮触发）。
 *
 * @returns 批量结果。
 */
async function submitPayload(): Promise<AclBatchResult> {
  return grantAcls(buildItems())
}

/** 保存。 */
async function save(): Promise<void> {
  if (!validate()) {
    return
  }
  await grant.run()
  if (grant.status.value !== 'done') {
    return
  }
  const changed = (grant.result.value as AclBatchResult).changed?.length ?? 0
  emit('saved', { changed })
  emit('update:modelValue', false)
}

/**
 * 关闭（取消语义）。
 *
 * @param value 目标显隐。
 */
function setVisible(value: boolean): void {
  emit('update:modelValue', value)
}

/** 动作文案。 */
function actionText(value: string): string {
  return message(ACL_ACTION_OPTIONS.find((item) => item.value === value)?.labelKey ?? 'msg.admin.acls.action')
}

/** 主体类型文案。 */
function principalTypeText(value: string): string {
  return message(
    ACL_PRINCIPAL_TYPE_OPTIONS.find((item) => item.value === value)?.labelKey ?? 'msg.admin.acls.principalType',
  )
}
</script>

<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    width="960px"
    :close-on-click-modal="false"
    data-test="acl-grant-drawer"
    @update:model-value="setVisible"
  >
    <el-alert
      v-if="locked"
      type="info"
      :closable="false"
      show-icon
      :title="message('msg.admin.acls.grant.keyLocked')"
      data-test="acl-grant-locked"
    />
    <el-alert
      v-if="mode === 'regrant'"
      type="warning"
      :closable="false"
      show-icon
      :title="message('msg.admin.acls.grant.regrantHint')"
      style="margin-top: 8px"
      data-test="acl-grant-regrant"
    />

    <el-table :data="rows" size="small" border style="margin-top: 12px" data-test="acl-grant-table">
      <el-table-column :label="message('msg.admin.acls.namespace')" width="150">
        <template #default="{ row }">
          <span v-if="locked">{{ row.namespace }}</span>
          <acl-namespace-select v-else v-model="row.namespace" />
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.acls.topic')" min-width="180">
        <template #default="{ row }">
          <span v-if="locked">{{ row.topic }}</span>
          <acl-topic-select
            v-else
            v-model="row.topic"
            :namespace="row.namespace"
            @select="onTopicSelect(row, $event)"
          />
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.acls.principalType')" width="110">
        <template #default="{ row }">
          <span v-if="locked">{{ principalTypeText(row.principalType) }}</span>
          <el-select
            v-else
            v-model="row.principalType"
            size="small"
            data-test="acl-grant-principal-type"
            @change="onPrincipalTypeChange(row)"
          >
            <el-option
              v-for="item in ACL_PRINCIPAL_TYPE_OPTIONS"
              :key="item.value"
              :label="message(item.labelKey)"
              :value="item.value"
            />
          </el-select>
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.acls.principalKey')" min-width="200">
        <template #default="{ row }">
          <span v-if="locked">{{ row.principalLabel || row.principalKey }}</span>
          <acl-principal-select
            v-else
            v-model="row.principalKey"
            v-model:label="row.principalLabel"
            :principal-type="row.principalType"
            :permission-codes="permissionCodes"
          />
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.acls.action')" width="110">
        <template #default="{ row }">
          <span v-if="locked">{{ actionText(row.action) }}</span>
          <el-select v-else v-model="row.action" size="small" data-test="acl-grant-action">
            <el-option
              v-for="item in ACL_ACTION_OPTIONS"
              :key="item.value"
              :label="message(item.labelKey)"
              :value="item.value"
            />
          </el-select>
        </template>
      </el-table-column>
      <el-table-column :label="message('msg.admin.acls.effect')" width="100">
        <template #default="{ row }">
          <el-select v-model="row.effect" size="small" data-test="acl-grant-effect">
            <el-option
              v-for="item in ACL_EFFECT_OPTIONS"
              :key="item.value"
              :label="message(item.labelKey)"
              :value="item.value"
            />
          </el-select>
        </template>
      </el-table-column>
      <el-table-column label="校验" min-width="150">
        <template #default="{ row }">
          <span v-if="row.error !== ''" class="acl-grant__error">{{ row.error }}</span>
          <span v-else class="acl-grant__hint">—</span>
        </template>
      </el-table-column>
      <el-table-column v-if="!locked" label="" width="70">
        <template #default="{ $index }">
          <el-button link type="danger" data-test="acl-grant-remove" @click="removeRow($index)">
            {{ message('msg.admin.acls.grant.removeRow') }}
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <div v-if="!locked" class="acl-grant__actions">
      <el-button size="small" data-test="acl-grant-add" @click="addRow">
        {{ message('msg.admin.acls.grant.addRow') }}
      </el-button>
      <span class="acl-grant__hint">{{ message('msg.admin.acls.grant.duplicate') }}</span>
    </div>

    <p v-if="errorText !== ''" class="acl-grant__error" data-test="acl-grant-error">{{ errorText }}</p>

    <template #footer>
      <el-button :disabled="grant.status.value === 'running'" data-test="acl-grant-cancel" @click="setVisible(false)">
        {{ message('msg.admin.common.cancel') }}
      </el-button>
      <el-button
        type="primary"
        :loading="grant.status.value === 'running'"
        :disabled="grant.status.value === 'running'"
        data-test="acl-grant-submit"
        @click="save"
      >
        {{ message('msg.admin.common.save') }}
      </el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.acl-grant__actions {
  display: flex;
  align-items: center;
  gap: var(--bms-space-2);
  margin-top: var(--bms-space-2);
}
.acl-grant__hint {
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.acl-grant__error {
  margin: var(--bms-space-2) 0 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
</style>
