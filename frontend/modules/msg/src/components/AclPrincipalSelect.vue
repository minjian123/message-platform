<script setup lang="ts">
/**
 * 主体选择件（用户 / 角色）：优先平台目录搜索，失败或无权限降级手填主体标识。
 *
 * - 已知缺 `user:query` / `role:query`：直接手填，不发注定 403 的请求；
 * - 未装载权限：搜索即探测，失败切手填；
 * - 手填只接受数字（雪花主键十进制字符串，≤64 位）。
 */

import { ElButton, ElInput, ElOption, ElSelect } from 'element-plus'
import { computed, onMounted, ref, watch } from 'vue'

import { message } from '../i18n/messages'
import {
  ROLE_QUERY_PERMISSION,
  USER_QUERY_PERMISSION,
  searchPlatformRoles,
  searchPlatformUsers,
} from '../services/platform-directory'
import { hostPermissions } from '../runtime'

interface Props {
  /** 主体类型（user / role）。 */
  principalType: string
  /** 主体标识。 */
  modelValue: string
  /** 已知名称（编辑回显）。 */
  label?: string
  /** 已持有权限码（缺省取宿主注入快照；测试可显式传）。 */
  permissionCodes?: readonly string[] | undefined
}

const props = withDefaults(defineProps<Props>(), {
  label: '',
  permissionCodes: undefined,
})

const emit = defineEmits<{
  'update:modelValue': [value: string]
  'update:label': [value: string]
}>()

/** 候选项。 */
interface PrincipalOption {
  value: string
  label: string
}

const mode = ref<'search' | 'manual'>('search')
const searchFailed = ref(false)
const loading = ref(false)
const options = ref<PrincipalOption[]>([])
const selectedLabel = ref(props.label)
const digitsError = ref(false)

/** 当前权限码（未注入为 `undefined`）。 */
const codes = computed(() => (props.permissionCodes === undefined ? hostPermissions() : props.permissionCodes))
/** 当前类型所需权限码。 */
const requiredPermission = computed(() => (props.principalType === 'role' ? ROLE_QUERY_PERMISSION : USER_QUERY_PERMISSION))

/** 合并当前值，避免已选值不在候选时被清空。 */
const mergedOptions = computed<PrincipalOption[]>(() => {
  const list = [...options.value]
  if (props.modelValue !== '' && !list.some((item) => item.value === props.modelValue)) {
    list.unshift({ value: props.modelValue, label: selectedLabel.value === '' ? props.modelValue : selectedLabel.value })
  }
  return list
})

/** 切换到手填态。 */
function switchManual(): void {
  mode.value = 'manual'
  searchFailed.value = true
  selectedLabel.value = ''
  emit('update:label', '')
}

/**
 * 是否已知缺权限（已装载且不含所需权限码）。
 *
 * @returns 是否缺权限。
 */
function knownMissing(): boolean {
  const current = codes.value
  return current !== undefined && !current.includes(requiredPermission.value)
}

/**
 * 搜索候选。
 *
 * @param keyword 关键字。
 */
async function search(keyword: string): Promise<void> {
  if (knownMissing()) {
    switchManual()
    return
  }
  loading.value = true
  try {
    if (props.principalType === 'role') {
      const page = await searchPlatformRoles(keyword)
      options.value = (page.list ?? []).map((item) => ({ value: item.id, label: item.name }))
    } else {
      const page = await searchPlatformUsers(keyword)
      options.value = (page.list ?? []).map((item) => ({
        value: item.id,
        label: item.name === '' ? item.username : `${item.name}（${item.username}）`,
      }))
    }
    mode.value = 'search'
  } catch {
    switchManual()
  } finally {
    loading.value = false
  }
}

/**
 * 下拉展开时加载默认候选。
 *
 * @param visible 是否展开。
 */
function onVisible(visible: boolean): void {
  if (visible) {
    void search('')
  }
}

/**
 * 选择候选。
 *
 * @param value 主体标识。
 */
function onSelect(value: string): void {
  const selected = mergedOptions.value.find((item) => item.value === value)
  selectedLabel.value = selected?.label ?? ''
  emit('update:modelValue', value)
  emit('update:label', selectedLabel.value)
}

/**
 * 手填输入。
 *
 * @param value 输入值。
 */
function onManualInput(value: string): void {
  const trimmed = value.trim()
  digitsError.value = trimmed !== '' && (!/^\d+$/.test(trimmed) || trimmed.length > 64)
  if (digitsError.value) {
    return
  }
  selectedLabel.value = ''
  emit('update:modelValue', trimmed)
  emit('update:label', '')
}

onMounted(() => {
  if (knownMissing()) {
    switchManual()
  }
})

watch(
  () => props.principalType,
  () => {
    mode.value = 'search'
    searchFailed.value = false
    digitsError.value = false
    options.value = []
    selectedLabel.value = ''
    emit('update:modelValue', '')
    emit('update:label', '')
    if (knownMissing()) {
      switchManual()
    }
  },
)

watch(
  () => props.label,
  (value) => {
    selectedLabel.value = value
  },
)

watch(
  () => props.modelValue,
  (value) => {
    if (value !== '' && !options.value.some((item) => item.value === value)) {
      options.value = [{ value, label: selectedLabel.value === '' ? value : selectedLabel.value }, ...options.value]
    }
  },
)
</script>

<template>
  <div class="acl-principal" data-test="acl-principal-select">
    <el-input
      v-if="mode === 'manual'"
      :model-value="modelValue"
      size="small"
      maxlength="64"
      :placeholder="message('msg.admin.acls.grant.manual')"
      data-test="acl-principal-manual"
      @update:model-value="onManualInput"
    />
    <el-select
      v-else
      :model-value="modelValue"
      size="small"
      filterable
      remote
      :remote-method="search"
      :loading="loading"
      :placeholder="
        message(
          principalType === 'role' ? 'msg.admin.acls.grant.searchRole' : 'msg.admin.acls.grant.searchUser',
        )
      "
      data-test="acl-principal-search"
      @visible-change="onVisible"
      @update:model-value="onSelect"
    >
      <el-option v-for="item in mergedOptions" :key="item.value" :label="item.label" :value="item.value" />
    </el-select>
    <el-button
      v-if="mode === 'search'"
      link
      type="primary"
      size="small"
      data-test="acl-principal-switch-manual"
      @click="switchManual"
    >
      {{ message('msg.admin.acls.grant.manual') }}
    </el-button>
    <p v-if="searchFailed" class="acl-principal__hint" data-test="acl-principal-search-failed">
      {{ message('msg.admin.acls.grant.searchFailed') }}
    </p>
    <p v-if="digitsError" class="acl-principal__error" data-test="acl-principal-digits">
      {{ message('msg.admin.acls.grant.principalDigits') }}
    </p>
  </div>
</template>

<style scoped>
.acl-principal {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.acl-principal__hint {
  margin: 0;
  color: var(--bms-color-warning);
  font-size: 11px;
}
.acl-principal__error {
  margin: 0;
  color: var(--bms-color-danger);
  font-size: 11px;
}
</style>
