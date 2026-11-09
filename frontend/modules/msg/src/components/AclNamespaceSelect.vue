<script setup lang="ts">
/**
 * 命名空间选择件（主题授权授予抽屉 / 筛选共用）。
 *
 * `filterable` + `remote` + `allow-create`：优先从主题列表接口搜索 distinct 命名空间，
 * 未命中时允许直接输入（不阻断授权）；接口失败时保留手输能力。
 */

import { ElOption, ElSelect } from 'element-plus'
import { computed, onMounted, ref, watch } from 'vue'

import { message } from '../i18n/messages'
import { fetchTopics } from '../services/admin-service'

interface Props {
  /** 当前命名空间。 */
  modelValue: string
  /** 尺寸（缺省 `small`）。 */
  size?: 'small' | 'default'
  /** 是否禁用。 */
  disabled?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  size: 'small',
  disabled: false,
})

const emit = defineEmits<{
  'update:modelValue': [value: string]
}>()

/** 候选命名空间（去重、保序）。 */
const options = ref<string[]>([])
const loading = ref(false)

/** 合并当前值，避免已选值不在候选时被清空。 */
const mergedOptions = computed(() => {
  const list = [...options.value]
  if (props.modelValue !== '' && !list.includes(props.modelValue)) {
    list.unshift(props.modelValue)
  }
  return list
})

/**
 * 远程搜索命名空间（取主题列表前 20 条的 distinct 值）。
 *
 * @param keyword 关键字。
 */
async function search(keyword: string): Promise<void> {
  loading.value = true
  try {
    const page = await fetchTopics({ keyword: keyword === '' ? undefined : keyword, page: 1, size: 20 })
    const namespaces = new Set<string>()
    for (const item of page.items ?? []) {
      namespaces.add(item.namespace)
    }
    options.value = [...namespaces]
  } catch {
    options.value = []
  } finally {
    loading.value = false
  }
}

watch(
  () => props.modelValue,
  (value) => {
    if (value !== '' && !options.value.includes(value)) {
      options.value = [value, ...options.value]
    }
  },
)

onMounted(() => {
  void search('')
})
</script>

<template>
  <el-select
    :model-value="modelValue"
    :size="size"
    :disabled="disabled"
    filterable
    allow-create
    default-first-option
    remote
    :remote-method="search"
    :loading="loading"
    :placeholder="message('msg.admin.acls.grant.namespacePlaceholder')"
    data-test="acl-namespace-select"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-option v-for="item in mergedOptions" :key="item" :label="item" :value="item" />
  </el-select>
</template>
