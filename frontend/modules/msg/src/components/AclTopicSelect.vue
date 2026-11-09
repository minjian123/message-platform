<script setup lang="ts">
/**
 * 主题选择件：从 01-4 主题列表接口远程搜索；选中后回传主题名与命名空间。
 *
 * 命名空间已填时按命名空间收窄；未填时全局搜索，选中后自动带出命名空间。
 */

import { ElOption, ElSelect } from 'element-plus'
import { ref, watch } from 'vue'

import { message } from '../i18n/messages'
import { fetchTopics, type TopicItem } from '../services/admin-service'

interface Props {
  /** 主题名。 */
  modelValue: string
  /** 命名空间（空串表示不限）。 */
  namespace?: string
  /** 是否禁用。 */
  disabled?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  namespace: '',
  disabled: false,
})

const emit = defineEmits<{
  'update:modelValue': [value: string]
  select: [payload: { name: string; namespace: string }]
}>()

/** 候选项。 */
const options = ref<TopicItem[]>([])
const loading = ref(false)

/**
 * 远程搜索主题。
 *
 * @param keyword 主题名关键字。
 */
async function search(keyword: string): Promise<void> {
  loading.value = true
  try {
    const page = await fetchTopics({
      namespace: props.namespace === '' ? undefined : props.namespace,
      keyword: keyword === '' ? undefined : keyword,
      page: 1,
      size: 20,
    })
    options.value = page.items ?? []
  } catch {
    options.value = []
  } finally {
    loading.value = false
  }
}

/**
 * 选中主题：回传名称与命名空间（自动带出）。
 *
 * @param value 主题名。
 */
function onChange(value: string): void {
  emit('update:modelValue', value)
  const selected = options.value.find((item) => item.name === value)
  if (selected !== undefined) {
    emit('select', { name: selected.name, namespace: selected.namespace })
  }
}

watch(
  () => props.namespace,
  () => {
    void search('')
  },
  { immediate: true },
)

watch(
  () => props.modelValue,
  (value) => {
    if (value !== '' && !options.value.some((item) => item.name === value)) {
      options.value = [{ name: value, namespace: props.namespace } as TopicItem, ...options.value]
    }
  },
)
</script>

<template>
  <el-select
    :model-value="modelValue"
    size="small"
    :disabled="disabled"
    filterable
    remote
    :remote-method="search"
    :loading="loading"
    :placeholder="message('msg.admin.acls.grant.topicPlaceholder')"
    data-test="acl-topic-select"
    @update:model-value="onChange"
  >
    <el-option
      v-for="item in options"
      :key="`${item.namespace}/${item.name}`"
      :label="item.name"
      :value="item.name"
    >
      <span>{{ item.name }}</span>
      <span class="acl-topic-option__namespace">{{ item.namespace }}</span>
    </el-option>
  </el-select>
</template>

<style scoped>
.acl-topic-option__namespace {
  float: right;
  color: var(--bms-color-text-secondary);
  font-size: 11px;
}
</style>
