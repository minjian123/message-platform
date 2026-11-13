<script setup lang="ts">
/**
 * 主题选择件：从 01-4 主题列表接口远程搜索；选中后回传主题名与命名空间。
 *
 * 命名空间已填时按命名空间收窄；未填时全局搜索，选中后自动带出命名空间。
 */

import { ElOption, ElSelect } from 'element-plus'
import { computed, ref, watch } from 'vue'

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
/** 候选加载失败（不阻断手填主题名）。 */
const failed = ref(false)

/**
 * 复合候选键（`命名空间/主题名`）：同名主题跨命名空间也能区分。
 *
 * @param item 主题行。
 * @returns 候选键。
 */
function candidateKey(item: TopicItem): string {
  return `${item.namespace}/${item.name}`
}

/** 当前选中项对应的复合键（找不到时回落原始主题名）。 */
const selectedKey = computed(() => {
  const match = options.value.find(
    (item) => item.name === props.modelValue && (props.namespace === '' || item.namespace === props.namespace),
  )
  return match === undefined ? props.modelValue : candidateKey(match)
})

/**
 * 远程搜索主题。
 *
 * @param keyword 主题名关键字。
 */
async function search(keyword: string): Promise<void> {
  loading.value = true
  failed.value = false
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
    failed.value = true
  } finally {
    loading.value = false
  }
}

/**
 * 选中主题：拆回复合键，回传主题名与命名空间（自动带出）。
 *
 * @param value 复合键（`命名空间/主题名`）。
 */
function onChange(value: string): void {
  const slash = value.indexOf('/')
  if (slash < 0) {
    emit('update:modelValue', value)
    return
  }
  const namespace = value.slice(0, slash)
  const name = value.slice(slash + 1)
  emit('update:modelValue', name)
  emit('select', { name, namespace })
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
    :model-value="selectedKey"
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
      :key="candidateKey(item)"
      :label="item.name"
      :value="candidateKey(item)"
    >
      <span>{{ item.name }}</span>
      <span class="acl-topic-option__namespace">{{ item.namespace }}</span>
    </el-option>
  </el-select>
  <p v-if="failed" class="acl-topic-error" data-test="acl-topic-error">
    {{ message('msg.admin.acls.topicsUnavailable') }}
  </p>
</template>

<style scoped>
.acl-topic-option__namespace {
  float: right;
  color: var(--bms-color-text-secondary);
  font-size: 11px;
}
.acl-topic-error {
  margin: var(--bms-space-1) 0 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
</style>
