<script setup lang="ts">
/** 主题管理列表（分页 / 筛选 / 四态 / 详情深链；新建主题按 `msg:manage` 渲染）。 */

import {
  ElButton,
  ElDialog,
  ElForm,
  ElFormItem,
  ElInput,
  ElInputNumber,
  ElMessage,
  ElOption,
  ElSelect,
  ElTable,
  ElTableColumn,
} from 'element-plus'
import { computed, reactive, ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { DEFAULT_RETENTION_MS, MS_PER_DAY } from '../constants'
import { message } from '../i18n/messages'
import { hostPermissions, MSG_MANAGE_PERMISSION } from '../runtime'
import { createTopic, loadTopics, type TopicItem } from '../services/admin-service'
import { durationText } from '../services/time-format'

const view = useMsgReadOnlyView<TopicItem>({ loader: loadTopics })
const namespace = ref('')
const keyword = ref('')
const { go } = useAdminRoute()

/** 是否具备写权限（无 `msg:manage` 时不渲染入口）。 */
const canManage = computed(
  () => hostPermissions() === undefined || hostPermissions()?.includes(MSG_MANAGE_PERMISSION) === true,
)

/** 新建表单（保留期以「天」为输入单位，提交时换算为毫秒）。 */
const form = reactive({
  namespace: 'msg',
  name: '',
  partitionCount: 1,
  retentionDays: DEFAULT_RETENTION_MS / MS_PER_DAY,
  retentionBytes: 0,
})
const createVisible = ref(false)
/** 表单就地校验的文案键（空串表示通过）。 */
const formError = ref('')

const createAction = useMsgWriteAction({
  runner: () =>
    createTopic({
      namespace: form.namespace.trim(),
      name: form.name.trim(),
      partition_count: form.partitionCount,
      retention_ms: form.retentionDays * MS_PER_DAY,
      retention_bytes: form.retentionBytes,
      cleanup_policy: 'delete',
    }),
})

/** 应用筛选（一次提交全部条件，只发一个请求）。 */
function apply(): void {
  view.setFilters({
    namespace: namespace.value.trim(),
    keyword: keyword.value.trim(),
  })
}

/** 重置筛选（先清表单，再一次性提交默认条件）。 */
function reset(): void {
  namespace.value = ''
  keyword.value = ''
  view.setFilters({ namespace: '', keyword: '' })
}

/** 打开新建对话框（清空上一轮失败态）。 */
function openCreate(): void {
  createAction.reset()
  formError.value = ''
  form.namespace = 'msg'
  form.name = ''
  form.partitionCount = 1
  form.retentionDays = DEFAULT_RETENTION_MS / MS_PER_DAY
  form.retentionBytes = 0
  createVisible.value = true
}

/**
 * 新建表单校验（返回未通过的文案键，空串表示通过）。
 *
 * @returns 文案键。
 */
function validateCreate(): string {
  if (form.namespace.trim() === '') {
    return 'msg.admin.topics.create.namespaceRequired'
  }
  if (!/^[a-z0-9._-]{1,200}$/.test(form.name.trim())) {
    return 'msg.admin.topics.create.nameInvalid'
  }
  if (form.partitionCount < 1 || form.partitionCount > 1024) {
    return 'msg.admin.topics.create.partitionsInvalid'
  }
  if (!Number.isInteger(form.retentionDays) || form.retentionDays < 1) {
    return 'msg.admin.topics.create.retentionInvalid'
  }
  return ''
}

/** 提交新建：校验通过后提交；成功关闭、提示并刷新列表。 */
async function submitCreate(): Promise<void> {
  formError.value = validateCreate()
  if (formError.value !== '') {
    return
  }
  await createAction.run()
  if (createAction.status.value === 'done') {
    createVisible.value = false
    ElMessage.success(message('msg.admin.common.done'))
    await view.refresh()
  }
}

/**
 * 打开主题详情深链（按详情端点主键定位）。
 *
 * @param row 主题行。
 */
function openDetail(row: unknown): void {
  const item = row as TopicItem
  go(`/msg/topics/${item.id}`)
}

/**
 * 主题状态文案（未知枚举原样返回）。
 *
 * @param status 状态枚举。
 * @returns 文案。
 */
function statusText(status: string): string {
  return message(`msg.admin.topicState.${status}`)
}
</script>

<template>
  <section class="admin-page" data-test="page-topics">
    <div class="admin-page__head">
      <span />
      <el-button v-if="canManage" type="primary" data-test="topics-create" @click="openCreate">
        {{ message('msg.admin.topics.create.title') }}
      </el-button>
    </div>
    <read-only-list
      :title="message('msg.admin.topics.title')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :page="view.page.value"
      :page-size="view.pageSize.value"
      :total="view.total.value"
      :skeleton="view.firstLoad.value"
      @refresh="view.refresh"
      @update:page="view.setPage"
    >
      <template #filters>
        <div class="admin-page__filters">
          <el-input
            v-model="namespace"
            :placeholder="message('msg.admin.topics.namespace')"
            clearable
            data-test="topics-namespace"
          />
          <el-input
            v-model="keyword"
            :placeholder="message('msg.admin.topics.keyword')"
            clearable
            data-test="topics-keyword"
          />
          <el-button type="primary" data-test="topics-query" @click="apply">
            {{ message('msg.admin.common.query') }}
          </el-button>
          <el-button data-test="topics-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="topics-table">
        <el-table-column prop="name" :label="message('msg.admin.topics.name')" min-width="150" />
        <el-table-column prop="namespace" :label="message('msg.admin.topics.namespace')" min-width="120" />
        <el-table-column prop="partition_count" :label="message('msg.admin.topics.partitionCount')" min-width="90" />
        <el-table-column :label="message('msg.admin.topics.retention')" min-width="120">
          <template #default="{ row }">
            <span :title="String((row as TopicItem).retention_ms)">{{ durationText((row as TopicItem).retention_ms) }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.topics.status')" min-width="100">
          <template #default="{ row }">
            {{ statusText((row as TopicItem).status) }}
          </template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.common.detail')" min-width="110">
          <template #default="{ row }">
            <el-button link type="primary" data-test="topics-detail" @click="openDetail(row)">
              {{ message('msg.admin.common.detail') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </read-only-list>

    <el-dialog
      v-model="createVisible"
      :title="message('msg.admin.topics.create.title')"
      width="520px"
      :close-on-click-modal="false"
      data-test="topics-create-dialog"
    >
      <el-form label-width="120px" data-test="topics-create-form">
        <el-form-item :label="message('msg.admin.topics.namespace')">
          <el-select v-model="form.namespace" data-test="create-namespace">
            <el-option label="msg" value="msg" />
          </el-select>
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.name')">
          <el-input v-model="form.name" data-test="create-name" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.partitions')">
          <el-input-number v-model="form.partitionCount" :min="1" :max="1024" data-test="create-partitions" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.retentionMs')">
          <el-input-number v-model="form.retentionDays" :min="1" data-test="create-retention" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.retentionBytes')">
          <el-input-number v-model="form.retentionBytes" :min="0" data-test="create-bytes" />
          <span class="admin-page__hint">{{ message('msg.admin.topics.create.unlimited') }}</span>
        </el-form-item>
      </el-form>
      <p v-if="formError !== ''" class="admin-page__error" data-test="create-validation">
        {{ message(formError) }}
      </p>
      <p v-if="createAction.errorKey.value !== ''" class="admin-page__error" data-test="create-error">
        {{ message(createAction.errorKey.value) }}
      </p>
      <template #footer>
        <el-button
          :disabled="createAction.status.value === 'running'"
          data-test="create-cancel"
          @click="createVisible = false"
        >
          {{ message('msg.admin.common.cancel') }}
        </el-button>
        <el-button
          type="primary"
          :loading="createAction.status.value === 'running'"
          :disabled="createAction.status.value === 'running'"
          data-test="create-submit"
          @click="submitCreate"
        >
          {{ message('msg.admin.topics.create.title') }}
        </el-button>
      </template>
    </el-dialog>
  </section>
</template>

<style scoped>
.admin-page {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.admin-page__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--bms-space-2);
}
.admin-page__hint {
  margin-left: var(--bms-space-2);
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.admin-page__error {
  margin: 0;
  color: var(--bms-color-danger);
  font-size: 12px;
}
.admin-page__filters {
  display: flex;
  gap: var(--bms-space-2);
  flex-wrap: wrap;
}
.admin-page__filters :deep(.el-input) {
  max-width: 200px;
}
</style>
