<script setup lang="ts">
/** 主题管理列表（分页 / 筛选 / 四态 / 详情深链；新建主题按 `msg:manage` 渲染）。 */

import { ElButton, ElDialog, ElForm, ElFormItem, ElInput, ElInputNumber, ElTable, ElTableColumn } from 'element-plus'
import { computed, reactive, ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { useMsgWriteAction } from '../composables/useMsgWriteAction'
import { message } from '../i18n/messages'
import { hostPermissions } from '../runtime'
import { createTopic, loadTopics, type TopicItem } from '../services/admin-service'

const view = useMsgReadOnlyView<TopicItem>({ loader: loadTopics })
const namespace = ref('')
const keyword = ref('')
const { go } = useAdminRoute()

/** 是否具备写权限（无 `msg:manage` 时不渲染入口）。 */
const canManage = computed(() => hostPermissions() === undefined || hostPermissions()?.includes('msg:manage') === true)

/** 新建表单（对话框打开时保持上次输入，失败可改后重试）。 */
const form = reactive({
  namespace: 'msg',
  name: '',
  partitionCount: 1,
  retentionMs: 86_400_000,
  retentionBytes: 0,
})
const createVisible = ref(false)
const createAction = useMsgWriteAction({
  runner: () =>
    createTopic({
      namespace: form.namespace,
      name: form.name,
      partition_count: form.partitionCount,
      retention_ms: form.retentionMs,
      retention_bytes: form.retentionBytes,
      cleanup_policy: 'delete',
    }),
})

/** 应用筛选。 */
function apply(): void {
  view.setFilter('namespace', namespace.value)
  view.setFilter('keyword', keyword.value)
}

/** 重置筛选。 */
function reset(): void {
  namespace.value = ''
  keyword.value = ''
  view.reset()
}

/** 打开新建对话框（清空上一轮失败态）。 */
function openCreate(): void {
  createAction.reset()
  createVisible.value = true
}

/** 提交新建：成功后关闭并刷新列表，失败保留表单由弹窗内提示。 */
async function submitCreate(): Promise<void> {
  await createAction.run()
  if (createAction.status.value === 'done') {
    createVisible.value = false
    view.refresh()
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
</script>

<template>
  <section class="admin-page" data-test="page-topics">
    <div class="admin-page__head">
      <h1>{{ message('msg.admin.topics.title') }}</h1>
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
      @refresh="view.refresh"
      @update:page="view.setPage"
    >
      <template #filters>
        <div class="admin-page__filters">
          <el-input v-model="namespace" :placeholder="message('msg.admin.topics.namespace')" clearable data-test="topics-namespace" />
          <el-input v-model="keyword" :placeholder="message('msg.admin.topics.keyword')" clearable data-test="topics-keyword" />
          <el-button type="primary" data-test="topics-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="topics-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="topics-table">
        <el-table-column prop="name" :label="message('msg.admin.topics.name')" min-width="150" />
        <el-table-column prop="namespace" :label="message('msg.admin.topics.namespace')" min-width="120" />
        <el-table-column prop="partition_count" :label="message('msg.admin.topics.partitionCount')" min-width="90" />
        <el-table-column prop="retention_ms" :label="message('msg.admin.topics.retention')" min-width="120" />
        <el-table-column prop="status" :label="message('msg.admin.topics.status')" min-width="100" />
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
          <el-input v-model="form.namespace" data-test="create-namespace" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.name')">
          <el-input v-model="form.name" data-test="create-name" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.partitions')">
          <el-input-number v-model="form.partitionCount" :min="1" :max="1024" data-test="create-partitions" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.retentionMs')">
          <el-input-number v-model="form.retentionMs" :min="1" data-test="create-retention" />
        </el-form-item>
        <el-form-item :label="message('msg.admin.topics.create.retentionBytes')">
          <el-input-number v-model="form.retentionBytes" :min="0" data-test="create-bytes" />
          <span class="admin-page__hint">{{ message('msg.admin.topics.create.unlimited') }}</span>
        </el-form-item>
      </el-form>
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
