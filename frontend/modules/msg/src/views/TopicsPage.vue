<script setup lang="ts">
/** 主题管理列表（分页 / 筛选 / 四态 / 详情深链；只读，新建主题归 06-2）。 */

import { ElButton, ElInput, ElTable, ElTableColumn } from 'element-plus'
import { ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadTopics, type TopicItem } from '../services/admin-service'

const view = useMsgReadOnlyView<TopicItem>({ loader: loadTopics })
const namespace = ref('')
const keyword = ref('')
const { go } = useAdminRoute()

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
    <h1>{{ message('msg.admin.topics.title') }}</h1>
    <p class="admin-page__hint">{{ message('msg.admin.topics.laterCreate') }}</p>
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
  </section>
</template>

<style scoped>
.admin-page {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.admin-page__hint {
  margin: 0;
  color: var(--bms-color-text-secondary);
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
