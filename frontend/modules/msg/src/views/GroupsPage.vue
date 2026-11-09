<script setup lang="ts">
/** 消费组列表（分页 / 筛选 / 四态 / 详情深链；位点重置归 06-2）。 */

import { ElButton, ElInput, ElTable, ElTableColumn } from 'element-plus'
import { ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadGroups, type GroupItem } from '../services/admin-service'

const view = useMsgReadOnlyView<GroupItem>({ loader: loadGroups })
const namespace = ref('')
const topic = ref('')
const { go } = useAdminRoute()

/** 应用筛选。 */
function apply(): void {
  view.setFilter('namespace', namespace.value)
  view.setFilter('topic', topic.value)
}

/** 重置筛选。 */
function reset(): void {
  namespace.value = ''
  topic.value = ''
  view.reset()
}

/**
 * 打开消费组详情深链。
 *
 * @param row 组列表行。
 */
function openDetail(row: unknown): void {
  const item = row as GroupItem
  go(`/msg/groups/${encodeURIComponent(item.namespace)}/${encodeURIComponent(item.topic)}/${encodeURIComponent(item.group)}`)
}
</script>

<template>
  <section class="admin-page" data-test="page-groups">
    <h1>{{ message('msg.admin.groups.title') }}</h1>
    <p class="admin-page__hint">{{ message('msg.admin.groups.laterReset') }}</p>
    <read-only-list
      :title="message('msg.admin.groups.title')"
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
          <el-input v-model="namespace" :placeholder="message('msg.admin.groups.namespace')" clearable data-test="groups-namespace" />
          <el-input v-model="topic" :placeholder="message('msg.admin.groups.topic')" clearable data-test="groups-topic" />
          <el-button type="primary" data-test="groups-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="groups-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="groups-table">
        <el-table-column prop="group" :label="message('msg.admin.groups.group')" min-width="150" />
        <el-table-column prop="namespace" :label="message('msg.admin.groups.namespace')" min-width="110" />
        <el-table-column prop="topic" :label="message('msg.admin.groups.topic')" min-width="140" />
        <el-table-column prop="member_count" :label="message('msg.admin.groups.memberCount')" min-width="90" />
        <el-table-column prop="assigned_partitions" :label="message('msg.admin.groups.assigned')" min-width="110" />
        <el-table-column prop="state" :label="message('msg.admin.groups.state')" min-width="100" />
        <el-table-column :label="message('msg.admin.common.detail')" min-width="110">
          <template #default="{ row }">
            <el-button link type="primary" data-test="groups-detail" @click="openDetail(row)">
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
