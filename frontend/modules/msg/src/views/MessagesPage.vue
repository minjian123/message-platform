<script setup lang="ts">
/**
 * 消息查询（组合筛选 + 消息头详情）。
 *
 * **只展示头与摘要，不展示正文**：响应结构里没有正文字段，页面也不提供正文入口。
 */

import { ElButton, ElDescriptions, ElDescriptionsItem, ElInput, ElTable, ElTableColumn } from 'element-plus'
import { ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadMessages, type MessageItem } from '../services/admin-service'

const namespace = ref('')
const topic = ref('')
const partition = ref('')
const offsetFrom = ref('')
const offsetTo = ref('')
const key = ref('')
const timeFrom = ref('')
const timeTo = ref('')
const logId = ref('')
const selected = ref<MessageItem | null>(null)

const view = useMsgReadOnlyView<MessageItem>({ loader: loadMessages, active: false, autoLoad: false })

/** 应用组合筛选；命名空间与主题必填，缺参退回「待选条件」态。 */
function apply(): void {
  if (namespace.value === '' || topic.value === '') {
    view.setActive(false)
    return
  }
  view.setFilter('namespace', namespace.value)
  view.setFilter('topic', topic.value)
  view.setFilter('partition', partition.value)
  view.setFilter('offset_from', offsetFrom.value)
  view.setFilter('offset_to', offsetTo.value)
  view.setFilter('key', key.value)
  view.setFilter('time_from', timeFrom.value)
  view.setFilter('time_to', timeTo.value)
  view.setFilter('log_id', logId.value)
  view.setActive(true)
}

/** 重置全部条件。 */
function reset(): void {
  namespace.value = ''
  topic.value = ''
  partition.value = ''
  offsetFrom.value = ''
  offsetTo.value = ''
  key.value = ''
  timeFrom.value = ''
  timeTo.value = ''
  logId.value = ''
  selected.value = null
  view.reset()
  view.setActive(false)
}

/**
 * 选中一行展示消息头。
 *
 * @param row 消息头行。
 */
function openDetail(row: unknown): void {
  selected.value = row as MessageItem
}

/**
 * 自定义头展示（无正文）。
 *
 * @param row 消息头行。
 * @returns 头字段文本。
 */
function headersOf(row: MessageItem): string {
  return Object.entries(row.headers ?? {})
    .map(([name, value]) => `${name}=${String(value)}`)
    .join(', ')
}
</script>

<template>
  <section class="admin-page" data-test="page-messages">
    <h1>{{ message('msg.admin.messages.title') }}</h1>
    <p class="admin-page__hint" data-test="messages-no-body">{{ message('msg.admin.messages.noBody') }}</p>
    <read-only-list
      :title="message('msg.admin.messages.title')"
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
          <el-input v-model="namespace" :placeholder="message('msg.admin.messages.namespace')" data-test="messages-namespace" />
          <el-input v-model="topic" :placeholder="message('msg.admin.messages.topic')" data-test="messages-topic" />
          <el-input v-model="partition" :placeholder="message('msg.admin.messages.partition')" data-test="messages-partition" />
          <el-input v-model="offsetFrom" :placeholder="message('msg.admin.messages.offsetFrom')" data-test="messages-offset-from" />
          <el-input v-model="offsetTo" :placeholder="message('msg.admin.messages.offsetTo')" data-test="messages-offset-to" />
          <el-input v-model="key" :placeholder="message('msg.admin.messages.key')" data-test="messages-key" />
          <el-input v-model="timeFrom" :placeholder="message('msg.admin.messages.timeFrom')" data-test="messages-time-from" />
          <el-input v-model="timeTo" :placeholder="message('msg.admin.messages.timeTo')" data-test="messages-time-to" />
          <el-input v-model="logId" :placeholder="message('msg.admin.messages.logId')" data-test="messages-log-id" />
          <el-button type="primary" data-test="messages-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="messages-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="messages-table">
        <el-table-column prop="log_id" :label="message('msg.admin.messages.logId')" min-width="180" />
        <el-table-column prop="partition" :label="message('msg.admin.messages.partition')" min-width="80" />
        <el-table-column prop="offset" :label="message('msg.admin.messages.offset')" min-width="90" />
        <el-table-column prop="published_at" :label="message('msg.admin.messages.publishedAt')" min-width="180" />
        <el-table-column prop="payload_len" :label="message('msg.admin.messages.payloadLen')" min-width="110" />
        <el-table-column prop="payload_sha256" :label="message('msg.admin.messages.sha256')" min-width="200" />
        <el-table-column :label="message('msg.admin.common.detail')" min-width="110">
          <template #default="{ row }">
            <el-button link type="primary" data-test="messages-detail" @click="openDetail(row)">
              {{ message('msg.admin.common.detail') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </read-only-list>
    <section v-if="selected !== null" data-test="messages-header">
      <h2>{{ message('msg.admin.messages.detail') }}</h2>
      <el-descriptions :column="2" border size="small">
        <el-descriptions-item :label="message('msg.admin.messages.logId')">{{ selected.log_id }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.messages.messageId')">{{ selected.message_id }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.messages.partition')">{{ selected.partition }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.messages.offset')">{{ selected.offset }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.messages.publishedAt')">{{ selected.published_at }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.messages.headers')">{{ headersOf(selected) }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.messages.payloadLen')">{{ selected.payload_len }}</el-descriptions-item>
        <el-descriptions-item :label="message('msg.admin.messages.sha256')">{{ selected.payload_sha256 }}</el-descriptions-item>
      </el-descriptions>
    </section>
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
  max-width: 170px;
}
.admin-page h2 {
  margin: 0;
  font-size: 15px;
}
</style>
