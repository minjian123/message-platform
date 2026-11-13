<script setup lang="ts">
/**
 * 消息查询（组合筛选 + 消息头详情）。
 *
 * **只展示头与摘要，不展示正文**：响应结构里没有正文字段，页面也不提供正文入口。
 * 时间选择器给本地时间，上送前统一转 UTC 带 `Z`；展示转本地。
 */

import {
  ElButton,
  ElDatePicker,
  ElDescriptions,
  ElDescriptionsItem,
  ElInput,
  ElTable,
  ElTableColumn,
} from 'element-plus'
import { onMounted, ref } from 'vue'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useAdminRoute } from '../composables/useAdminRoute'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadMessages, type MessageItem } from '../services/admin-service'
import { isValidLocal, toLocalText, toUtcParam } from '../services/time-format'

const { params, query } = useAdminRoute()

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
/** 时间格式校验错误（空串表示通过）。 */
const timeError = ref('')

const view = useMsgReadOnlyView<MessageItem>({ loader: loadMessages, active: false, autoLoad: false })

/**
 * 当前条件快照（空值不落参，避免上送空串）。
 *
 * @returns 筛选条件。
 */
function currentFilters(): Record<string, string> {
  return {
    namespace: namespace.value.trim(),
    topic: topic.value.trim(),
    partition: partition.value.trim(),
    offset_from: offsetFrom.value.trim(),
    offset_to: offsetTo.value.trim(),
    key: key.value.trim(),
    time_from: toUtcParam(timeFrom.value) ?? '',
    time_to: toUtcParam(timeTo.value) ?? '',
    log_id: logId.value.trim(),
  }
}

/** 应用组合筛选；命名空间与主题必填，缺参退回「待选条件」态。 */
function apply(): void {
  if (!isValidLocal(timeFrom.value) || !isValidLocal(timeTo.value)) {
    timeError.value = message('msg.admin.common.invalidTime')
    return
  }
  timeError.value = ''
  if (namespace.value === '' || topic.value === '') {
    view.setActive(false)
    return
  }
  view.setFilters(currentFilters())
  view.setActive(true)
}

/** 重置全部条件（先清表单，再一次性提交空条件）。 */
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
  timeError.value = ''
  view.setActive(false)
  view.setFilters({})
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

// 深链 `/msg/messages/:logId`：带入 log_id（命名空间 / 主题取查询参数）并自动查询一次。
onMounted(() => {
  const target = params.logId ?? ''
  if (target === '') {
    return
  }
  namespace.value = query.namespace ?? ''
  topic.value = query.topic ?? ''
  logId.value = target
  apply()
})
</script>

<template>
  <section class="admin-page" data-test="page-messages">
    <p class="admin-page__hint" data-test="messages-no-body">{{ message('msg.admin.messages.noBody') }}</p>
    <read-only-list
      :title="message('msg.admin.messages.title')"
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
          <el-input v-model="namespace" :placeholder="message('msg.admin.messages.namespace')" data-test="messages-namespace" />
          <el-input v-model="topic" :placeholder="message('msg.admin.messages.topic')" data-test="messages-topic" />
          <el-input v-model="partition" :placeholder="message('msg.admin.messages.partition')" data-test="messages-partition" />
          <el-input v-model="offsetFrom" :placeholder="message('msg.admin.messages.offsetFrom')" data-test="messages-offset-from" />
          <el-input v-model="offsetTo" :placeholder="message('msg.admin.messages.offsetTo')" data-test="messages-offset-to" />
          <el-input v-model="key" :placeholder="message('msg.admin.messages.key')" data-test="messages-key" />
          <el-date-picker
            v-model="timeFrom"
            type="datetime"
            value-format="YYYY-MM-DDTHH:mm:ss"
            :placeholder="message('msg.admin.messages.timeFrom')"
            data-test="messages-time-from"
          />
          <el-date-picker
            v-model="timeTo"
            type="datetime"
            value-format="YYYY-MM-DDTHH:mm:ss"
            :placeholder="message('msg.admin.messages.timeTo')"
            data-test="messages-time-to"
          />
          <el-input v-model="logId" :placeholder="message('msg.admin.messages.logId')" data-test="messages-log-id" />
          <el-button type="primary" data-test="messages-query" @click="apply">{{ message('msg.admin.common.query') }}</el-button>
          <el-button data-test="messages-reset" @click="reset">{{ message('msg.admin.common.reset') }}</el-button>
        </div>
        <p v-if="timeError !== ''" class="admin-page__error" data-test="messages-time-error">{{ timeError }}</p>
      </template>
      <el-table :data="view.rows.value" size="small" border data-test="messages-table">
        <el-table-column prop="log_id" :label="message('msg.admin.messages.logId')" min-width="180" />
        <el-table-column prop="partition" :label="message('msg.admin.messages.partition')" min-width="80" />
        <el-table-column prop="offset" :label="message('msg.admin.messages.offset')" min-width="90" />
        <el-table-column :label="message('msg.admin.messages.publishedAt')" min-width="180">
          <template #default="{ row }">{{ toLocalText((row as MessageItem).published_at) }}</template>
        </el-table-column>
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
        <el-descriptions-item :label="message('msg.admin.messages.publishedAt')">
          {{ toLocalText(selected.published_at) }}
        </el-descriptions-item>
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
  max-width: 170px;
}
.admin-page h2 {
  margin: 0;
  font-size: 15px;
}
</style>
