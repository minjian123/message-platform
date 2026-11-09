<script setup lang="ts">
/**
 * 死信与重放（只读入口）。
 *
 * 契约快照尚无死信列表 / 重放任务列表的只读端点（归 06-5 / 06-6），
 * 本页不造端点、不伪造示例数据：两张表按空态呈现并明确标注后续任务占位。
 */

import { ElTable, ElTableColumn } from 'element-plus'
import { computed } from 'vue'

import type { ReadOnlyViewState } from '../base/BaseMsgReadOnlyView'
import ReadOnlyList from '../components/ReadOnlyList.vue'
import { message } from '../i18n/messages'
import { MSG_QUERY_PERMISSION, hostPermissions } from '../runtime'

/** 展示层权限判定（集合未装载按宿主守卫口径放行）。 */
const denied = computed(() => {
  const codes = hostPermissions()
  return codes !== undefined && !codes.includes(MSG_QUERY_PERMISSION)
})
/** 展示态：无权限为 denied，其余展示空表 + 占位说明（表内空态由 `empty-text` 承担）。 */
const state = computed<ReadOnlyViewState>(() => (denied.value ? 'denied' : 'ready'))
/** 展示态文案键。 */
const textKey = computed(() => (denied.value ? 'state.denied' : 'msg.admin.dlq.placeholder'))
</script>

<template>
  <section class="admin-page" data-test="page-dlq">
    <h1>{{ message('msg.admin.dlq.title') }}</h1>
    <read-only-list
      :title="message('msg.admin.dlq.deadLetters')"
      :state="state"
      :text-key="textKey"
      :paginated="false"
      :skeleton="false"
    >
      <el-table :data="[]" size="small" border data-test="dlq-dead-table" :empty-text="message('state.empty')">
        <el-table-column prop="log_id" :label="message('msg.admin.messages.logId')" min-width="160" />
        <el-table-column prop="topic" :label="message('msg.admin.dlq.topic')" min-width="140" />
        <el-table-column prop="offset" :label="message('msg.admin.messages.offset')" min-width="90" />
        <el-table-column prop="reason" :label="message('msg.admin.groups.state')" min-width="120" />
        <el-table-column prop="delivery_count" :label="message('msg.admin.delayed.count')" min-width="90" />
        <el-table-column prop="status" :label="message('msg.admin.dlq.status')" min-width="100" />
        <el-table-column prop="trace_id" :label="message('msg.admin.messages.traceId')" min-width="140" />
      </el-table>
    </read-only-list>
    <read-only-list
      :title="message('msg.admin.dlq.replayTasks')"
      :state="state"
      :text-key="textKey"
      :paginated="false"
      :skeleton="false"
    >
      <el-table :data="[]" size="small" border data-test="dlq-replay-table" :empty-text="message('state.empty')">
        <el-table-column prop="task_id" :label="message('msg.admin.dlq.title')" min-width="140" />
        <el-table-column prop="topic" :label="message('msg.admin.dlq.topic')" min-width="140" />
        <el-table-column prop="condition" :label="message('msg.admin.delayed.hint')" min-width="140" />
        <el-table-column prop="rate" :label="message('msg.admin.overview.throughput')" min-width="100" />
        <el-table-column prop="progress" :label="message('msg.admin.delayed.count')" min-width="100" />
        <el-table-column prop="status" :label="message('msg.admin.dlq.status')" min-width="100" />
      </el-table>
    </read-only-list>
    <div class="admin-page__later" data-test="dlq-later">
      <p>{{ message('msg.admin.dlq.placeholder') }}</p>
      <p>{{ message('msg.admin.common.later.dispose') }} · {{ message('msg.admin.common.later.replay') }}</p>
      <p>{{ message('msg.admin.common.later.write') }}</p>
    </div>
  </section>
</template>

<style scoped>
.admin-page {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-3);
}
.admin-page__later {
  border: 1px dashed var(--bms-color-border);
  border-radius: 6px;
  padding: var(--bms-space-2);
  color: var(--bms-color-text-secondary);
  font-size: 12px;
}
.admin-page__later p {
  margin: 0 0 var(--bms-space-1);
}
</style>
