<script setup lang="ts">
/**
 * 连接列表（`/msg/connections`）：单实例视图，数据来自 `GET /admin/connections`（06-1-4）。
 *
 * 地址已在后端脱敏；页面只做展示与分页，不提供断开 / 摘除操作（跨实例聚合与运维动作归 02-8-4）。
 */

import { ElTable, ElTableColumn, ElTag } from 'element-plus'

import ReadOnlyList from '../components/ReadOnlyList.vue'
import { useMsgReadOnlyView } from '../composables/useMsgReadOnlyView'
import { message } from '../i18n/messages'
import { loadConnections, type ConnectionItem } from '../services/admin-service'
import { toLocalText } from '../services/time-format'

const view = useMsgReadOnlyView<ConnectionItem>({ loader: loadConnections })

/** 状态 → 标签色。 */
const STATE_TAG: Record<string, 'success' | 'warning' | 'danger' | 'info'> = {
  active: 'success',
  idle: 'info',
  draining: 'warning',
  stale: 'danger',
}

/**
 * 状态文案（未知枚举原样返回）。
 *
 * @param state 状态枚举。
 * @returns 文案。
 */
function stateText(state: string): string {
  return message(`msg.admin.connections.state.${state}`)
}

/**
 * 状态标签类型。
 *
 * @param state 状态枚举。
 * @returns 标签类型。
 */
function stateTag(state: string): 'success' | 'warning' | 'danger' | 'info' {
  return STATE_TAG[state] ?? 'info'
}
</script>

<template>
  <section class="admin-page" data-test="page-connections">
    <p class="admin-page__hint" data-test="connections-single-instance">
      {{ message('msg.admin.connections.singleInstance') }}
    </p>
    <read-only-list
      :title="message('msg.admin.connections.title')"
      :state="view.state.value"
      :text-key="view.textKey.value"
      :page="view.page.value"
      :page-size="view.pageSize.value"
      :total="view.total.value"
      :skeleton="view.firstLoad.value"
      @refresh="view.refresh"
      @update:page="view.setPage"
    >
      <el-table :data="view.rows.value" size="small" border data-test="connections-table">
        <el-table-column prop="connection_id" :label="message('msg.admin.connections.id')" min-width="140" />
        <el-table-column prop="remote_prefix" :label="message('msg.admin.connections.remote')" min-width="150" />
        <el-table-column prop="http_version" :label="message('msg.admin.connections.httpVersion')" min-width="110" />
        <el-table-column prop="scheme" :label="message('msg.admin.connections.scheme')" min-width="90" />
        <el-table-column :label="message('msg.admin.connections.openedAt')" min-width="170">
          <template #default="{ row }">{{ toLocalText((row as ConnectionItem).opened_at) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.connections.lastSeenAt')" min-width="170">
          <template #default="{ row }">{{ toLocalText((row as ConnectionItem).last_seen_at) }}</template>
        </el-table-column>
        <el-table-column :label="message('msg.admin.connections.lastHeartbeatAt')" min-width="170">
          <template #default="{ row }">{{ toLocalText((row as ConnectionItem).last_heartbeat_at) }}</template>
        </el-table-column>
        <el-table-column prop="streams" :label="message('msg.admin.connections.streams')" min-width="90" align="right" />
        <el-table-column prop="requests" :label="message('msg.admin.connections.requests')" min-width="90" align="right" />
        <el-table-column :label="message('msg.admin.connections.state')" min-width="100">
          <template #default="{ row }">
            <el-tag :type="stateTag((row as ConnectionItem).state)" size="small">
              {{ stateText((row as ConnectionItem).state) }}
            </el-tag>
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
</style>
