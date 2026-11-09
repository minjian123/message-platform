<script setup lang="ts">
/**
 * 只读列表骨架：标题 / 筛选插槽 / 分页 / 加载·空·错误·无权限·待选条件五态 / 返回深链。
 *
 * 状态与判定来自 `useMsgReadOnlyView()` 投影，本组件只做渲染，不实现状态机。
 */
import { ElAlert, ElButton, ElEmpty, ElPagination, ElSkeleton } from 'element-plus'
import { computed } from 'vue'

import type { ReadOnlyViewState } from '../base/BaseMsgReadOnlyView'
import { message } from '../i18n/messages'

interface Props {
  /** 页面标题。 */
  title: string
  /** 展示态。 */
  state: ReadOnlyViewState
  /** 当前展示态文案键（错误态为 `error.<码>`）。 */
  textKey?: string
  /** 当前页。 */
  page?: number
  /** 每页条数。 */
  pageSize?: number
  /** 过滤后总数。 */
  total?: number
  /** 是否显示返回按钮（详情页）。 */
  back?: boolean
  /** 首次加载是否显示骨架（轮询刷新为 false，保留已有数值）。 */
  skeleton?: boolean
  /** 是否显示分页（详情页 / 无分页端点为 false）。 */
  paginated?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  textKey: '',
  page: 1,
  pageSize: 20,
  total: 0,
  back: false,
  skeleton: true,
  paginated: true,
})

const emit = defineEmits<{
  refresh: []
  'update:page': [page: number]
  back: []
}>()

/** 当前态文案（键化，未命中由 `message` 原样返回）。 */
const stateText = computed(() => (props.textKey === '' ? message('state.empty') : message(props.textKey)))
/** 重新加载按钮文案。 */
const refreshText = computed(() => message('msg.admin.common.refresh'))
/** 返回按钮文案。 */
const backText = computed(() => message('msg.admin.common.back'))
</script>

<template>
  <section class="readonly-list" data-test="readonly-list" :data-state="state">
    <header class="readonly-list__head">
      <h2>{{ title }}</h2>
      <div class="readonly-list__actions">
        <el-button v-if="back" size="small" data-test="readonly-back" @click="emit('back')">{{ backText }}</el-button>
        <el-button size="small" data-test="readonly-refresh" @click="emit('refresh')">{{ refreshText }}</el-button>
      </div>
    </header>
    <slot name="filters" />
    <el-alert
      v-if="state === 'denied'"
      type="warning"
      :closable="false"
      show-icon
      :title="stateText"
      data-test="readonly-denied"
    />
    <el-alert
      v-else-if="state === 'choose'"
      type="info"
      :closable="false"
      show-icon
      :title="stateText"
      data-test="readonly-choose"
    />
    <el-alert
      v-else-if="state === 'error'"
      type="error"
      :closable="false"
      show-icon
      :title="stateText"
      data-test="readonly-error"
    />
    <el-skeleton
      v-else-if="state === 'loading' && skeleton"
      :rows="4"
      animated
      data-test="readonly-loading"
    />
    <el-empty
      v-else-if="state === 'empty'"
      :description="stateText"
      data-test="readonly-empty"
    />
    <template v-else>
      <div data-test="readonly-content"><slot /></div>
      <el-pagination
        v-if="paginated && state === 'ready'"
        :current-page="page"
        :page-size="pageSize"
        :total="total"
        layout="total, prev, pager, next"
        data-test="readonly-pagination"
        @current-change="emit('update:page', $event)"
      />
    </template>
  </section>
</template>

<style scoped>
.readonly-list {
  display: flex;
  flex-direction: column;
  gap: var(--bms-space-2);
}
.readonly-list__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--bms-space-2);
}
.readonly-list__head h2 {
  margin: 0;
  font-size: 16px;
}
.readonly-list__actions {
  display: flex;
  gap: var(--bms-space-1);
}
</style>
