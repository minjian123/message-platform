/**
 * 重放任务的展示侧纯函数（分区文本解析 + 条件摘要）。
 *
 * 只做字符串解析与展示拼接：**不计算进度真值、不计算积压**（进度取 `replayed` / `total`，
 * 积压取 `affected_groups`，均由后端给出）。列表「条件」列与创建表单的「范围摘要」共用本模块，
 * 避免两处各写一套格式化。
 */

import { message } from '../i18n/messages'
import { isValidLocal, toLocalText, toUtcParam } from './time-format'

/** 条件摘要的输入（与建任务请求的字段同名同义）。 */
export interface ReplayScope {
  /** 分区号列表；`null` 表示全部分区。 */
  partitions: readonly number[] | null
  /** 起始 offset（含）；`null` 不限。 */
  offset_from: number | null
  /** 结束 offset（不含）；`null` 不限。 */
  offset_to: number | null
  /** 起始发布时间（含，UTC）；`null` 不限。 */
  time_from: string | null
  /** 结束发布时间（不含，UTC）；`null` 不限。 */
  time_to: string | null
  /** 分区键精确匹配；`null` 不过滤。 */
  key: string | null
}

/** 分区文本解析结果。 */
export interface PartitionParse {
  /** 是否可解析（`false` 表示含非数字片段）。 */
  ok: boolean
  /** 解析结果（去重升序）；空文本按全部分区记 `null`。 */
  value: number[] | null
}

/**
 * 解析分区文本（逗号 / 空格 / 中文逗号分隔，空文本 = 全部分区）。
 *
 * @param text 用户输入。
 * @returns 解析结果；含非数字片段时 `ok = false`。
 */
export function parsePartitionText(text: string): PartitionParse {
  const trimmed = text.trim()
  if (trimmed === '') {
    return { ok: true, value: null }
  }
  const values: number[] = []
  for (const token of trimmed.split(/[,，\s]+/)) {
    if (!/^\d+$/.test(token)) {
      return { ok: false, value: null }
    }
    const value = Number(token)
    if (!values.includes(value)) {
      values.push(value)
    }
  }
  values.sort((left, right) => left - right)
  return { ok: true, value: values }
}

/**
 * 把 UTC 时间串转成本地展示形态（空值记 `*`）。
 *
 * @param value UTC ISO 串。
 * @returns 展示文本。
 */
function formatTime(value: string | null): string {
  return value === null ? '*' : toLocalText(value)
}

/**
 * 条件摘要（列表「条件」列与创建表单「范围摘要」共用）。
 *
 * @param scope 条件字段。
 * @returns 摘要文本，如 `分区 0,1 · offset 100~200 · key=abc`。
 */
export function describeReplayScope(scope: ReplayScope): string {
  const parts: string[] = []
  if (scope.partitions === null) {
    parts.push(message('msg.admin.replay.allPartitions'))
  } else {
    parts.push(`${message('msg.admin.replay.partitions')} ${scope.partitions.join(',')}`)
  }
  if (scope.offset_from !== null || scope.offset_to !== null) {
    parts.push(`offset ${scope.offset_from ?? '*'}~${scope.offset_to ?? '*'}`)
  }
  if (scope.time_from !== null || scope.time_to !== null) {
    parts.push(`${formatTime(scope.time_from)} ~ ${formatTime(scope.time_to)}`)
  }
  if (scope.key !== null) {
    parts.push(`key=${scope.key}`)
  }
  return parts.join(' · ')
}

/** 创建表单的可编辑值（控件绑定形态，未做空值归一）。 */
export interface ReplayForm {
  /** 命名空间。 */
  namespace: string
  /** 主题名。 */
  topic: string
  /** 起始 offset（未填为 `undefined`）。 */
  offsetFrom: number | undefined
  /** 结束 offset（未填为 `undefined`）。 */
  offsetTo: number | undefined
  /** 起始发布时间（空串表示未填）。 */
  timeFrom: string
  /** 结束发布时间（空串表示未填）。 */
  timeTo: string
  /** 分区键。 */
  key: string
  /** 分区文本（逗号分隔，空串 = 全部分区）。 */
  partitions: string
  /** 速率上限（条/秒）。 */
  rate: number
}

/** 建任务请求体（前端字段归一后的形态，字段名与契约同名）。 */
export interface ReplayBody {
  /** 命名空间。 */
  namespace: string
  /** 主题名。 */
  topic: string
  /** 分区号列表；`null` 为全部分区。 */
  partitions: number[] | null
  /** 起始 offset（含）。 */
  offset_from: number | null
  /** 结束 offset（不含）。 */
  offset_to: number | null
  /** 起始发布时间（含，UTC）。 */
  time_from: string | null
  /** 结束发布时间（不含，UTC）。 */
  time_to: string | null
  /** 分区键精确匹配。 */
  key: string | null
  /** 速率上限（条/秒）。 */
  rate_per_second: number
}

/**
 * 表单校验（返回未通过的文案键，空串表示通过）。
 *
 * @param form 表单值。
 * @returns 文案键。
 */
export function validateReplayForm(form: ReplayForm): string {
  if (form.namespace.trim() === '' || form.topic.trim() === '') {
    return 'msg.admin.replay.validation'
  }
  if (!isValidLocal(form.timeFrom) || !isValidLocal(form.timeTo)) {
    return 'msg.admin.common.invalidTime'
  }
  if (!parsePartitionText(form.partitions).ok) {
    return 'msg.admin.replay.invalidPartitions'
  }
  if (form.offsetFrom !== undefined && form.offsetTo !== undefined && form.offsetFrom >= form.offsetTo) {
    return 'msg.admin.replay.invalidRange'
  }
  if (form.timeFrom !== '' && form.timeTo !== '' && form.timeFrom >= form.timeTo) {
    return 'msg.admin.replay.invalidRange'
  }
  return ''
}

/**
 * 表单 → 请求体（空值归一为 `null`，分区文本解析为数字数组）。
 *
 * @param form 表单值（调用前须已通过 `validateReplayForm`）。
 * @returns 请求体。
 */
export function toReplayBody(form: ReplayForm): ReplayBody {
  const parsed = parsePartitionText(form.partitions)
  return {
    namespace: form.namespace.trim(),
    topic: form.topic.trim(),
    partitions: parsed.value,
    offset_from: form.offsetFrom ?? null,
    offset_to: form.offsetTo ?? null,
    time_from: toUtcParam(form.timeFrom),
    time_to: toUtcParam(form.timeTo),
    key: form.key.trim() === '' ? null : form.key.trim(),
    rate_per_second: form.rate,
  }
}
