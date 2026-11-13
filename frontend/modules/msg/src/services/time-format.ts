/**
 * 时间口径纯函数：本地时间 ↔ UTC（06-2-3）。
 *
 * 契约口径：查询参数一律「UTC ISO 串带 `Z`」，展示一律转本地。
 * 页面不再各自 `replace('T', ' ')`，也不再按无时区串上送。
 */

import { message } from '../i18n/messages'
import { MS_PER_DAY, MS_PER_HOUR, MS_PER_MINUTE } from '../constants'

/** 本地时间展示格式（`sv-SE` 天然给出 `YYYY-MM-DD HH:mm:ss`）。 */
const LOCAL_FORMATTER = new Intl.DateTimeFormat('sv-SE', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
})

/**
 * 选择器给出的本地时间 → 上送用的 UTC ISO 串（带 `Z`）。
 *
 * 空串 / `null` / 非法值一律返回 `null`（调用方负责提示「时间格式不正确」）。
 *
 * @param local 本地时间（`YYYY-MM-DDTHH:mm:ss` 或已带时区的 ISO 串）。
 * @returns UTC ISO 串；无法解析为 `null`。
 */
export function toUtcParam(local: string | null | undefined): string | null {
  const trimmed = (local ?? '').trim()
  if (trimmed === '') {
    return null
  }
  const normalized = trimmed.includes(' ') ? trimmed.replace(' ', 'T') : trimmed
  const date = new Date(normalized)
  if (Number.isNaN(date.getTime())) {
    return null
  }
  return date.toISOString()
}

/**
 * 后端 UTC 串 → 本地展示文本（`YYYY-MM-DD HH:mm:ss`）。
 *
 * @param utc UTC ISO 串（`null` / 空串 / 非法值显示占位符）。
 * @returns 本地时间文本。
 */
export function toLocalText(utc: string | null | undefined): string {
  const trimmed = (utc ?? '').trim()
  if (trimmed === '') {
    return message('msg.admin.common.placeholder')
  }
  const date = new Date(trimmed)
  if (Number.isNaN(date.getTime())) {
    return message('msg.admin.common.placeholder')
  }
  return LOCAL_FORMATTER.format(date)
}

/**
 * 毫秒 → 可读时长（取最大可读单位，向下取整）。
 *
 * @param ms 毫秒（`null` / 负数 / 非有限值显示占位符）。
 * @returns 可读时长，如 `30 天` / `12 小时`。
 */
export function durationText(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) {
    return message('msg.admin.common.placeholder')
  }
  if (ms >= MS_PER_DAY) {
    return message('msg.admin.common.durationDays').replace('{n}', String(Math.floor(ms / MS_PER_DAY)))
  }
  if (ms >= MS_PER_HOUR) {
    return message('msg.admin.common.durationHours').replace('{n}', String(Math.floor(ms / MS_PER_HOUR)))
  }
  if (ms >= MS_PER_MINUTE) {
    return message('msg.admin.common.durationMinutes').replace('{n}', String(Math.floor(ms / MS_PER_MINUTE)))
  }
  return message('msg.admin.common.durationSeconds').replace('{n}', String(Math.floor(ms / 1000)))
}

/**
 * 本地时间串是否可解析（供表单就地校验）。
 *
 * @param local 本地时间串。
 * @returns 是否可解析。
 */
export function isValidLocal(local: string | null | undefined): boolean {
  const trimmed = (local ?? '').trim()
  if (trimmed === '') {
    return true
  }
  return toUtcParam(trimmed) !== null
}
