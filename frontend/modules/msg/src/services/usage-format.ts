/**
 * 用量数值格式化（06-8）：字节 1024 进制单位换算。
 *
 * 单位口径全仓只此一份——配额页签与用量页签 / 独立页共用，避免同一数值在两处显示不同单位。
 * 只做**展示换算**，不做统计口径计算（用量一律由后端给定，前端不重算）。
 *
 * 计数类（主题数 / 分区数 / 今日投递量）不在此换算：交给平台指标卡按 `number` 口径走
 * `Intl` 千分位（见 `MetricCard`），避免「格式化后的字符串被指标卡判为空值」。
 */

import { message } from '../i18n/messages'

/** 不可用文案键（与配额页签同字面，键各自独立）。 */
export const UNAVAILABLE_KEY = 'msg.quota.state.unavailable'

/** 需要按字节展示的列键（与端点 `columns[].key` 同名）。 */
export const STORAGE_BYTES_COLUMN = 'storage_bytes'

/** 字节单位（1024 进制，封顶 TiB）。 */
const BYTE_UNITS: readonly string[] = ['B', 'KiB', 'MiB', 'GiB', 'TiB']

/** 字节拆分结果（数值 + 单位，供指标卡分别渲染）。 */
export interface ByteParts {
  /** 换算后的数值。 */
  amount: number
  /** 单位（`B` / `KiB` / `MiB` / `GiB` / `TiB`）。 */
  unit: string
}

/**
 * 拆分字节数为数值 + 单位（1024 进制，封顶 TiB；整数不带小数，非整数保留一位）。
 *
 * 取整在此完成而非渲染层：指标卡按 `number` 口径输出数值时会自行决定小数位，
 * 若把原始浮点（如 `8.7412…`）传进去会显示三位小数，与单位口径不一致。
 *
 * @param value 字节数（非负数）。
 * @returns 数值与单位。
 */
export function splitBytes(value: number): ByteParts {
  let amount = value
  let index = 0
  while (amount >= 1024 && index < BYTE_UNITS.length - 1) {
    amount /= 1024
    index += 1
  }
  return {
    amount: Number.isInteger(amount) ? amount : Number(amount.toFixed(1)),
    unit: BYTE_UNITS[index],
  }
}

/**
 * 字节格式化（1024 进制；整数不带小数，非整数保留一位）。
 *
 * @param value 字节数（`null` / 非有限数显示「暂不可用」）。
 * @returns 展示文本，如 `3.4 GiB`。
 */
export function formatBytes(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return message(UNAVAILABLE_KEY)
  }
  const parts = splitBytes(value)
  return `${parts.amount} ${parts.unit}`
}
