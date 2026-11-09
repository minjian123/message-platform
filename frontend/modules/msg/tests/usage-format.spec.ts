// kiwi_id: 2471
/** 用量数值格式化：字节 1024 进制各级 / 封顶 TiB / 精度 / 不可用。 */

import { describe, expect, it } from 'vitest'

import { message } from '../src/i18n/messages'
import { formatBytes, splitBytes, STORAGE_BYTES_COLUMN, UNAVAILABLE_KEY } from '../src/services/usage-format'

describe('usage-format（Kiwi 2471）', () => {
  it('字节换算按 1024 进制逐级提升单位', () => {
    expect(splitBytes(512)).toEqual({ amount: 512, unit: 'B' })
    expect(splitBytes(1024)).toEqual({ amount: 1, unit: 'KiB' })
    expect(splitBytes(1024 * 1024)).toEqual({ amount: 1, unit: 'MiB' })
    expect(splitBytes(1024 * 1024 * 1024)).toEqual({ amount: 1, unit: 'GiB' })
    expect(splitBytes(1024 * 1024 * 1024 * 1024)).toEqual({ amount: 1, unit: 'TiB' })
  })

  it('超过 TiB 不再升级单位（封顶）', () => {
    expect(splitBytes(1024 ** 5)).toEqual({ amount: 1024, unit: 'TiB' })
  })

  it('整数不带小数，非整数保留一位', () => {
    expect(formatBytes(0)).toBe('0 B')
    expect(formatBytes(2048)).toBe('2 KiB')
    expect(formatBytes(1536)).toBe('1.5 KiB')
    expect(formatBytes(3650722201)).toBe('3.4 GiB')
  })

  it('空值与不可用：统一「暂不可用」文案，不写 0', () => {
    expect(formatBytes(null)).toBe(message(UNAVAILABLE_KEY))
    expect(formatBytes(Number.POSITIVE_INFINITY)).toBe(message(UNAVAILABLE_KEY))
    expect(UNAVAILABLE_KEY).toBe('msg.quota.state.unavailable')
    expect(STORAGE_BYTES_COLUMN).toBe('storage_bytes')
  })
})
