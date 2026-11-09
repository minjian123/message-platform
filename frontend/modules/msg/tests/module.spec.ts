// kiwi_id: 2454
/** msg 模块定义：清单身份 / 区域注册 / 文案包。 */

import { MODULE_CONTRACT_VERSION } from '@bms/core'
import { describe, expect, it } from 'vitest'

import msgModule from '../src'

describe('msg 模块定义（Kiwi 2454）', () => {
  it('清单身份与契约版本', () => {
    expect(msgModule.manifest.name).toBe('msg')
    expect(msgModule.manifest.version).toBe(__BMS_MODULE_VERSION__)
    expect(msgModule.manifest.contractVersion).toBe(MODULE_CONTRACT_VERSION)
  })

  it('登记十条管理模块路由、一张概览卡片与租户记录页「消息配额」只读页签、文案包', async () => {
    const registration = await msgModule.setup({} as never)
    expect(registration.routes ?? []).toHaveLength(10)
    expect((registration.routes ?? []).filter((route) => route.meta?.menu !== false)).toHaveLength(8)
    expect((registration.cards ?? []) as unknown[]).toHaveLength(1)
    const regions = registration.regions ?? []
    expect(regions).toHaveLength(1)
    expect(regions[0]?.key).toBe('msg:tenant-quota')
    expect(regions[0]?.area).toBe('sys.record.detail.tabs')
    expect(regions[0]?.order).toBe(40)
    expect(regions[0]?.title).toBe('消息配额')
    expect(regions[0]?.perm).toBe('msg:query')
    expect((registration.i18nPacks ?? []).map((pack) => pack.key)).toEqual(['msg:zh-cn', 'msg:en'])
  })
})
