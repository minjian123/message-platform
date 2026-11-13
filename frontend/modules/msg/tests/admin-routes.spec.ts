// kiwi_id: 2464
/** 模块注册：九路由 / 七菜单项 / 工作台卡片 / 命名空间与权限 / i18n 双包 / 宿主上下文。 */

import { MODULE_CONTRACT_VERSION, WorkbenchCardProvider } from '@bms/core'
import { describe, expect, it } from 'vitest'

import msgModule from '../src'
import {
  GRAFANA_OVERVIEW_URL,
  MSG_MENU_GROUP,
  ORDER_RESERVED_TENANT_USAGE,
  ORDER_RESERVED_TOPIC_ACL,
  OVERVIEW_POLL_INTERVAL_MS,
} from '../src/constants'
import { MSG_MESSAGES_EN, MSG_MESSAGES_ZH_CN, message } from '../src/i18n/messages'
import {
  MODULE_API_ABSENT,
  MSG_QUERY_PERMISSION,
  applyHostContext,
  hostPermissions,
  isApiAbsent,
  requiredHostApi,
} from '../src/runtime'

/** 取注册声明（注入一份假的宿主请求能力）。 */
async function registration() {
  return msgModule.setup({ api: { product: () => ({ get: async () => ({ code: 0, data: null }) }) } } as never)
}

const MENU_PATHS = [
  '/msg/overview',
  '/msg/topics',
  '/msg/groups',
  '/msg/connections',
  '/msg/partitions',
  '/msg/messages',
  '/msg/delayed',
  '/msg/quotas',
  '/msg/dlq',
  '/msg/acls',
  '/msg/usage',
]

describe('msg 模块注册（Kiwi 2464）', () => {
  it('清单身份与契约版本', () => {
    expect(msgModule.manifest.name).toBe('msg')
    expect(msgModule.manifest.version).toBe(__BMS_MODULE_VERSION__)
    expect(msgModule.manifest.contractVersion).toBe(MODULE_CONTRACT_VERSION)
  })

  it('十四条路由：十一项菜单 + 三条详情深链', async () => {
    const result = await registration()
    const routes = result.routes ?? []
    expect(routes).toHaveLength(14)
    expect(routes.map((route) => route.path)).toEqual([
      '/msg/overview',
      '/msg/topics',
      '/msg/topics/:topicId',
      '/msg/groups',
      '/msg/connections',
      '/msg/groups/:namespace/:topic/:group',
      '/msg/partitions',
      '/msg/messages',
      '/msg/messages/:logId',
      '/msg/delayed',
      '/msg/quotas',
      '/msg/dlq',
      '/msg/acls',
      '/msg/usage',
    ])
    const menus = routes.filter((route) => route.meta?.menu !== false)
    expect(menus.map((route) => route.path)).toEqual(MENU_PATHS)
    const details = routes.filter((route) => route.meta?.menu === false)
    expect(details).toHaveLength(3)
    expect(details.every((route) => route.meta?.title === undefined)).toBe(true)
    expect(routes.every((route) => typeof route.component === 'function')).toBe(true)
  })

  it('菜单 meta：分组 / 权限 / 次序，主题授权用 msg:manage 与 el:Key，租户用量用 order 90', async () => {
    const result = await registration()
    const menus = (result.routes ?? []).filter((route) => route.meta?.menu !== false)
    expect(menus.every((route) => route.meta?.group === MSG_MENU_GROUP)).toBe(true)
    expect(menus.slice(0, 9).every((route) => route.meta?.perm === MSG_QUERY_PERMISSION)).toBe(true)
    expect(menus.map((route) => route.meta?.order)).toEqual([10, 20, 30, 35, 40, 50, 60, 65, 70, 80, 90])
    const acl = menus[9]
    expect(acl?.name).toBe('msg.acl')
    expect(acl?.meta?.title).toBe('主题授权')
    expect(acl?.meta?.perm).toBe('msg:manage')
    expect(acl?.meta?.icon).toBe('el:Key')
    expect(acl?.meta?.order).toBe(ORDER_RESERVED_TOPIC_ACL)
    expect(ORDER_RESERVED_TENANT_USAGE).toBeGreaterThan(ORDER_RESERVED_TOPIC_ACL)

    const usage = menus[10]
    expect(usage?.name).toBe('msg.usage')
    expect(usage?.path).toBe('/msg/usage')
    expect(usage?.meta?.title).toBe('租户用量')
    expect(usage?.meta?.perm).toBe(MSG_QUERY_PERMISSION)
    expect(usage?.meta?.icon).toBe('el:DataAnalysis')
    expect(usage?.meta?.order).toBe(ORDER_RESERVED_TENANT_USAGE)
  })

  it('工作台卡片：命名空间键、标题、异步加载器、装配器可识别的 provider', async () => {
    const result = await registration()
    const cards = (result.cards ?? []) as WorkbenchCardProvider[]
    expect(cards).toHaveLength(1)
    expect(cards[0]).toBeInstanceOf(WorkbenchCardProvider)
    expect(cards[0]?.key).toBe('msg:overview-card')
    expect(cards[0]?.title).toBe('消息平台概览')
    expect(typeof cards[0]?.component).toBe('function')
  })

  it('全部注册项的异步加载器可解析（纯声明护栏用例）', async () => {
    const result = await registration()
    for (const route of result.routes ?? []) {
      const loaded = await route.component()
      expect(loaded).toBeTruthy()
    }
    const card = (result.cards ?? [])[0] as WorkbenchCardProvider
    const cardModule = await (card.component as () => Promise<unknown>)()
    expect(cardModule).toBeTruthy()
    for (const region of result.regions ?? []) {
      const regionModule = await (region.component as () => Promise<unknown>)()
      expect(regionModule).toBeTruthy()
    }
  })

  it('两条租户记录页签：配额 40 在前、用量 41 在后，同区域同权限', async () => {
    const result = await registration()
    const regions = result.regions ?? []
    expect(regions).toHaveLength(2)
    expect(regions.map((region) => region.key)).toEqual(['msg:tenant-quota', 'msg:tenant-usage'])
    expect(regions.every((region) => region.area === 'sys.record.detail.tabs')).toBe(true)
    expect(regions.every((region) => region.perm === 'msg:query')).toBe(true)
    expect(regions.map((region) => region.order)).toEqual([40, 41])
    expect(regions.map((region) => region.title)).toEqual(['消息配额', '消息用量'])
    expect((result.i18nPacks ?? []).map((pack) => pack.key)).toEqual(['msg:zh-cn', 'msg:en'])
  })
})

describe('模块常量与文案（Kiwi 2464）', () => {
  it('Grafana 地址缺省为空（缺配置不渲染），轮询 30 秒', () => {
    expect(GRAFANA_OVERVIEW_URL).toBe('')
    expect(OVERVIEW_POLL_INTERVAL_MS).toBe(30_000)
    expect(MSG_MENU_GROUP).toBe('消息平台')
  })

  it('中英两包键集合一致，错误码 / 四态 / hint 键齐备', () => {
    expect(Object.keys(MSG_MESSAGES_ZH_CN).sort()).toEqual(Object.keys(MSG_MESSAGES_EN).sort())
    for (const code of [350001, 350009, 350010, 350011, 350012, 350013, 350027, 10001, 30001]) {
      expect(MSG_MESSAGES_ZH_CN[`error.${String(code)}`]).toBeTruthy()
      expect(MSG_MESSAGES_EN[`error.${String(code)}`]).toBeTruthy()
    }
    for (const key of ['error.fallback', 'state.loading', 'state.empty', 'state.error', 'state.denied', 'state.choose', 'state.collecting', 'hint.normal', 'hint.stale', 'hint.near_quota']) {
      expect(MSG_MESSAGES_ZH_CN[key]).toBeTruthy()
    }
    expect(message('error.350001')).toBe('主题不存在或未启用')
    expect(message('error.350001', 'en')).toBe('Topic not found or disabled')
    expect(message('not.registered')).toBe('not.registered')
  })
})

describe('宿主上下文（Kiwi 2464）', () => {
  it('用户权限快照：codes / permissions / 非数组 / 非对象 / 非字符串过滤', () => {
    applyHostContext({ user: { codes: ['msg:query', 7, 'msg:manage'] } } as never)
    expect(hostPermissions()).toEqual(['msg:query', 'msg:manage'])
    applyHostContext({ user: { permissions: ['msg:manage'] } } as never)
    expect(hostPermissions()).toEqual(['msg:manage'])
    applyHostContext({ user: { codes: 'invalid' } } as never)
    expect(hostPermissions()).toBeUndefined()
    applyHostContext({ user: null } as never)
    expect(hostPermissions()).toBeUndefined()
    applyHostContext({ user: 'anonymous' } as never)
    expect(hostPermissions()).toBeUndefined()
  })

  it('请求能力缺失抛哨兵；注入后可用；哨兵判定覆盖两类异常', () => {
    applyHostContext({} as never)
    expect(() => requiredHostApi()).toThrow(MODULE_API_ABSENT)
    try {
      requiredHostApi()
    } catch (error) {
      expect(isApiAbsent(error)).toBe(true)
    }
    expect(isApiAbsent(new Error('other'))).toBe(false)
    expect(isApiAbsent('boom')).toBe(false)
    applyHostContext({ api: { product: () => ({}) } } as never)
    expect(requiredHostApi()).toBeTruthy()
  })
})
