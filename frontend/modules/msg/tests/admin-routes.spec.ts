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
  '/msg/partitions',
  '/msg/messages',
  '/msg/delayed',
  '/msg/quotas',
  '/msg/dlq',
]

describe('msg 模块注册（Kiwi 2464）', () => {
  it('清单身份与契约版本', () => {
    expect(msgModule.manifest.name).toBe('msg')
    expect(msgModule.manifest.version).toBe(__BMS_MODULE_VERSION__)
    expect(msgModule.manifest.contractVersion).toBe(MODULE_CONTRACT_VERSION)
  })

  it('十条路由：八项菜单 + 两条详情深链', async () => {
    const result = await registration()
    const routes = result.routes ?? []
    expect(routes).toHaveLength(10)
    expect(routes.map((route) => route.path)).toEqual([
      '/msg/overview',
      '/msg/topics',
      '/msg/topics/:topicId',
      '/msg/groups',
      '/msg/groups/:namespace/:topic/:group',
      '/msg/partitions',
      '/msg/messages',
      '/msg/delayed',
      '/msg/quotas',
      '/msg/dlq',
    ])
    const menus = routes.filter((route) => route.meta?.menu !== false)
    expect(menus.map((route) => route.path)).toEqual(MENU_PATHS)
    const details = routes.filter((route) => route.meta?.menu === false)
    expect(details).toHaveLength(2)
    expect(details.every((route) => route.meta?.title === undefined)).toBe(true)
    expect(routes.every((route) => typeof route.component === 'function')).toBe(true)
  })

  it('菜单 meta：分组 / 权限 / 次序，且给 06-7、06-8 留位', async () => {
    const result = await registration()
    const menus = (result.routes ?? []).filter((route) => route.meta?.menu !== false)
    expect(menus.every((route) => route.meta?.group === MSG_MENU_GROUP)).toBe(true)
    expect(menus.every((route) => route.meta?.perm === MSG_QUERY_PERMISSION)).toBe(true)
    expect(menus.map((route) => route.meta?.order)).toEqual([10, 20, 30, 40, 50, 60, 65, 70])
    expect(ORDER_RESERVED_TOPIC_ACL).toBeGreaterThan(70)
    expect(ORDER_RESERVED_TENANT_USAGE).toBeGreaterThan(ORDER_RESERVED_TOPIC_ACL)
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
    const region = result.regions?.[0]
    const regionModule = await (region?.component as () => Promise<unknown>)()
    expect(regionModule).toBeTruthy()
  })

  it('既有租户配额页签与两个 i18n 包保持不变', async () => {
    const result = await registration()
    expect(result.regions).toHaveLength(1)
    expect(result.regions?.[0]?.key).toBe('msg:tenant-quota')
    expect(result.regions?.[0]?.perm).toBe('msg:query')
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
