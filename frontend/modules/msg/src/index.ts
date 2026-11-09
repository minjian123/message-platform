/* eslint-disable @typescript-eslint/triple-slash-reference -- MF 类型声明生成以 plain tsc 编译，需引用 env.d.ts */
/// <reference path="./env.d.ts" />
/**
 * msg 消息平台运行时模块定义（Module Federation remote 入口：默认导出模块定义）。
 *
 * 注册扩展点：`routes`（管理模块十二条路由，含两条详情深链）、`cards`（消息平台概览）、
 * `regions`（租户记录页「消息配额」「消息用量」两条只读页签）与 `i18nPacks`（中英两包）。
 * 只声明注册项，不写注册表、不替换宿主路由、不自建宿主级实例。
 */

import { MODULE_CONTRACT_VERSION, WorkbenchCardProvider, defineModule } from '@bms/core'

import { MSG_MENU_GROUP, ORDER_RESERVED_TENANT_USAGE, ORDER_RESERVED_TOPIC_ACL } from './constants'
import { applyHostContext, MSG_MANAGE_PERMISSION, MSG_QUERY_PERMISSION } from './runtime'

/** 菜单项公共元信息（分组与权限由宿主读取）。 */
function menu(title: string, order: number): Record<string, unknown> {
  return {
    title,
    group: MSG_MENU_GROUP,
    perm: MSG_QUERY_PERMISSION,
    order,
  }
}

/** msg 消息平台模块（清单 `name` / `version` 须与平台清单条目严格一致）。 */
export const msgModule = defineModule({
  manifest: { name: 'msg', version: __BMS_MODULE_VERSION__, contractVersion: MODULE_CONTRACT_VERSION },
  setup: async (context) => {
    applyHostContext(context)
    const { MSG_MESSAGES_EN, MSG_MESSAGES_ZH_CN } = await import('./i18n/messages')
    return {
      routes: [
        {
          path: '/msg/overview',
          component: () => import('./views/OverviewPage.vue'),
          meta: menu('平台概览', 10),
        },
        {
          path: '/msg/topics',
          component: () => import('./views/TopicsPage.vue'),
          meta: menu('主题管理', 20),
        },
        {
          path: '/msg/topics/:topicId',
          component: () => import('./views/TopicDetailPage.vue'),
          meta: { menu: false, perm: MSG_QUERY_PERMISSION },
        },
        {
          path: '/msg/groups',
          component: () => import('./views/GroupsPage.vue'),
          meta: menu('消费组与积压', 30),
        },
        {
          path: '/msg/groups/:namespace/:topic/:group',
          component: () => import('./views/GroupDetailPage.vue'),
          meta: { menu: false, perm: MSG_QUERY_PERMISSION },
        },
        {
          path: '/msg/partitions',
          component: () => import('./views/PartitionsPage.vue'),
          meta: menu('分区', 40),
        },
        {
          path: '/msg/messages',
          component: () => import('./views/MessagesPage.vue'),
          meta: menu('消息查询', 50),
        },
        {
          path: '/msg/delayed',
          component: () => import('./views/DelayedPage.vue'),
          meta: menu('延迟消息', 60),
        },
        {
          path: '/msg/quotas',
          component: () => import('./views/QuotasPage.vue'),
          meta: menu('配额管理', 65),
        },
        {
          path: '/msg/dlq',
          component: () => import('./views/DlqPage.vue'),
          meta: menu('死信与重放', 70),
        },
        {
          path: '/msg/acls',
          name: 'msg.acl',
          component: () => import('./views/AclPage.vue'),
          meta: {
            title: '主题授权',
            group: MSG_MENU_GROUP,
            icon: 'el:Key',
            perm: MSG_MANAGE_PERMISSION,
            order: ORDER_RESERVED_TOPIC_ACL,
          },
        },
        {
          path: '/msg/usage',
          name: 'msg.usage',
          component: () => import('./views/UsagePage.vue'),
          meta: {
            title: '租户用量',
            group: MSG_MENU_GROUP,
            icon: 'el:DataAnalysis',
            perm: MSG_QUERY_PERMISSION,
            order: ORDER_RESERVED_TENANT_USAGE,
          },
        },
      ],
      cards: [
        new WorkbenchCardProvider(
          'msg:overview-card',
          () => import('./components/OverviewCard.vue'),
          '消息平台概览',
        ),
      ],
      regions: [
        {
          key: 'msg:tenant-quota',
          area: 'sys.record.detail.tabs',
          component: () => import('./components/QuotaPanel.vue'),
          order: 40,
          title: '消息配额',
          perm: MSG_QUERY_PERMISSION,
        },
        {
          key: 'msg:tenant-usage',
          area: 'sys.record.detail.tabs',
          component: () => import('./components/UsagePanel.vue'),
          order: 41,
          title: '消息用量',
          perm: MSG_QUERY_PERMISSION,
        },
      ],
      i18nPacks: [
        { key: 'msg:zh-cn', messages: MSG_MESSAGES_ZH_CN },
        { key: 'msg:en', messages: MSG_MESSAGES_EN },
      ],
    }
  },
})

export default msgModule
