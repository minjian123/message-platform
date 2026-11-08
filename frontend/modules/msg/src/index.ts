/* eslint-disable @typescript-eslint/triple-slash-reference -- MF 类型声明生成以 plain tsc 编译，需引用 env.d.ts */
/// <reference path="./env.d.ts" />
/**
 * msg 消息平台运行时模块定义（Module Federation remote 入口：默认导出模块定义）。
 *
 * 只登记一类扩展点：`regions`（租户记录页「消息配额」只读页签）+ `i18nPacks`；
 * 无路由 / 组件 / 字段渲染器 / 图标 / 卡片 / 主题令牌。
 */

import { MODULE_CONTRACT_VERSION, defineModule } from '@bms/core'

import { MSG_MESSAGES_EN, MSG_MESSAGES_ZH_CN } from './i18n/messages'
import { applyHostContext, MSG_QUERY_PERMISSION } from './runtime'

/** msg 消息平台模块（清单 `name` / `version` 须与平台清单条目严格一致）。 */
export const msgModule = defineModule({
  manifest: { name: 'msg', version: __BMS_MODULE_VERSION__, contractVersion: MODULE_CONTRACT_VERSION },
  setup: (context) => {
    applyHostContext(context)
    return {
      regions: [
        {
          key: 'msg:tenant-quota',
          area: 'sys.record.detail.tabs',
          component: () => import('./components/QuotaPanel.vue'),
          order: 40,
          title: '消息配额',
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
