/** 独立预览入口（无宿主：仅渲染插件件并显示降级提示）。 */

import { createApp, h } from 'vue'

import QuotaPanel from './components/QuotaPanel.vue'

createApp({
  render: () => h(QuotaPanel),
}).mount('#app')
