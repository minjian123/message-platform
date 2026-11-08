import { fileURLToPath, URL } from 'node:url'

import { federation } from '@module-federation/vite'
import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vite'

import {
  createModuleMetaPlugin,
  loadModuleContractVersion,
  loadModulePackage,
  moduleVersionDefine,
} from '../../../../bms/frontend/scripts/module-meta.mjs'
import { loadSharedDependencies } from '../../../../bms/frontend/scripts/shared-deps.mjs'

/** 模块名（= 模块清单 `name` = MF 容器名；带产品前缀，防与平台模块重名；登记型标识一律 snake_case）。 */
const MODULE_NAME = 'msg'

/** 远端容器入口文件名（契约常量 `MODULE_REMOTE_ENTRY_FILE` 同值，构建期须一致）。 */
const REMOTE_ENTRY_FILE = 'remoteEntry.js'

/** 暴露键（契约常量 `MODULE_EXPOSE_KEY` 同值；模块定义默认导出）。 */
const EXPOSE_KEY = './module'

/** 固定独立预览端口（与平台仓各模块端口区分；平台发布存储仍为 5002）。 */
const PORT = 5005

/** 模块工程目录（版本单一来源：`package.json`）。 */
const MODULE_DIR = fileURLToPath(new URL('.', import.meta.url))

/**
 * 平台前端目录（**工作区 bms 克隆**；本模块的基座包与单一来源文件都在这里）。
 *
 * 构建前提：`mdm/` 与 `bms/` 同级克隆（见任务 01_04 详细设计 §3.2）。
 * 显式传入该根，避免平台脚本在「非 `file:` 协议载入」时按 `process.cwd()` 向上回退而找不到
 * 单一来源文件（跨仓场景下回退路径不存在）。
 */
const BMS_FRONTEND_DIR = fileURLToPath(new URL('../../../../bms/frontend/', import.meta.url))

/** 模块版本（构建期注入模块定义，定义不再手写版本）。 */
const MODULE_VERSION = loadModulePackage(MODULE_DIR).version

/** 平台模块契约版本（单一来源：`<bms>/frontend/module-contract.json`）。 */
const CONTRACT_VERSION = loadModuleContractVersion(BMS_FRONTEND_DIR)

/**
 * Module Federation 共享依赖（**单例 + 版本要求**）：与平台同源——共享面与 `requiredVersion`
 * 取自平台单一来源 `<bms>/frontend/shared-dependencies.json`（不复制、不各写一份）。
 *
 * 模块为**消费方**（`role: 'remote'`）：每项带 `import: false`（**不打包本地回退副本**）
 * 与 `strictVersion: true`（版本不满足即拒绝加载）——宁可加载失败，不要静默出现第二份实例。
 */
const { shared: SHARED_DEPENDENCIES } = loadSharedDependencies({ role: 'remote', root: BMS_FRONTEND_DIR })

// msg 消息平台运行时模块（MF remote）：独立构建、产物独立（dist/ 不并入宿主产物）；
// 平台宿主经模块清单在运行期注册并加载本容器暴露的模块定义。
//
// **两个构建目标**（`--mode standalone` 切换）：
//   - 缺省（`remote`）→ `dist/`：**远端产物**，只含 MF 容器与暴露的模块定义及其异步分包，
//     入口显式声明为 `src/index.ts`、**不带 HTML 壳**；
//   - `standalone` → `dist-standalone/`：**独立预览产物**，带 HTML 壳与本地框架副本，
//     供模块脱离宿主独立开发与预览（本目标下框架依赖本地提供，不走共享域）。
export default defineConfig(({ mode }) => {
  const isStandaloneBuild = mode === 'standalone'
  return {
    // 版本单一来源注入（模块定义经 `__BMS_MODULE_VERSION__` 取得 `package.json` 版本）
    define: moduleVersionDefine(MODULE_VERSION),
    plugins: [
      vue(),
      // 产物元数据（版本发现：发布与护栏据此校验「清单 = 产物 = 源码」）
      createModuleMetaPlugin({ name: MODULE_NAME, version: MODULE_VERSION, contractVersion: CONTRACT_VERSION }),
      // **独立预览目标不启用 Module Federation**：该目标下框架依赖本地提供、不走共享域
      // （`shared` 的 `import: false` 在无宿主时无从解析，会致运行期缺实现——2026-10-08 实测白屏后修正）。
      ...(isStandaloneBuild
        ? []
        : [
            federation({
              name: MODULE_NAME,
              filename: REMOTE_ENTRY_FILE,
              exposes: { [EXPOSE_KEY]: './src/index.ts' },
              shared: SHARED_DEPENDENCIES,
            }),
          ]),
    ],
    build: {
      // MF 依赖顶层 await 与动态导入；无历史浏览器包袱，取 esnext
      target: 'esnext',
      outDir: isStandaloneBuild ? 'dist-standalone' : 'dist',
      // sourcemap：外部 `.map`（不内联源码），随发布按版本归档；线上异常可映射到源码位置
      sourcemap: true,
      rollupOptions: {
        // 远端产物显式声明入口（不带 HTML 壳）；独立预览目标沿用 HTML 入口
        ...(isStandaloneBuild ? {} : { input: { module: fileURLToPath(new URL('./src/index.ts', import.meta.url)) } }),
        output: {
          // 第三方 UI 库独立分包：`element-plus` 自带全局副作用（popper / message / focus-trap 触
          // `document.body`、`window.addEventListener`、原型探测），若并入页面块会被隔离扫描（P4）
          // 误判为「模块自有块全局污染」；独立为 vendor 块后页面块只含模块自有代码（与平台同一分包口径）。
          codeSplitting: {
            groups: [{ name: 'vendor-element-plus', test: /node_modules[\\/](element-plus|@element-plus)/ }],
          },
        },
      },
    },
    server: {
      port: PORT,
      strictPort: true,
      // 跨端口加载：放开 CORS 并声明自身 origin（remoteEntry 内部资源 URL 依据）
      cors: true,
      origin: `http://localhost:${String(PORT)}`,
    },
    preview: {
      port: PORT,
      strictPort: true,
      cors: true,
    },
  }
})
