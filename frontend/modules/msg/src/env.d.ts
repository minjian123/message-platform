/**
 * 构建期注入的模块版本（`package.json` 版本单一来源；由平台 `module-meta.mjs` 的 `moduleVersionDefine` 注入）。
 */
declare const __BMS_MODULE_VERSION__: string

/**
 * 模块工程类型声明补充：`bpmn-moddle` 自带类型文件不含构造器声明，
 * 模块经 `@bms/ui_ep`（源码直出）间接引用其强比对工具，需补齐同一声明避免悬空类型
 * （同平台 `frontend/packages/ui-ep/src/env.d.ts` 与宿主 `src/env.d.ts`）。
 */

declare module 'bpmn-moddle' {
  /** BPMN 元素（仅声明实际读取的字段）。 */
  export interface BpmnModdleElement {
    /** 元素类型（含命名空间前缀）。 */
    $type: string
    /** 元素标识。 */
    id?: string
    /** 元素名称。 */
    name?: string
    /** 子流程元素（流程根下）。 */
    flowElements?: BpmnModdleElement[]
    /** 定义根元素（定义根下）。 */
    rootElements?: BpmnModdleElement[]
    /** 连线起点。 */
    sourceRef?: { id?: string }
    /** 连线终点。 */
    targetRef?: { id?: string }
  }

  /** 解析结果。 */
  export interface BpmnModdleParseResult {
    /** 定义根元素。 */
    rootElement: BpmnModdleElement
  }

  /** BPMN 模型构造器（`SimpleBpmnModdle` 的对外别名）。 */
  export class BpmnModdle {
    /**
     * 解析 BPMN XML。
     *
     * @param xml BPMN XML 文本。
     * @returns 解析结果。
     */
    fromXML(xml: string): Promise<BpmnModdleParseResult>
  }
}

/** `.vue` 单文件组件模块声明（Module Federation 类型声明生成以 plain `tsc` 编译，需显式 shim）。 */
declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<Record<string, unknown>, Record<string, unknown>, unknown>
  export default component
}
