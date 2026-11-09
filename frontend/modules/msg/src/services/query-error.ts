/** 产品域本地契约错误（不含宿主依赖，供取数层与测试共用）。 */

/** 业务码错误（接口成功但 `data` 为空等）。 */
export class MsgQueryError extends Error {
  /** 业务码（空数据固定 10000）。 */
  readonly code: number

  /**
   * @param code 业务码。
   */
  constructor(code: number) {
    super(String(code))
    this.name = 'MsgQueryError'
    this.code = code
  }
}
