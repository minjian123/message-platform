// kiwi_id: 2466
/** `useMsgWriteAction` 投影：状态同步 / 成功 / 失败 / 无权限 / 取消订阅。 */

import { effectScope } from 'vue'
import { describe, expect, it, vi } from 'vitest'

const { permissionRef } = vi.hoisted(() => ({
  permissionRef: { value: undefined as readonly string[] | undefined },
}))

vi.mock('../src/runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/runtime')>()
  return {
    ...actual,
    hostPermissions: () => permissionRef.value,
  }
})

import { useMsgWriteAction } from '../src/composables/useMsgWriteAction'

describe('useMsgWriteAction（Kiwi 2466）', () => {
  it('成功：status done、result 可取、canSubmit 恢复', async () => {
    permissionRef.value = ['msg:manage']
    const scope = effectScope()
    const action = scope.run(() => useMsgWriteAction({ runner: async () => 'created' }))!
    expect(action.canSubmit.value).toBe(true)
    await action.run()
    expect(action.status.value).toBe('done')
    expect(action.result.value).toBe('created')
    expect(action.errorKey.value).toBe('')
    scope.stop()
  })

  it('失败：errorCode 与文案键同步，impactRows / requiresConfirm 暴露给页面', async () => {
    permissionRef.value = ['msg:manage']
    const action = useMsgWriteAction({
      runner: async () => {
        throw Object.assign(new Error('x'), { code: 350003 })
      },
      impactRows: [{ label: '分区', value: '0' }],
    })
    await action.run()
    expect(action.status.value).toBe('error')
    expect(action.errorCode.value).toBe(350003)
    expect(action.errorKey.value).toBe('error.350003')
    expect(action.requiresConfirm).toBe(true)
    expect(action.impactRows).toHaveLength(1)
    action.dispose()
  })

  it('无权限：blocked 置真、不发请求（runner 零调用）', async () => {
    permissionRef.value = ['msg:query']
    const runner = vi.fn(async () => 'x')
    const action = useMsgWriteAction({ runner })
    expect(action.canSubmit.value).toBe(false)
    await action.run()
    expect(runner).not.toHaveBeenCalled()
    expect(action.blocked.value).toBe(true)
    expect(action.errorCode.value).toBe(30001)
    action.dispose()
  })

  it('确认标记与重置：acknowledge 透传、reset 清空错误与结果态', async () => {
    permissionRef.value = ['msg:manage']
    const action = useMsgWriteAction({ runner: async () => 'ok' })
    action.acknowledge()
    expect(action.acknowledged.value).toBe(true)
    await action.run()
    action.reset()
    expect([action.acknowledged.value, action.errorCode.value, action.errorKey.value]).toEqual([false, undefined, ''])
    action.dispose()
  })

  it('dispose 幂等，且卸载作用域时自动取消订阅', async () => {
    permissionRef.value = ['msg:manage']
    const action = useMsgWriteAction({ runner: async () => 'ok' })
    action.dispose()
    action.dispose()
    const scope = effectScope()
    const scoped = scope.run(() => useMsgWriteAction({ runner: async () => 'ok' }))!
    scope.stop()
    await scoped.run()
    expect(scoped.status.value).toBe('done')
  })
})
