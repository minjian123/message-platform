import js from '@eslint/js'
import skipFormatting from '@vue/eslint-config-prettier/skip-formatting'
import tseslint from 'typescript-eslint'

/**
 * 工作区根 ESLint 口径（仅覆盖本目录直落文件：根级脚本等）。
 *
 * 各成员包（`modules/*` / `packages/*`）**各自持有** `eslint.config.js`
 * （锚定自身 `tsconfigRootDir`，见 bms 模块工程同口径），根配置不重复扫描成员源码。
 */
export default tseslint.config(
  {
    name: 'workspace/files-to-ignore',
    ignores: ['**/node_modules/**', '**/dist/**', '**/dist-standalone/**', '**/coverage/**', '**/.mf/**', 'modules/**', 'packages/**'],
  },
  { name: 'workspace/files-to-lint', files: ['**/*.{ts,mts,mjs}'] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    name: 'workspace/node-scripts',
    files: ['**/*.mjs'],
    languageOptions: { globals: { console: 'readonly', process: 'readonly', URL: 'readonly' } },
  },
  skipFormatting,
)
