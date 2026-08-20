# Design System

<!-- impeccable:design-schema 1 -->

> 由 2026-08-20 视觉重设计建立。深色优先的现代专业工具风格。

## World

**精密数据库工具**（参考 Linear / TablePlus / Sentry 的克制优雅）。面向 DBA 长时间使用的恢复控制台：深色背景降低视觉疲劳，语义色传达操作状态，等宽字体呈现技术数据。视觉语言服务于高风险恢复操作的可读性与可信任感，不追求装饰性表达。

## Color

色彩策略：**Restrained** —— 中性深蓝灰底 + 单一蓝色主色 + 语义状态色。

### 深色主题（默认，`:root`）

| Token | 值 | 用途 |
|-------|-----|------|
| `--background` | `oklch(0.135 0.008 265)` | 页面背景 |
| `--card` | `oklch(0.175 0.01 265)` | 卡片表面 |
| `--foreground` | `oklch(0.92 0.005 80)` | 主文字（暖白） |
| `--muted-foreground` | `oklch(0.55 0.01 265)` | 辅助文字 |
| `--primary` | `oklch(0.55 0.18 265)` | 主色（蓝） |
| `--border` | `oklch(0.26 0.015 265)` | 边框 |

### 浅色主题（`.light`）

暖白背景 `oklch(0.965 0.005 80)`，纯白卡片，主色加深至 `oklch(0.45 0.18 265)` 保持对比度。

### 语义色（两主题共用）

| Token | 值 | 用途 |
|-------|-----|------|
| `--success` | `oklch(0.62 0.18 150)` | online / done |
| `--warning` | `oklch(0.68 0.16 85)` | paused / blocked / 待审批 |
| `--destructive` | `oklch(0.55 0.22 25)` | failed / error |
| `--info` | `oklch(0.65 0.15 220)` | scanning / executing |

状态徽章统一使用 `.status-badge` + `.status-dot`（圆点 + 文字），活跃状态圆点带 `pulse` 动画。

## Typography

- **字体**：Inter Variable（UI）+ 系统等宽（SQL/ID/GTID/时间戳）
- **层级**：页面标题 20px/600、卡片标题 16px/600、正文 14px/400、辅助 13px、徽章 12px/500、技术数据等宽 12px
- 数字列使用 `tabular-nums` 对齐

## Layout

- **侧边栏**：`w-52` 深色，活跃项左侧 2px 蓝色指示条（`.sidebar-active-indicator`）
- **内容区**：`p-6`，页面标题 + 副标题 + 内容卡片
- **卡片**：`--card` 底色 + 1px 边框，圆角 `--radius: 0.5rem`，hover 时 `card-hover` 微浮
- **表格**：sticky 表头，行 hover `bg-muted/40`，行间 `border-border/50`

## Motion

全部动画使用 `opacity`/`transform`（GPU 加速），`prefers-reduced-motion: reduce` 时禁用。

| 场景 | 动画 |
|------|------|
| 登录/注册背景 | 网格淡入 + 两个光晕 `float` 漂移（20s 周期） |
| 登录卡片入场 | `fade-slide-up` 500ms ease-out，子元素 200ms 级联延迟 |
| 页面切换 | 内容区 `animate-fade-in` 300ms |
| 向导步骤切换 | 内容 `opacity + translateY` 300ms 过渡 |
| 状态徽章（活跃） | 圆点 `pulse-dot` 2s 呼吸 |
| 执行进度条 | `progress-stripe` 条纹流动（执行中），暂停转琥珀色 |
| 加载状态 | `.skeleton` shimmer 骨架屏 |
| 终态横幅 | `scale-in` 300ms 弹出 |
| 表格行/按钮/导航 | `transition-colors duration-150` |

## Components

- **status-badge / status-dot**：全局状态徽章系统（`app.css`）
- **skeleton**：骨架屏（`app.css`）
- **card-hover**：卡片悬浮微动效
- **login-grid-bg**：登录页网格背景
- shadcn-svelte 组件库配合语义 token 使用

## Accessibility

- 辅助文字对比度 ≥4.5:1（浅色主题 `--muted-foreground` 从 4.1:1 提升至约 4.7:1）
- 状态徽章圆点 + 文字双通道，不依赖纯色区分
- `accent-primary` 复选框主色统一
- `prefers-reduced-motion` 全局动画禁用
