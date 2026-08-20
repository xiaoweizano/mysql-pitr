# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

**DBA / SRE 和 后端开发**两种角色各有侧重：

- **DBA / SRE**：管理多个 MySQL 实例，需要快速定位并恢复误删/误更新数据，熟悉 binlog 概念，关注操作的精确性和审计追踪
- **后端开发**：兼管数据库，需要简单易用的恢复工具，在发生误操作时能快速自救，不希望记忆复杂命令行工具

两种角色共享同一条核心工作流：浏览事务 → 选择恢复目标 → 执行回滚。

## Product Purpose

持续归档 MySQL 二进制日志（binlog），为任意时间点的误操作（DELETE、UPDATE）生成逆向 SQL，并通过 Web 控制台提供可浏览、可选择、可审计的事务级恢复能力。

## Positioning

一个单二进制即可部署的开源 MySQL PITR 平台，融合了 Web 控制台的易用性与 go-mysql 引擎的解析精度。不同于 MySQL 原生工具（mysqlbinlog）需要手动拼接命令，也不同于商业恢复工具的成本和锁定——它让恢复操作像浏览表格一样直观。

## Operating Context

- 用户通过浏览器访问 Web 控制台（内嵌于 server 二进制，无需额外部署前端）
- 典型场景：接到告警 → 登录控制台 → 选择实例 → 设置时间范围扫描 → 浏览事务列表 → 勾选要回滚的事务 → 执行逆向 SQL
- 操作过程有 SSE 进度流推送，长时间运行的任务可中断后从检查点继续
- 多实例通过 agent 管理，每个 agent 运行在 MySQL 主机上，通过 mTLS WebSocket 与 server 通信

## Capabilities and Constraints

### 已确认能力

- 误删恢复：由 DELETE 的行镜像生成逆向 INSERT
- UPDATE 回滚：逆向 UPDATE 恢复 Before 镜像值
- 指定时间恢复：扫描到任意时间点的 binlog
- 指定事务恢复：按 GTID / XID 精确定位并恢复指定事务
- GTID 定位：按 GTID 集过滤候选事务
- 大 binlog 增量归档：本地归档目录保留完整的 binlog 镜像，不受 MySQL 清理窗口影响
- 多实例管理：一个 server 通过 mTLS 管理多个 agent
- 检查点化执行：中断后从最后已提交批次继续回滚
- 5 步 PITR 向导式操作流程
- 审计日志、组织管理
- JWT 认证
- 内嵌前端（go:embed），单二进制部署

### 约束

- 依赖 MySQL binlog 开启（ROW 格式）
- 恢复粒度受限于 binlog 中记录的镜像信息
- 仅支持 DELETE 和 UPDATE 的逆向（DDL 变更不在当前恢复范围内）

## Brand Commitments

- 产品名称：**MySQL PITR**（不预设特定品牌名，README 中称为"MySQL PITR 平台"）
- 当前 logo：`docs/diagrams/logo.svg`
- 默认界面语言：简体中文（zh-CN 为第一语言，英文为第二语言）
- 视觉风格定位：现代、精致、专业的产品感
- 开源协议：MIT
- 仓库：GitHub

## Evidence on Hand

- 完整的 README.zh-CN.md 和 README.md 中文/英文文档
- 完整的系统架构图（docs/diagrams/pitr-architecture.png）
- 操作状态机文档
- 功能完整的 Web 前端骨架（SvelteKit 2 SPA），含路由、布局、i18n、认证
- 现有的 shadcn-svelte 组件库 + Tailwind CSS v4 主题系统
- Go server 后端代码

## Product Principles

1. **恢复操作必须精确可审计**——每一步操作都要有记录，SQL 预览必须与实际执行一致，不允许黑盒操作。
2. **降低认知负担**——向导式流程将复杂操作分解为可理解的步骤，让非 DBA 用户也能完成恢复。
3. **可靠优先于速度**——逆向 SQL 必须准确，宁可慢一些也不产生二次伤害；检查点机制确保可中断恢复。
4. **单二进制部署，零外部依赖**——从下载到运行，不依赖外部数据库、消息队列或容器编排。
5. **中文优先，国际化就绪**——默认中文界面，但架构上支持多语言扩展。

## Accessibility & Inclusion

Web 应用，需支持基本的键盘导航和屏幕阅读器兼容性。当前使用 shadcn-svelte 组件库，其底层 bits-ui 提供了 ARIA 支持。