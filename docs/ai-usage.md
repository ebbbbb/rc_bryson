# AI Agent 使用说明与工程判断

本项目组合使用 Codex 原生 Plan mode、Goal、项目级 Skill 和 Hooks：Plan mode 负责收集上下文、澄清需求并形成实施计划，Goal 维持长任务连续性，项目级 Skill 固化可复用的实现与 Review 流程，Hooks 执行格式化、状态跟踪和完成门禁等机械动作。长期约束写入 `AGENTS.md`，`CLAUDE.md` 只引用同一规则源，避免依赖单次会话的长 Prompt。

使用 AI Agent 完成需求调查、方案比较、工程实现、验证和 Code Review；本人保留产品语义、架构批准、范围控制、风险接受和最终验收权。

## 1. 协作方式

| 角色 | 职责 |
|---|---|
| 本人 | 定义问题与外部承诺，批准架构和技术栈，判断 AI 建议，控制外部写操作并验收证据 |
| AI Agent | 调查上下文，提出候选方案，分析故障路径，编辑代码和文档，运行验证并审查 diff |

AI Agent 的输出先经过 Decision Gate，再进入 Product Spec、ADR、代码或测试。未经批准的建议保持为候选方案；Code Review Comment 也必须先证明具体执行路径，不能直接成为架构决策。

## 2. 三个任务中的 AI Agent 使用

### 2.1 工程方法与决策监督

使用的能力：Plan mode 设计、分阶段 Prompt、显式 Steering、独立复核。

1. **设计阶段门：** 将工作拆成需求与架构、工程契约、工具链、Vertical Slice、独立 Review，避免用一条长 Prompt 直接生成完整项目。
2. **设计 Plan mode 入口：** 起草只读分析 Prompt，要求先澄清可靠性语义和系统边界；真正的 Plan 执行与产物落盘由主实现任务完成。
3. **提供第二视角：** 独立检查 SLO 前提、Scheduler/Reconciler 分工、DLQ、Replay 和 SSRF 边界，只把差异化意见发送给主实现任务。
4. **纠正职责越界：** 该任务误启动实现 Goal 和创建分支后，按本人要求取消 Goal、删除误建分支，并只保留交给主实现任务的精简 Prompt。

#### 未采用的外部工作流

项目开始阶段即调研了 Superpowers 和 Spec Kit，并在建立仓库工具链之前决定不引入。调研判断是：两者主要通过模型外的强制工作流或模板补足较早 LLM 与 Agent Harness 在规划、持续执行和验证方面的不稳定性；本项目使用的当前模型和 Codex Harness 已具备更完整的原生 Plan、Goal、Skill 与 Review 能力。

| 工作流 | 本项目未采用的原因 |
|---|---|
| [Superpowers](https://github.com/obra/superpowers) | 其强制 Brainstorming、Planning、TDD、Sub-agent 和 Review Skill 链会与 Codex 原生规划和执行循环重复，容易让模型在两套指令之间“左右脑互搏”，增加 Token、Review 轮次和完成时间 |
| [GitHub Spec Kit](https://github.com/github/spec-kit) | 完整 `Spec -> Plan -> Tasks -> Implement` 流程对单服务 MVP 过重，会生成超出当前需要的文档和模板，增加 Token 消耗、上下文负担与后续维护成本 |

[Codex Plan mode](https://learn.chatgpt.com/guides/best-practices#plan-first-for-difficult-tasks) 可以先收集上下文、提出澄清问题并形成更可靠的计划；本项目用它拆解需求、确认边界并只生成必要的 Product Spec、Architecture、ADR 和 `exec-plan`。Goal 和项目级 Skill 再承接长任务与专属流程，因此无需额外安装完整框架。

### 2.2 可靠通知 MVP 主实现

使用的能力：Plan mode、Goal、`skill-creator`、项目级 Skill、Hooks、Terminal。

1. **执行 Plan mode：** 通过短问题确认 `202`、At-least-once、Supplier 幂等、预注册目标、重试与留存等外部语义，再比较 PostgreSQL-only、PostgreSQL + MQ 和 Cloud Queue。
2. **先建立契约：** 创建 Product Spec、Architecture、ADR 和 `exec-plan`；技术栈未批准前过早生成的 Go/SQL 脚手架被删除，之后才开始实现。
3. **使用 Goal 持续推进：** Goal 只保存 Slice 1-8、逐片验证和 checkpoint 的长期目标，不重复 `AGENTS.md`、Skill 与 Hook 中的稳定规则。
4. **使用分阶段 Prompt：** 依次完成契约修订、工具链、Slice 1-8 和缺口修复，每阶段都限制改动范围并要求可执行完成证据。
5. **使用 Hooks 做机械门禁：** 成功编辑后定向格式化和跟踪 production diff；只有真实成功的验证与 Review receipt 才清除 Stop gate。
6. **保持语义与机械检查分离：** Hook 不判断 Test-first 或可维护性，相关判断由项目级 Skill 和测试完成。

项目级 Skill 由 `skill-creator` 初始化并验证：

| 项目级 Skill | 用途 |
|---|---|
| [`implement-slice`](../.agents/skills/implement-slice/SKILL.md) | 执行真实 Red、最小 Green、Refactor 和 Slice 回归 |
| [`reliability-review`](../.agents/skills/reliability-review/SKILL.md) | 审查状态、Outbox、generation、lease、ACK、retry、replay 和 SSRF 路径 |
| [`verify`](../.agents/skills/verify/SKILL.md) | 将 `make verify` 作为唯一实现完成入口 |
| [`review-maintainability`](../.agents/skills/review-maintainability/SKILL.md) | 检查过度防御、薄封装、无依据兼容、test-shaped code 和重复 Policy |

实现按 [`exec-plan.md`](exec-plan.md) 的 Slice 1-8 推进；结果、恢复场景和容量条件分别记录在 README、[`architecture.md`](architecture.md) 与 [`capacity-report.md`](capacity-report.md)，不在本说明重复展开。

### 2.3 独立 PR 审查与需求一致性

使用的能力：`skill-creator`、项目级 Review Skill、Codex 原生 Code Review、GitHub ChatGPT/Codex Review。

1. **建立项目专属 Review：** 使用 `skill-creator` 创建 `reliability-review`，后续补充 `review-maintainability`，把本项目的状态机、Outbox、fencing、SSRF 和 AI 代码维护性风险固化为可复用审查流程。
2. **完成本地第一轮 Review：** 将项目级 Review Skill 与 Codex 原生 Code Review 结合，在 Commit 前审查当前 diff；先修复发现的问题并完成验证，确认无阻塞问题后才提交 Commit。
3. **启用自动 PR Review：** 将 ChatGPT/Codex Review 集成到 GitHub 仓库，提交 PR 后自动执行独立的 PR 级审查；[PR #2 Review](https://github.com/ebbbbb/rc_bryson/pull/2#pullrequestreview-4770012392) 是该自动流程成功运行的证据。
4. **形成双重安全检查：** 本地 Review 利用项目专属规则在提交前拦截问题，GitHub Review 在 PR 层提供第二个独立视角，在减少人工重复检查的同时降低遗漏风险。

PR #2 的自动 Review 发现了 [`Retry-After` 溢出](https://github.com/ebbbbb/rc_bryson/pull/2#discussion_r3642835435) 和 [网络策略应在 DNS 前拒绝](https://github.com/ebbbbb/rc_bryson/pull/2#discussion_r3642835436)两个问题；两项均先复现 Red、完成最小修复并重新验证。

## 3. 跨任务的 AI Agent 控制面

| 工具或技术 | 用途 |
|---|---|
| `AGENTS.md` | 保存长期不变量、稳定命令、Review 要求和 Definition of Done |
| `CLAUDE.md` | 仅引用 `AGENTS.md`，避免为不同 AI Agent 维护两套规则 |
| 项目级 Skill | 保存需要语义判断且会重复使用的实现、可靠性、验证和可维护性流程 |
| Hooks | 自动执行格式化、变更跟踪和 Stop gate 等确定性动作，不冒充语义审查 |
| 分层 Prompt | 早期发现规则，稳定后只传递目标和增量决策，减少重复上下文与规则漂移 |

## 4. AI 输出的采纳、修正与拒绝

| AI 输出或倾向 | 处理 | 判断依据 |
|---|---|---|
| 建议 PostgreSQL-only Queue | 改为 PostgreSQL + RabbitMQ + Transactional Outbox | 需要 MQ 信号层和 API/Worker 隔离，同时保持 PostgreSQL 唯一事实源 |
| 技术栈未批准前创建脚手架 | 删除并退回契约阶段 | 实现不能反向决定产品语义 |
| 将逻辑职责暗示为多个服务 | 保持单 Binary 内独立 Loop | MVP 不需要额外部署、发现和运维故障面 |
| 将 `p99 <= 60s` 写成无条件保证 | 增加依赖健康、Backlog 和 Destination 限流前提 | 单目标吞吐上限决定该指标不可能无条件成立 |
| 要求所有验证都依赖容器内 Go | 改为本地开发可用宿主工具，最终产物必须容器化 | 兼顾迭代效率与交付可复现性 |
| Goal Prompt 重复全部仓库规则 | 缩短为目标、补充批准项和 checkpoint | `AGENTS.md`、项目级 Skill 和 Hooks 才是稳定事实源 |
| 提前引入 Redis、每目标 Queue 或 MQ 事实源 | 不采纳，选择更小的 deferred signal 与 `observed_at` 修复 | 避免新增状态源和动态拓扑，保持现有不变量 |
| 让 API Readiness 依赖 RabbitMQ | 不采纳 | RabbitMQ 是可重建 signal 层，队列故障时 API 仍应可靠接收 |
| 让 Hook 判断 Test-first 或可维护性 | 不采纳，Hook 只检查证据是否存在 | Shell Hook 无法可靠完成语义判断 |
| 宽松接受未知 MQ 字段和静默吞掉 Bootstrap 冲突 | 改为严格 Schema 与显式配置冲突 | 没有批准的兼容契约，不应猜测未来数据 |
| 直接采用 PR Comment 的实现建议 | 先复现问题，再选择符合现有契约的最小修复 | Review 提供候选问题，不拥有架构决策权 |

## 5. 本人保留的关键决策

README 和 ADR 记录技术细节；此处只说明哪些决策没有委托给 AI Agent。

| 决策范围 | 本人作出的决定 | 原因 |
|---|---|---|
| 外部产品承诺 | 确定 `202`、At-least-once、稳定幂等值、24 小时重试和人工 Replay | 这些语义决定调用方与 Supplier 承担的业务风险 |
| 架构边界 | 批准 PostgreSQL 唯一事实源、RabbitMQ signal layer、Transactional Outbox 和预注册 Destination | 这些选择决定长期数据一致性、安全边界和运维成本 |
| MVP 复杂度 | 批准单 Binary、单 Worker、in-process limiter，不提前引入 Redis 或微服务 | 当前容量不需要分布式协调，额外组件会扩大故障面 |
| 开发权限 | 要求契约先于代码，限制每阶段范围，并批准 Branch、Commit、Push 和 PR 写入 | AI Agent 的执行权限不能替代范围与外部状态控制 |
| 审查与验收 | 区分 PR diff Review 与完整仓库主线检查，决定哪些评论采纳及何时完成 | 局部代码正确不等于产品方向正确，自动评论也不等于事实 |

## 可核验依据

- AI Agent 长期规则：[`AGENTS.md`](../AGENTS.md)
- 项目级 Skill：[`.agents/skills/`](../.agents/skills/)
- Hook 配置与 Fixture：[`.codex/hooks.json`](../.codex/hooks.json)、[`.codex/hooks/tests/hooks_test.sh`](../.codex/hooks/tests/hooks_test.sh)
- 实现计划与证据：[`exec-plan.md`](exec-plan.md)、[`capacity-report.md`](capacity-report.md)
- 自动 PR Review：[PR #2](https://github.com/ebbbbb/rc_bryson/pull/2)
