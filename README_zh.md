# HartMesh

[English](README.md) | 简体中文 | [日本語](README_ja.md) | [Français](README_fr.md) | [Русский](README_ru.md) | [Español](README_es.md) | [Português](README_pt.md) | [Deutsch](README_de.md)

## DeerFlow 智能体的可靠执行层。

HartMesh 是面向运维的 DeerFlow 下游发行版，提供重放安全的调用、可执行的策略边界、可检查的生命周期证据，以及明确说明适用范围的部署保证。

它建立在 DeerFlow 的工作区、沙箱、记忆、技能、工具、子智能体、定时任务和渠道能力之上。

> [!IMPORTANT]
> **状态：预发布。** 已发布的 `v2.1.0+hartmesh.*` 标签包含持久调用运行时及其 HTTP API。本源码树中的证据层——绑定装配、持久工具回执、服务端拥有的租户标识、受治理工具面、已接受沙箱证据、可移植运行证据包与执行策略——尚未包含在任何发布标签中。
>
> 精确的真实部署资格认定仍是独立、与制品绑定的门槛。

HartMesh 是 ByteDance [DeerFlow](https://github.com/bytedance/deer-flow) 的独立下游发行版。它不是 DeerFlow 官方版本，与 ByteDance 没有关联，也未获得其认可。

[**评估预览版**](#工作区快速入门) · [**检查证据**](backend/docs/INVOCATION_RUNTIME.md) · [**探索运行时契约**](backend/packages/runtime-api/README.md)

## HartMesh 为何存在

智能体容易启动，却很难稳定运营。

客户端会重试，策略会变化，技能会演进，进程会失败，多个服务也可能需要观察同一项工作。

HartMesh 在 DeerFlow 周围增加可靠的调用边界，让已接受的工作能够一致地重试、治理、检查和控制。

在同一认证作用域内，只要常规运行记录仍被保留，使用同一外部键重复完全相同的严格请求，HartMesh 就会返回已保留的 `run_id`。

若在该键下改变规范化执行意图，则返回冲突。

这是重放安全的准入，不是对模型、工具、提供商或其他外部副作用的恰好一次执行。

HartMesh 首要面向：

- 将 DeerFlow 工作嵌入 API、服务、定时任务或渠道的平台开发者；
- 评估受治理、可持久化单 Gateway 拓扑的运维人员；以及
- 运行高价值定时工作流和签名 GitHub 工作流的团队。

## 基于 DeerFlow，为运维而强化

DeerFlow 提供智能体基础：工作区、LangGraph harness、沙箱、记忆、技能、工具、子智能体、定时任务和原生渠道。

HartMesh 保留这些体验，并增加围绕长时间工作接受、治理、观察和控制的控制平面。

以下是固定快照比较。确切提交见[兼容性与来源](#兼容性上游基线与发布状态)；这不是对未来所有上游版本的声明。

| 从基线保留的能力 | HartMesh 在其周围增加的能力 |
| --- | --- |
| 工作区、harness、记忆、沙箱、技能、工具、子智能体、定时任务和渠道 | 一个感知来源的准入边界；交付持久性仍因传输方式而异 |
| DeerFlow 线程/运行生命周期、Gateway REST 路由和 LangGraph 兼容路由 | 按来源划分的规范外部键、保留的准入身份，以及明确的意图变更冲突 |
| 智能体、扩展和沙箱配置 | 固定的已接受执行材料 |
| Gateway 与嵌入式集成接口 | 严格的 `ensure`、`observe` 和带 fencing 的 `control` 记录 |
| 本地与 Helm 运行 | 生命周期证据，以及明确的持久性、拓扑和资格报告 |

你保留的是 DeerFlow 的智能体基础和兼容接口；新增的是带证据的调用控制平面。

你仍不会获得主动-主动 Gateway 高可用、调度器高可用、通用崩溃恢复或外部副作用恰好一次保证。

## 当……时会怎样？

| 场景 | HartMesh 行为 |
| --- | --- |
| [客户端丢失响应后重试](backend/tests/test_invocation_idempotency.py) | 相同规范意图返回已接受的运行；意图变化则冲突。 |
| [技能在准入后发生变化](backend/tests/test_accepted_skill_snapshots.py) | 已接受的调用在智能体与沙箱消费者之间保持同一份捕获技能树；后续工作才会看到修改。 |
| [远程沙箱执行已接受技能](backend/tests/test_kubernetes_accepted_skill_projection.py) | 支持的 `rwx_verified_copy_v2` 投影在图/模型工作前绑定并重新验证准入技能与隔离证据；真实跨节点资格仍与精确制品绑定。 |
| [服务代表人类执行或观察其他所有者](backend/tests/test_service_observation_grants.py) | 人类主体、代理服务和来源证据保持分离；跨所有者观察需要有限授权及当前授权判定。 |
| [必需的策略能力不健康](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | 就绪检查和真正的新准入会关闭失败；相同重放复用密封的接受证据，而当前观察授权仍控制披露。 |
| [运维人员询问哪些内容可持久化或已获资格](backend/app/runtime/deployment.py) | 报告会命名持久层；声明的引用属于运维方断言，只有与[离线验证器](backend/scripts/verify_qualification_evidence.py)精确匹配的证据才支持独立资格认定。 |
| [生命周期历史被裁剪或不一致](backend/tests/test_invocation_lifecycle_query.py) | 有界观察返回类型化游标或完整性结果，而不是静默展示无效历史。 |
| [签名 GitHub 交付被中断或线程繁忙](backend/tests/test_durable_inbound_receipts.py) | PostgreSQL 支持的回执恢复可回收过期租约、保持 FIFO 延后顺序，并收敛到同一已接受运行。 |

<!-- 未来演示：添加 30–60 秒终端录屏，展示带键调用、模拟响应丢失、相同重试返回同一 run_id、意图变化冲突和生命周期观察。 -->

## 选择路径

| 路径 | 适合对象 | 第一项证明 |
| --- | --- | --- |
| [工作区](#工作区快速入门) | 使用 HartMesh 控制评估完整 DeerFlow 工作区的开发者 | 通过统一本地入口获得模型响应 |
| [运行时集成](#durable-invocation-http-api) | 将智能体工作嵌入服务、定时任务或渠道的平台 | 相同带键请求返回一个 `run_id`，随后可观察生命周期 |

评估受治理部署的运维人员可直接阅读[部署与持久性边界](#部署与持久性边界)。

> [!CAUTION]
> **部署前先了解边界。**
>
> - 已验证拓扑只有一个 Gateway 副本。
> - 不声明主动-主动 Gateway 高可用、调度器高可用或零停机滚动更新。
> - 重放安全准入不等于外部副作用恰好一次，也不等于通用崩溃恢复。
> - 本地内存和 SQLite 渠道入口仍为尽力而为。
> - 下文相应的共享持久性和资格声明需要 PostgreSQL 及可独立验证的精确证据。

## 工作区快速入门

请在当前 HartMesh checkout 的仓库根目录工作。

此预览路径需要 Python 3.12+、Node.js 22+、pnpm 或 Corepack、`uv`、GNU Make、nginx、Docker 或 Apple Container、模型凭据，以及约 4 核 CPU 和 8 GB 内存。Windows 本地开发使用 Git Bash。

`make setup` 询问执行模式时，请选择 **Container sandbox**。`LocalSandboxProvider` 仅在有效技能集明确为空时支持持久调用；此 checkout 默认启用内置技能。

> [!WARNING]
> `make dev` 是可信网络开发路径：Gateway `8001`、frontend `3000` 和本地 nginx `2026` 都使用主机通配监听。完成首位管理员设置时，只能在可信或受主机防火墙保护的机器上运行。

配置、诊断并启动：

```bash
make check
make setup
make doctor
make dev
```

`make setup` 写入被 Git 忽略的本地配置。`make dev` 会再次检查工具、同步依赖，并启动 Gateway、frontend 和 nginx。

需要 pre-commit hooks 的贡献者可选运行 `make install`。

打开 [http://localhost:2026](http://localhost:2026)。全新安装时，先完成首位管理员设置，再创建线程并提交提示。

成功标志是工作区通过 Gateway 支持的运行生命周期流式返回已配置模型的响应。

在另一个终端停止：

```bash
make stop
```

这会评估继承的工作区和本地栈。继续运行时路径以验证 HartMesh 保留 `run_id` 的行为；仅工作区成功并不能证明 PostgreSQL 持久性或真实 Kubernetes 资格。

## Durable Invocation HTTP API

当可信后端服务需要 `ensure → observe` 时，使用预发布的 `/api/runtime/v1` 接口。

它使用仅依赖标准库的 [`deerflow-runtime-api`](backend/packages/runtime-api/README.md) 中的严格记录。

此路径不需要浏览器工作区，但仍需要已配置的 HartMesh checkout。首次评估者应在仓库根目录运行：

```bash
make check
make setup
make doctor
```

为 `make setup` 选择的模型配置凭据。若已完成工作区设置，可跳过这些命令。

首次运行时评估也必须选择 **Container sandbox**。Local provider 的持久执行要求有效技能集为空。

如上所述，`make dev` 在端口 `8001`、`3000` 和 `2026` 使用主机通配监听；请使用可信或受主机防火墙保护的机器。

如果根目录 `.env` 已定义非空 `DEER_FLOW_INTERNAL_AUTH_TOKEN`，请在客户端终端使用该值。

删除或注释空赋值，因为加载 `.env` 会覆盖下面的 shell export。否则，请在启动 HartMesh 前生成令牌：

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Copy this token into the trusted client terminal:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

在第二个终端导出打印出的令牌，并运行这个仅使用标准库的客户端：

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN='<paste the generated token>'

uv run --project backend python - <<'PY'
import json
import os
from urllib.request import Request, urlopen
from uuid import uuid4

BASE = "http://localhost:2026/api/runtime/v1"
HEADERS = {
    "Content-Type": "application/json",
    "X-DeerFlow-Internal-Token": os.environ["DEER_FLOW_INTERNAL_AUTH_TOKEN"],
}


def call(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = Request(BASE + path, data=data, headers=HEADERS, method=method)
    with urlopen(request) as response:
        return response.status, json.load(response)


evaluation_id = uuid4().hex
intent = {
    "api_version": "deerflow.runtime/v1",
    "kind": "invocation.ensure",
    "external_key": f"readme-evaluation-{evaluation_id}",
    "thread_id": f"readme-evaluation-{evaluation_id}",
    "agent_hint": None,
    "input": {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.input.graph",
        "value": {
            "messages": [
                {
                    "role": "user",
                    "content": "Explain replay-safe admission in one sentence.",
                }
            ]
        },
    },
    "options": {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.options",
        "model_name": None,
        "thinking_enabled": None,
        "multitask_strategy": "reject",
        "checkpoint_id": None,
        "interrupt_before": None,
        "interrupt_after": None,
    },
}

first_status, first = call("POST", "/invocations/ensure", intent)
replay_status, replay = call("POST", "/invocations/ensure", intent)
assert first["run_id"] == replay["run_id"]
print("ensure:", first_status, first["disposition"], first["run_id"])
print("replay:", replay_status, replay["disposition"], replay["run_id"])

run_id = first["run_id"]
_, observation = call("GET", f"/invocations/{run_id}?limit=100")
print("observe:", observation["status"], observation["state_version"])
PY
```

新键返回 `201 created`；相同重放返回 `200 known` 和同一 `run_id`。保持键不变但修改消息，会收到类型化 `409 conflict`，而不是在一个键下启动不同工作。

此示例使用内置 `gateway-internal` 服务，并有意省略人类委托。绝不能把内部令牌暴露给浏览器或不可信客户端。

运行时特定的所有者委托会依据现有本地用户重新验证 `X-DeerFlow-Owner-User-Id`。

参见[主体投影](backend/app/gateway/services.py)和[身份测试](backend/tests/test_invocation_identity_separation.py)。

DTO、游标分页、类型化失败和带 fencing 的取消，参见[运行时契约](backend/packages/runtime-api/README.md)与 [HTTP 参考](backend/docs/API.md#durable-invocation-runtime-api)。

一次澄清回答会在同一 DeerFlow 线程上启动一个**新的调用**。

评估结束后，从仓库根目录停止 HartMesh：

```bash
make stop
```

## 运维价值

### 重放安全准入

稳定外部键与完整的规范调用方意图会收敛到一个保留调用。相同意图在任意生命周期状态下返回该记录；意图变化则冲突，且只有创建者会附加 worker。

此保证只在常规运行记录保留期间有效，不能去重任意外部副作用。

证据：[`idempotency.py`](backend/app/runtime/idempotency.py) 和 [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py)。

### 单一准入边界

HTTP create、stream、wait 路由、定时任务、经认证的原生渠道以及嵌入式服务都会进入同一 `InvocationRuntime`。来源认证、确认和入口持久性仍因来源而异。

证据：[`invocation.py`](backend/app/runtime/invocation.py) 和[闭环矩阵](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix)。

### 固定的已接受执行材料

准入会固定智能体修订版、扩展代次、可信上下文、约束证据、有效技能包，以及执行/投影 profile。

同一棵不可变已接受技能树服务于主智能体和子智能体提示、激活、策略及沙箱读取。

非空已接受技能使用受支持的 accepted-only profile：本地容器型 AIO，或带 fenced `rwx_verified_copy_v2` 投影的 Kubernetes。

`LocalSandboxProvider`、E2B、自定义和其他远程 profile 仍仅支持空技能。离线投影证据不能证明真实跨节点资格。

证据：[`runtime/` 源码目录](backend/packages/harness/deerflow/runtime/)中的 `accepted_invocation.py`、`agent_revision.py` 和 `skill_snapshot.py`。

远程证据：[`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) 和[投影测试](backend/tests/test_kubernetes_accepted_skill_projection.py)。

### 随执行而行的策略

HartMesh 将有效主体、代理服务和来源证据保持分离。

除非运维人员授予有限观察范围，经认证服务仍受所有者约束。授权限定发现范围；当前授权仍决定可返回内容，取消操作不会继承该授权。

当运维人员启用调用操作授权或配置权威 v2 约束时，这些命名操作会关闭失败。运维人员要求的能力健康检查和 MCP 准备也如此。

授权和调用操作控制默认关闭，`required_capabilities` 默认为空。

可选观察中间件和旧版 API 可写 MCP interceptor 仍采用 fail-open 或警告后跳过行为。

所有者与路由检查仍然存在，但 HartMesh 不提供通用组织策略，也不保证任意第三方工具。

证据：[扩展契约](backend/packages/extension-api/README.md)、[授权](backend/app/runtime/authorization.py)、[约束](backend/app/runtime/constraints.py)和[可见性](backend/app/runtime/visibility.py)。

### 可移植运行时集成

仅依赖标准库的 `deerflow-runtime-api` 定义严格不可变记录和一个 `DurableInvocationPort`：`ensure`、调用/上下文 `observe`、带 fencing 的 `control` 以及 `capabilities`。

经认证 HTTP 与应用托管的进程内 adapter 共享这些记录和一套一致性测试。同步 `DeerFlowClient` 不是持久 adapter；v1 不提供 broker push、上下文导出或上下文退役。

证据：[运行时包](backend/packages/runtime-api/README.md)和[传输一致性](backend/tests/test_runtime_api_conformance.py)。

### 事务性生命周期完整性

使用 SQL store 时，状态变化及其安全生命周期事件会在一个状态版本下原子提交。有界观察使用权威快照，并对已裁剪、未来或不一致历史返回类型化结果。

只有外部 PostgreSQL 门槛通过后，才能声明 PostgreSQL repeatable-read 和跨会话行为。

证据：[`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) 和 [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py)。

契约测试：[原子生命周期 store](backend/tests/test_invocation_lifecycle_store.py) 和[类型化生命周期查询](backend/tests/test_invocation_lifecycle_query.py)。

### 持久化签名 GitHub 入口

通过 HMAC 验证与 PostgreSQL 回执，签名 GitHub 入口会在确认前持久保存有界来源绑定。

租约与 fence 可在中断后回收过期租约、保持繁忙线程 FIFO，并收敛到同一已接受调用。

此声明仅限使用 PostgreSQL 的已验证签名 GitHub 入口；其他及本地渠道路径仍为尽力而为。

证据：[回执 store](backend/app/channels/inbound_receipts.py)和[回执测试](backend/tests/test_durable_inbound_receipts.py)。

## 部署与持久性边界

遵循任何部署指南前，请先阅读这些限制：

- 已验证拓扑只有一个 Gateway 副本。
- 不声明主动-主动 Gateway 高可用、调度器高可用或零停机滚动更新。
- 重放安全准入不是外部副作用的通用恰好一次执行。
- 使用持久调用存储时，进程丢失恢复会保留权威终止证据；它不会透明恢复每个图或工具调用。
- Memory 是进程本地的，SQLite 为调用状态提供节点持久性，PostgreSQL 是共享持久 store。
- Memory 和 SQLite 原生渠道入口仍为尽力而为。
- 持久原生入口目前指使用 PostgreSQL 的已验证签名 GitHub 交付。
- 非空已接受技能需要受支持的 accepted-only 沙箱路径。
- Kubernetes/PostgreSQL 资格要求与指定镜像、chart、配置、schema、拓扑、范围和场景精确对应的通过证据。
- 已收集或跳过的可选门槛不构成资格通过。

对于审计的源码快照，外部 PostgreSQL 和 Kubernetes 可选门槛未配置，也没有精确的通过资格制品。

它们是尚未通过的发布门槛，不表示已实现行为不存在。

| 模式 | 报告边界 |
| --- | --- |
| `local_development` | 允许进程本地状态，但不声明持久性。 |
| `durable_production` | 启动和就绪时拒绝进程本地调用状态。 |
| Helm `local_evaluation` | 单 Gateway 评估默认值；明确未获资格。 |
| Helm `durable_one_replica` | 要求 digest 固定的镜像、PostgreSQL/共享状态以及安全 probe 和关闭时序；没有精确通过证据时仍未获资格。 |

管理员部署报告将持久层级、健康、来源和资格分开。

提供的资格引用仍标为 `operator_asserted`；只有离线验证器可针对精确证据建立 `external_evidence_verified`。可移植 capabilities 不携带部署声明。

`GET /health` 报告进程存活。`GET /ready` 是有界就绪/未就绪信号。管理员通过 `GET /api/runtime/v1/deployment` 检查持久性和资格。

证据：[部署报告](backend/app/runtime/deployment.py)、[资格验证](backend/scripts/verify_qualification_evidence.py)和 [Helm 部署契约](deploy/helm/deer-flow/README.md)。

使用持久调用存储时，如果活跃调用随进程丢失，恢复会记录权威终止证据，例如 `stop_reason=orphan_recovered`。

相同重放会返回该保留的终止运行。继续产品意图需要在新进程代次下创建新调用。

参见[权威生命周期与故障恢复](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery)。

### 安全边界

Compose 栈仅发布 nginx，默认绑定 `127.0.0.1:2026`。本地 `make dev` 则在端口 `8001`、`3000` 和 `2026` 使用主机通配监听；不要把它视为具有相同的发布端口边界。

HartMesh 智能体可以执行命令，并读取或写入配置工具允许的文件。隔离取决于提供商：`LocalSandboxProvider` 共享 Gateway 主机身份，并非操作系统隔离边界。

命令分类与路径重写只是纵深防御。对不可信工作使用受支持的隔离提供商。

在服务可从 loopback 之外访问前完成首位管理员设置。

管理员可以配置 stdio MCP 进程和可信 Python 插件，因此管理员访问等同于代码执行。

单 Gateway 渲染契约、已接受技能投影、凭据和精确资格流程见 [Helm 部署指南](deploy/helm/deer-flow/README.md)。

## 扩展模型

HartMesh 保留 DeerFlow 的技能、工具、MCP server、自定义智能体和中间件模型。

与宿主无关的 [`deerflow-extension-api`](backend/packages/extension-api/README.md) 增加了授权、身份和可信上下文贡献的类型化契约。

它还涵盖限制性约束、能力健康和必需 MCP 准备。

Python 插件是可信运维代码，从 `config.yaml` 顶层 `plugins:` 在启动时加载。该列表有意位于 API 可写的 `extensions_config.json` 之外；后者负责 MCP 和技能配置。

已接受调用会固定一个启动时冻结的扩展代次。技能变化影响后续准入；插件变化需要重启 Gateway 才会创建新代次。两者都不会改变已接受工作。

## 兼容性、上游基线与发布状态

HartMesh 保留现有 `deerflow.*` namespace、包名、`DEER_FLOW_*` 变量、Docker/Helm 标识符、文件系统路径和 Gateway 兼容接口。

产品比较使用固定本地范围 `e16ef2969b1446162e19af7bdde1446674851e66...4023cb434aa67011b9d18e90029f473b55323856`。

HartMesh `main` 已同步 upstream `deerflow/main` 至 `0f7d8709d3bbf0be26460b6277fbad9329302243`（2026-09-04）。

这是背景信息，不是上述基线；HartMesh 不作持续优于上游的声明。

此仓库尚未记录 HartMesh 同步节奏、API/配置/数据库兼容窗口、支持窗口、安全修复接收策略或上游贡献策略。

这些 hash 仅代表来源，不是维护承诺。

Alembic 图只有一个 head：`0011_mcp_tasks` 分支为 HartMesh 迁移（直至 `0019_inbound_event_identity`）与 upstream 的 `0012_mcp_task_results`，再由 `0020_merge_mcp_task_results` 合并。

PostgreSQL 运维人员应在回滚前停止写入并备份数据；迁移说明见 [backend/AGENTS.md](backend/AGENTS.md)。

版本源报告 `2.1.0+hartmesh.4`，即最新标签。它早于上述证据层，因此仅凭版本字符串无法判断某个检出包含哪些 HartMesh 工作。

[RELEASING.md](RELEASING.md) 记录了已发布标签所用的 `X.Y.Z+hartmesh.N` 分支发布流程。

## 文档

- [持久调用运行时](backend/docs/INVOCATION_RUNTIME.md) — 保证、证据、恢复和延后范围
- [运行时 API](backend/packages/runtime-api/README.md) — DTO 与 `DurableInvocationPort`
- [Gateway API](backend/docs/API.md) — 经认证 HTTP 行为
- [扩展 API](backend/packages/extension-api/README.md) — 策略与信任边界
- [Helm 部署](deploy/helm/deer-flow/README.md) — 单 Gateway 模式与资格
- [配置](config.example.yaml) — 运维设置
- [后端指南](backend/AGENTS.md)和[前端指南](frontend/AGENTS.md) — 架构与测试

## 支持与安全

在仓库根目录运行本地诊断：

```bash
make doctor
make support-bundle
```

分享前请检查生成的支持材料。

此仓库尚未记录 HartMesh 自有 issue tracker、发布渠道或私密漏洞报告入口。

[CONTRIBUTING.md](CONTRIBUTING.md) 和 [SECURITY.md](SECURITY.md) 保留 ByteDance DeerFlow 的上游目的地；它们不是 HartMesh 自有支持渠道。

不要在公开 issue 中放入凭据、令牌、私有提示、客户数据或漏洞细节。请将内部令牌、webhook secret、提供商 key 和数据库凭据视为秘密。

## 参与贡献

本地开发请遵循 [CONTRIBUTING.md](CONTRIBUTING.md) 中继承的约定，以及最近的 [AGENTS.md](AGENTS.md) 中的仓库命令与模块归属。

## 许可证

HartMesh 保留 DeerFlow 的 [MIT License](LICENSE) 和现有声明。

## 致谢

HartMesh 的存在得益于 ByteDance 和 DeerFlow 贡献者开放了其所扩展的智能体基础。我们也感谢 LangChain、LangGraph 和更广泛的开源智能体生态。

HartMesh 下游的运维声明、发布状态、资格和支持边界由 HartMesh 自行负责。
