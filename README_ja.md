# HartMesh

[English](README.md) | [简体中文](README_zh.md) | 日本語 | [Français](README_fr.md) | [Русский](README_ru.md)

## DeerFlow エージェントの信頼できる実行レイヤー。

HartMesh は、リプレイセーフな呼び出し、強制可能なポリシー境界、検査可能なライフサイクル証拠、適用範囲を明示するデプロイ保証を提供する、運用重視の DeerFlow ディストリビューションです。

DeerFlow のワークスペース、サンドボックス、メモリ、スキル、ツール、サブエージェント、スケジュール、チャネルを基盤にしています。

> [!IMPORTANT]
> **ステータス：プレリリース。** このソースツリーには実装済みランタイムとオフライン契約証拠がありますが、それを含む HartMesh リリースタグはまだありません。
>
> 実デプロイの厳密な適格性確認は、成果物に紐づく別のゲートです。

HartMesh は ByteDance の [DeerFlow](https://github.com/bytedance/deer-flow) から派生した独立ディストリビューションです。公式 DeerFlow リリースではなく、ByteDance との提携や推奨関係もありません。

[**プレビューを評価**](#ワークスペースクイックスタート) · [**証拠を確認**](backend/docs/INVOCATION_RUNTIME.md) · [**ランタイム契約を見る**](backend/packages/runtime-api/README.md)

## HartMesh が必要な理由

エージェントは始めやすく、運用し続けるのは困難です。

クライアントは再試行し、ポリシーは変わり、スキルは更新され、プロセスは失敗し、複数サービスが同じ処理を観測することもあります。

HartMesh は DeerFlow の周囲に信頼できる呼び出し境界を加え、受理した処理を一貫して再試行、統制、検査、制御できるようにします。

同じ認証スコープで通常の run 行が保持されている間、同じ外部キーで同一の厳密なリクエストを繰り返すと、HartMesh は保持済みの `run_id` を返します。

そのキーで正規化された実行意図を変えると、競合を返します。

これはリプレイセーフな受理であり、モデル、ツール、プロバイダー、その他の外部副作用を exactly-once で実行する保証ではありません。

HartMesh が第一に対象とするのは：

- DeerFlow の処理を API、サービス、スケジュール、チャネルへ組み込むプラットフォーム開発者；
- 統制された永続的な単一 Gateway トポロジーを評価する運用担当者；および
- 高価値のスケジュール処理や署名付き GitHub ワークフローを自動化するチームです。

## DeerFlow を基盤に、運用向けに強化

DeerFlow はエージェント基盤として、ワークスペース、LangGraph harness、サンドボックス、メモリ、スキル、ツール、サブエージェント、スケジュール、ネイティブチャネルを提供します。

HartMesh はその体験を維持し、長時間処理の受理、統制、観測、制御を担うコントロールプレーンを追加します。

以下は固定スナップショットの比較です。正確なコミットは[互換性と来歴](#互換性上流ベースラインリリース状況)を参照してください。将来の上流リリースすべてに対する主張ではありません。

| ベースラインから維持するもの | HartMesh が周囲に追加するもの |
| --- | --- |
| ワークスペース、harness、メモリ、サンドボックス、スキル、ツール、サブエージェント、スケジュール、チャネル | ソースを認識する単一の受理境界。配送の永続性はトランスポートごとに異なる |
| DeerFlow の thread/run ライフサイクル、Gateway REST ルート、LangGraph 互換ルート | ソース単位の正規外部キー、保持される受理 ID、実行意図変更時の明示的競合 |
| エージェント、拡張、サンドボックス設定 | 固定された受理済み実行素材 |
| Gateway と組み込み統合サーフェス | 厳密な `ensure`、`observe`、フェンス付き `control` レコード |
| ローカル運用と Helm | ライフサイクル証拠、明示的な永続性・トポロジー・適格性レポート |

維持されるのは DeerFlow のエージェント基盤と互換サーフェスです。新たに得られるのは、証拠を伴う呼び出しコントロールプレーンです。

active-active Gateway HA、スケジューラー HA、汎用的なクラッシュ再開、外部副作用の exactly-once は提供しません。

## もし……が起きたら？

| シナリオ | HartMesh の動作 |
| --- | --- |
| [レスポンスを失ったクライアントが再試行](backend/tests/test_invocation_idempotency.py) | 同じ正規意図なら受理済み run を返し、意図が変われば競合します。 |
| [受理後にスキルが変更](backend/tests/test_accepted_skill_snapshots.py) | 受理済み呼び出しはエージェントとサンドボックス利用者の間で同じ取得済みスキルツリーを使い続け、変更は後続処理にのみ反映されます。 |
| [リモートサンドボックスが受理済みスキルを実行](backend/tests/test_kubernetes_accepted_skill_projection.py) | 対応する `rwx_verified_copy_v2` 投影が graph/model 処理前に受理済みスキルと分離証拠を結合・再検証します。実クラスタのクロスノード適格性は別の成果物依存ゲートです。 |
| [サービスが人間を代理、または別 owner を観測](backend/tests/test_service_observation_grants.py) | 人間主体、代理サービス、ソース証拠を分離します。owner をまたぐ観測には有限 grant と現在の認可が必要です。 |
| [必須ポリシー能力が不健全](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | readiness と真正な新規受理は fail closed になります。同一リプレイは封印済み受理証拠を再利用しますが、開示には現在の観測認可が適用されます。 |
| [何が永続的・適格かを運用者が確認](backend/app/runtime/deployment.py) | レポートは永続化層を示します。宣言参照は operator assertion であり、[オフライン検証器](backend/scripts/verify_qualification_evidence.py)と一致する厳密な証拠だけが独立した適格性を支えます。 |
| [履歴が枝刈りまたは不整合](backend/tests/test_invocation_lifecycle_query.py) | 有界観測は、無効な履歴を黙って表示せず、型付きカーソルまたは整合性結果を返します。 |
| [署名付き GitHub 配送が中断、または thread が混雑](backend/tests/test_durable_inbound_receipts.py) | PostgreSQL receipt recovery は期限切れ lease を回収し、FIFO 遅延を維持し、同じ受理済み run へ収束できます。 |

<!-- 将来のデモ：キー付き呼び出し、レスポンス喪失の模擬、同じ run_id を返す再試行、意図変更の競合、ライフサイクル観測を示す 30–60 秒の端末録画を追加。 -->

## 評価パスを選ぶ

| パス | 最適な対象 | 最初の証明 |
| --- | --- | --- |
| [ワークスペース](#ワークスペースクイックスタート) | HartMesh の制御を含む完全な DeerFlow ワークスペースを評価する開発者 | 統一ローカル入口を通じたモデル応答 |
| [ランタイム統合](#durable-invocation-http-api) | エージェント処理をサービス、スケジュール、チャネルへ組み込むプラットフォーム | 同じキーのリクエストが一つの `run_id` を返し、その後ライフサイクルを観測 |

統制されたデプロイを評価する運用担当者は、[デプロイと永続性の境界](#デプロイと永続性の境界)へ直接進めます。

> [!CAUTION]
> **デプロイ前に境界を確認してください。**
>
> - 検証済みトポロジーは Gateway 1 replica です。
> - active-active Gateway HA、スケジューラー HA、zero-downtime rollout は主張しません。
> - リプレイセーフな受理は、外部副作用の exactly-once や汎用クラッシュ再開ではありません。
> - ローカル memory/SQLite のチャネル ingress は best-effort です。
> - 以下の shared-durable と適格性の主張には PostgreSQL と独立検証可能な厳密な証拠が必要です。

## ワークスペースクイックスタート

現在の HartMesh checkout のリポジトリルートで作業してください。

このプレビューには Python 3.12+、Node.js 22+、pnpm または Corepack、`uv`、GNU Make、nginx、Docker または Apple Container、モデル認証情報、約 4 CPU core と 8 GB RAM が必要です。Windows のローカル開発では Git Bash を使います。

`make setup` が実行モードを尋ねたら **Container sandbox** を選択してください。`LocalSandboxProvider` で永続呼び出しを使えるのは有効スキル集合が明示的に空の場合だけですが、この checkout は組み込みスキルを既定で有効にします。

> [!WARNING]
> `make dev` は信頼済みネットワーク向けの開発パスです。Gateway `8001`、frontend `3000`、ローカル nginx `2026` はホストのワイルドカードで listen します。最初の admin 設定中は、信頼済みまたはホスト firewall で保護されたマシンだけで実行してください。

設定、診断、起動：

```bash
make check
make setup
make doctor
make dev
```

`make setup` は gitignore 対象のローカル設定を書き込みます。`make dev` はツール確認を再実行し、依存関係を同期して Gateway、frontend、nginx を起動します。

pre-commit hook も必要な contributor は `make install` を任意で実行できます。

[http://localhost:2026](http://localhost:2026) を開きます。新規インストールでは最初の admin 設定を完了し、thread を作成して prompt を送信してください。

成功すると、設定したモデルの応答が Gateway 管理の run ライフサイクルを通じてワークスペースへ stream されます。

別の端末から停止：

```bash
make stop
```

これは継承したワークスペースとローカル stack の評価です。HartMesh が `run_id` を保持する動作はランタイムパスで確認してください。ワークスペースの成功だけでは PostgreSQL 永続性や実 Kubernetes 適格性を証明しません。

## Durable Invocation HTTP API

信頼済みバックエンドサービスが `ensure → observe` を必要とする場合は、プレリリースの `/api/runtime/v1` サーフェスを使います。

標準ライブラリのみの [`deerflow-runtime-api`](backend/packages/runtime-api/README.md) にある厳密なレコードを使用します。

ブラウザワークスペースは不要ですが、設定済み HartMesh checkout は必要です。初めて評価する場合はリポジトリルートで実行します：

```bash
make check
make setup
make doctor
```

`make setup` で選んだモデルの認証情報を設定します。ワークスペース設定済みなら、この三つは省略できます。

初回ランタイム評価でも **Container sandbox** を選んでください。Local provider の永続実行には空の有効スキル集合が必要です。

前述のとおり、`make dev` は `8001`、`3000`、`2026` をホストのワイルドカードで listen します。信頼済みまたはホスト firewall で保護されたマシンを使ってください。

ルート `.env` に空でない `DEER_FLOW_INTERNAL_AUTH_TOKEN` がある場合は、その値をクライアント端末で使います。

空の代入は削除またはコメントアウトしてください。`.env` の読み込みが下記 shell export を上書きします。それ以外は HartMesh 起動前に token を生成します：

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Copy this token into the trusted client terminal:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

二つ目の端末で表示された token を export し、標準ライブラリだけのクライアントを実行します：

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

新しいキーは `201 created` を返し、同一リプレイは同じ `run_id` と `200 known` を返します。キーを維持して message を変えると、同じキーで別処理を始めず型付き `409 conflict` を返します。

この例は組み込みの `gateway-internal` サービスを使い、人間の委任を意図的に省略しています。内部 token をブラウザや信頼できないクライアントへ公開しないでください。

ランタイム固有の owner 委任は、既存のローカル user に対して `X-DeerFlow-Owner-User-Id` を再検証します。

[principal projection](backend/app/gateway/services.py) と [identity test](backend/tests/test_invocation_identity_separation.py) を参照してください。

DTO、cursor paging、型付き failure、フェンス付き cancellation は、[ランタイム契約](backend/packages/runtime-api/README.md)と [HTTP リファレンス](backend/docs/API.md#durable-invocation-runtime-api)を参照してください。

clarification の回答は、同じ DeerFlow thread 上で**新しい呼び出し**を開始します。

評価完了後、リポジトリルートから HartMesh を停止します：

```bash
make stop
```

## 運用上の価値

### リプレイセーフな受理

安定した外部キーと完全な正規 caller intent は、一つの保持済み呼び出しへ収束します。同じ意図ならどのライフサイクル状態でもその行を返し、意図が変われば競合します。worker を付けるのは creator だけです。

この保証は通常の run 行が保持されている間だけ有効で、任意の外部副作用を重複排除しません。

証拠：[`idempotency.py`](backend/app/runtime/idempotency.py) と [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py)。

### 一つの受理境界

HTTP create/stream/wait ルート、スケジュールタスク、認証済みネイティブチャネル、組み込みサービスは同じ `InvocationRuntime` に入ります。ソース認証、acknowledgement、ingress 永続性はソース固有です。

証拠：[`invocation.py`](backend/app/runtime/invocation.py) と [closure matrix](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix)。

### 固定された受理済み実行素材

受理時に agent revision、extension generation、trusted context、constraint evidence、有効 skill package、execution/projection profile を固定します。

同じ不変の受理済み skill tree を lead/subagent prompt、activation、policy、sandbox read が共有します。

空でない受理済み skill は、対応する accepted-only profile、つまりローカル container-backed AIO または fenced `rwx_verified_copy_v2` projection を持つ Kubernetes を使います。

`LocalSandboxProvider`、E2B、custom、その他 remote profile は empty-only のままです。offline projection evidence は実クロスノード適格性を証明しません。

証拠：[`runtime/` ソース](backend/packages/harness/deerflow/runtime/)の `accepted_invocation.py`、`agent_revision.py`、`skill_snapshot.py`。

リモート証拠：[`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) と [projection test](backend/tests/test_kubernetes_accepted_skill_projection.py)。

### 実行に追随するポリシー

HartMesh は effective subject、acting service、source evidence を分離します。

operator が有限の observation scope を付与しない限り、認証済み service は owner scope に限定されます。grant は discovery を制限し、現在の認可が返せる内容を決めます。cancellation は grant を継承しません。

operator が invocation-operation authorization を有効にするか authoritative v2 constraint を設定すると、その指定操作は fail closed になります。必須 capability health と MCP preparation も同様です。

authorization と invocation-operation control は既定で無効、`required_capabilities` は既定で空です。

任意の observational middleware と旧 API-writable MCP interceptor は、fail-open または警告して skip する動作を維持します。

owner と route の check は残りますが、HartMesh は汎用 organization policy や任意の third-party tool を保証しません。

証拠：[extension contract](backend/packages/extension-api/README.md)、[authorization](backend/app/runtime/authorization.py)、[constraints](backend/app/runtime/constraints.py)、[visibility](backend/app/runtime/visibility.py)。

### ポータブルなランタイム統合

標準ライブラリのみの `deerflow-runtime-api` は厳密で不変な record と一つの `DurableInvocationPort` を定義します：`ensure`、呼び出し/context の `observe`、フェンス付き `control`、`capabilities`。

認証済み HTTP とアプリケーション内 adapter は、その record と conformance suite を共有します。同期 `DeerFlowClient` は durable adapter ではなく、v1 は broker push、context export、context retirement を提供しません。

証拠：[runtime package](backend/packages/runtime-api/README.md) と [transport conformance](backend/tests/test_runtime_api_conformance.py)。

### トランザクション単位のライフサイクル整合性

SQL store では、状態変更と安全な lifecycle event を同じ state version で atomic commit します。有界観測は authoritative snapshot を使い、pruned、future、不整合な履歴に型付き結果を返します。

PostgreSQL repeatable-read と cross-session の動作は、外部 PostgreSQL gate が通過した場合だけリリース主張にできます。

証拠：[`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) と [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py)。

契約テスト：[atomic lifecycle store](backend/tests/test_invocation_lifecycle_store.py) と [typed lifecycle query](backend/tests/test_invocation_lifecycle_query.py)。

### 永続的な署名付き GitHub ingress

HMAC 検証と PostgreSQL receipt により、署名付き GitHub ingress は acknowledgement 前に有界 source binding を永続化します。

lease と fence は中断後に期限切れ lease を回収し、混雑 thread の FIFO を保ち、同じ受理済み呼び出しへ収束できます。

この主張は PostgreSQL を使う検証済み署名付き GitHub ingress に限ります。その他とローカルチャネルは best-effort のままです。

証拠：[receipt store](backend/app/channels/inbound_receipts.py) と [receipt test](backend/tests/test_durable_inbound_receipts.py)。

## デプロイと永続性の境界

デプロイガイドを実行する前に、次の制限を確認してください：

- 検証済みトポロジーは Gateway 1 replica です。
- active-active Gateway HA、スケジューラー HA、zero-downtime rollout は主張しません。
- リプレイセーフな受理は外部副作用の汎用 exactly-once ではありません。
- durable invocation storage では process-loss recovery が authoritative terminal evidence を保持しますが、すべての graph/tool call を透過的に再開しません。
- Memory は process-local、SQLite は invocation state の node-durable、PostgreSQL は shared-durable store です。
- Memory/SQLite の native-channel ingress は best-effort です。
- durable native ingress は現在、PostgreSQL を使う検証済み署名付き GitHub 配送を指します。
- 空でない受理済み skill には対応する accepted-only sandbox path が必要です。
- Kubernetes/PostgreSQL 適格性には、指定 image、chart、configuration、schema、topology、scope、scenario に対する厳密な合格証拠が必要です。
- 収集または skip された opt-in gate は合格ではありません。

監査した source snapshot では、外部 PostgreSQL/Kubernetes opt-in gate は未設定で、厳密な合格 qualification artifact もありませんでした。

これは未通過の release gate であり、実装済みの動作が存在しないという証拠ではありません。

| モード | レポートされる境界 |
| --- | --- |
| `local_development` | 永続性を主張せず process-local state を許可。 |
| `durable_production` | startup/readiness で process-local invocation state を拒否。 |
| Helm `local_evaluation` | 1 Gateway 評価の既定値。明示的に unqualified。 |
| Helm `durable_one_replica` | digest-pinned image、PostgreSQL/shared state、安全な probe/shutdown timing が必要。厳密な合格証拠なしでは unqualified。 |

administrator deployment report は persistence tier、health、provenance、qualification を分離します。

指定 qualification reference は `operator_asserted` のままです。offline verifier だけが厳密な証拠に対して `external_evidence_verified` を確立できます。portable capabilities は deployment claim を含みません。

`GET /health` は process liveness、`GET /ready` は有界な ready/not-ready を報告します。administrator は `GET /api/runtime/v1/deployment` で persistence と qualification を確認します。

証拠：[deployment report](backend/app/runtime/deployment.py)、[qualification verification](backend/scripts/verify_qualification_evidence.py)、[Helm deployment contract](deploy/helm/deer-flow/README.md)。

durable invocation storage で active invocation が process とともに失われると、recovery は `stop_reason=orphan_recovered` などの authoritative terminal evidence を記録します。

同一リプレイはその保持済み terminal run を返します。製品意図を続けるには、新しい process generation で新しい呼び出しが必要です。

[authoritative lifecycle and failure recovery](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery) を参照してください。

### セキュリティ境界

Compose stack は nginx だけを publish し、既定で `127.0.0.1:2026` に bind します。ローカル `make dev` は `8001`、`3000`、`2026` をホストのワイルドカードで listen するため、同じ published-port fence があると考えないでください。

HartMesh エージェントは、設定された tool が許す command 実行と file read/write が可能です。分離は provider 次第で、`LocalSandboxProvider` は Gateway の host identity を共有し、OS isolation boundary ではありません。

command classification と path rewrite は defense in depth です。信頼できない処理には対応する isolated provider を使ってください。

loopback を越えてサービスへ到達可能にする前に、最初の admin 設定を完了してください。

administrator は stdio MCP process と信頼済み Python plugin を設定できるため、administrator access は code execution と同等です。

1 Gateway render contract、受理済み skill projection、credential、厳密な qualification 手順は [Helm deployment guide](deploy/helm/deer-flow/README.md)を参照してください。

## 拡張モデル

HartMesh は DeerFlow の skill、tool、MCP server、custom agent、middleware model を維持します。

host-independent な [`deerflow-extension-api`](backend/packages/extension-api/README.md) は authorization、identity、trusted-context contribution の型付き契約を追加します。

restrictive constraint、capability health、required MCP preparation も対象です。

Python plugin は信頼済み operator code で、`config.yaml` の top-level `plugins:` から startup 時に読み込みます。この list は意図的に API-writable な `extensions_config.json` の外にあり、後者が MCP と skill configuration を管理します。

受理済み呼び出しは startup-frozen extension generation を固定します。skill 変更は後続受理に反映され、plugin 変更は新 generation を作る Gateway restart が必要です。どちらも受理済み処理を変更しません。

## 互換性、上流ベースライン、リリース状況

HartMesh は既存の `deerflow.*` namespace、package 名、`DEER_FLOW_*` 変数、Docker/Helm identifier、filesystem path、Gateway compatibility surface を維持します。

製品比較は固定ローカル範囲 `e16ef2969b1446162e19af7bdde1446674851e66...ca2400f3059b3ac93249473e97ed83c5296fb0f0` です。

2026-08-09 のリポジトリ監査時、別に確認した `deerflow/main` snapshot は `e401ae2d7b8e4fc73fc82a1143c989c54f3f4de6` で、共有 base より upstream-only commit が一つありました。

これは背景情報で、上記 baseline ではありません。HartMesh は将来も上流より優れるという主張をしません。

この repository は HartMesh の sync cadence、API/config/database compatibility window、support window、security-fix intake policy、upstream contribution policy をまだ記載していません。

これらの hash は provenance であり、maintenance promise ではありません。

この checkout の Alembic graph は線形です：`0011_mcp_tasks` → `0011_accepted_invocation` → `0019_inbound_event_identity` までの invocation migration。

PostgreSQL operator は rollback 前に writer を停止し data を backup してください。migration guidance は [backend/AGENTS.md](backend/AGENTS.md) にあります。

version source は `2.1.0` を示しますが、監査した HartMesh 実装を含む tag はありません。version string は HartMesh release を確立しません。

[RELEASING.md](RELEASING.md) は継承した DeerFlow tag mechanics であり、HartMesh 独自の release channel ではありません。

## ドキュメント

- [Durable invocation runtime](backend/docs/INVOCATION_RUNTIME.md) — 保証、証拠、recovery、deferred scope
- [Runtime API](backend/packages/runtime-api/README.md) — DTO と `DurableInvocationPort`
- [Gateway API](backend/docs/API.md) — 認証済み HTTP 動作
- [Extension API](backend/packages/extension-api/README.md) — policy と trust boundary
- [Helm deployment](deploy/helm/deer-flow/README.md) — 1 Gateway mode と qualification
- [Configuration](config.example.yaml) — operator setting
- [Backend guide](backend/AGENTS.md) と [frontend guide](frontend/AGENTS.md) — architecture と test

## サポートとセキュリティ

リポジトリルートでローカル診断を実行します：

```bash
make doctor
make support-bundle
```

生成された support material は共有前に確認してください。

この repository は HartMesh 独自の issue tracker、release channel、非公開 vulnerability-reporting route をまだ記載していません。

[CONTRIBUTING.md](CONTRIBUTING.md) と [SECURITY.md](SECURITY.md) は ByteDance DeerFlow の上流 destination を保持しています。HartMesh 所有の support ではありません。

credential、token、private prompt、customer data、vulnerability detail を public issue に載せないでください。internal token、webhook secret、provider key、database credential は secret として扱います。

## コントリビュート

ローカル作業では [CONTRIBUTING.md](CONTRIBUTING.md) の継承済み convention と、最寄りの [AGENTS.md](AGENTS.md) にある repository command/module ownership に従ってください。

## ライセンス

HartMesh は DeerFlow の [MIT License](LICENSE) と既存 notice を維持します。

## 謝辞

HartMesh は ByteDance と DeerFlow contributor が公開したエージェント基盤の上に成り立っています。LangChain、LangGraph、より広いオープンソースエージェント ecosystem にも感謝します。

HartMesh の下流運用主張、release status、qualification、support boundary は HartMesh 自身の責任です。
