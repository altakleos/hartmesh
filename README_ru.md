# HartMesh

[English](README.md) | [简体中文](README_zh.md) | [日本語](README_ja.md) | [Français](README_fr.md) | Русский

## Надёжный слой исполнения для агентов DeerFlow.

HartMesh — эксплуатационный дистрибутив DeerFlow с безопасным повтором вызовов, применимыми политиками, проверяемыми свидетельствами жизненного цикла и гарантиями развёртывания с явной областью действия.

Он построен на workspace, sandbox, памяти, skills, инструментах, subagents, расписаниях и каналах DeerFlow.

> [!IMPORTANT]
> **Статус: предварительная версия.** В этом дереве исходников реализован runtime и есть офлайн-свидетельства контрактов, но пока ни один тег релиза HartMesh их не содержит.
>
> Точная квалификация реального развёртывания остаётся отдельным барьером, привязанным к конкретному артефакту.

HartMesh — независимый downstream-дистрибутив [DeerFlow](https://github.com/bytedance/deer-flow) от ByteDance. Это не официальный релиз DeerFlow; проект не связан с ByteDance и не одобрен ею.

[**Оценить предварительную версию**](#быстрый-старт-workspace) · [**Изучить свидетельства**](backend/docs/INVOCATION_RUNTIME.md) · [**Посмотреть контракт runtime**](backend/packages/runtime-api/README.md)

## Зачем нужен HartMesh

Агента легко запустить и трудно эксплуатировать.

Клиенты повторяют запросы, политики меняются, skills обновляются, процессы завершаются с ошибкой, а нескольким сервисам может понадобиться наблюдать одну и ту же работу.

HartMesh добавляет вокруг DeerFlow надёжную границу вызова, чтобы принятую работу можно было согласованно повторять, контролировать политиками, проверять и останавливать.

В одной аутентифицированной области, пока обычная строка run сохранена, повтор того же строгого запроса с тем же внешним ключом возвращает сохранённый `run_id`.

Изменение канонического намерения исполнения под этим ключом вызывает конфликт.

Это безопасный повтор приёма, а не гарантия exactly-once для модели, инструментов, провайдеров или иных внешних побочных эффектов.

HartMesh в первую очередь предназначен для:

- разработчиков платформ, встраивающих работу DeerFlow в API, сервисы, расписания или каналы;
- операторов, оценивающих управляемую долговечную топологию с одним Gateway; и
- команд, автоматизирующих ценные запланированные и подписанные GitHub workflows.

## Построен на DeerFlow, усилен для эксплуатации

DeerFlow предоставляет основу агента: workspace, LangGraph harness, sandboxes, память, skills, инструменты, subagents, расписания и нативные каналы.

HartMesh сохраняет этот опыт и добавляет control plane для приёма, управления политиками, наблюдения и контроля длительной работы.

Ниже сравниваются фиксированные снимки. Точные commits указаны в разделе [совместимость и происхождение](#совместимость-базовая-версия-upstream-и-статус-релиза); это не утверждение обо всех будущих upstream-релизах.

| Что сохраняется из базовой версии | Что добавляет HartMesh |
| --- | --- |
| Workspace, harness, память, sandboxes, skills, инструменты, subagents, расписания и каналы | Одна учитывающая источник граница приёма; долговечность доставки по-прежнему зависит от транспорта |
| Жизненный цикл thread/run DeerFlow, REST-маршруты Gateway и совместимые с LangGraph маршруты | Канонические внешние ключи в области источника, сохранённая идентичность приёма и явный конфликт при изменении намерения |
| Конфигурация агентов, расширений и sandbox | Зафиксированный материал принятого исполнения |
| Gateway и встроенные поверхности интеграции | Строгие записи `ensure`, `observe` и ограждённого `control` |
| Локальная эксплуатация и Helm | Свидетельства жизненного цикла и явные отчёты о хранении, топологии и квалификации |

Вы сохраняете основу агентов и совместимую поверхность DeerFlow. Вы получаете control plane вызовов со свидетельствами.

Вы не получаете active-active Gateway HA, HA планировщика, универсальное продолжение после сбоя или exactly-once для внешних побочных эффектов.

## Что произойдёт, если…?

| Сценарий | Поведение HartMesh |
| --- | --- |
| [Клиент повторяет запрос после потери ответа](backend/tests/test_invocation_idempotency.py) | Одинаковое каноническое намерение возвращает принятый run; изменённое намерение вызывает конфликт. |
| [Skill меняется после приёма](backend/tests/test_accepted_skill_snapshots.py) | Принятый вызов сохраняет одно снятое дерево skills для агента и sandbox; изменения видит только последующая работа. |
| [Удалённый sandbox исполняет принятые skills](backend/tests/test_kubernetes_accepted_skill_projection.py) | Поддерживаемая проекция `rwx_verified_copy_v2` связывает и повторно проверяет принятый skill и свидетельства изоляции до работы graph/model; реальная межузловая квалификация требует отдельного точного артефакта. |
| [Сервис действует за человека или наблюдает другого владельца](backend/tests/test_service_observation_grants.py) | Человеческий субъект, действующий сервис и свидетельства источника остаются раздельными; межвладельческое наблюдение требует конечного grant и текущей авторизации. |
| [Обязательная policy capability неисправна](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | Readiness и действительно новый приём закрываются при ошибке; одинаковый повтор использует запечатанные свидетельства приёма, но раскрытие всё равно регулирует текущая авторизация наблюдения. |
| [Оператор спрашивает, что долговечно или квалифицировано](backend/app/runtime/deployment.py) | Отчёт называет уровень хранения; объявленная ссылка остаётся утверждением оператора, и только точное свидетельство, принятое [офлайн-верификатором](backend/scripts/verify_qualification_evidence.py), подтверждает независимую квалификацию. |
| [История жизненного цикла урезана или противоречива](backend/tests/test_invocation_lifecycle_query.py) | Ограниченное наблюдение возвращает типизированный результат курсора или целостности вместо молчаливого показа недействительной истории. |
| [Подписанная доставка GitHub прервана или thread занят](backend/tests/test_durable_inbound_receipts.py) | Восстановление receipt в PostgreSQL может вернуть истёкший lease, сохранить FIFO-отсрочку и сойтись к тому же принятому run. |

<!-- Будущее демо: добавить 30–60-секундную запись терминала с вызовом по ключу, имитацией потерянного ответа, тем же run_id при повторе, конфликтом изменённого намерения и наблюдением жизненного цикла. -->

## Выберите путь

| Путь | Для кого | Первое доказательство |
| --- | --- | --- |
| [Workspace](#быстрый-старт-workspace) | Разработчики, оценивающие полный workspace DeerFlow с контролями HartMesh | Ответ модели через единый локальный вход |
| [Интеграция runtime](#durable-invocation-http-api) | Платформы, встраивающие работу агентов в сервисы, расписания или каналы | Запросы с одинаковым ключом возвращают один `run_id`, после чего жизненный цикл можно наблюдать |

Операторы, оценивающие управляемое развёртывание, могут сразу перейти к [границам развёртывания и долговечности](#границы-развёртывания-и-долговечности).

> [!CAUTION]
> **Изучите границу до развёртывания.**
>
> - Проверенная топология содержит ровно одну реплику Gateway.
> - Не заявляются active-active Gateway HA, HA планировщика или rollout без простоя.
> - Безопасный повтор приёма не означает exactly-once внешних эффектов или универсальное продолжение после сбоя.
> - Локальный ingress каналов с Memory и SQLite остаётся best-effort.
> - Для заявлений о shared-durable и квалификации ниже нужны PostgreSQL и точные независимо проверяемые свидетельства.

## Быстрый старт workspace

Работайте из корня текущего checkout HartMesh.

Для предварительной оценки нужны Python 3.12+, Node.js 22+, pnpm или Corepack, `uv`, GNU Make, nginx, Docker или Apple Container, учётные данные модели, примерно 4 CPU core и 8 ГБ RAM. Локальная разработка в Windows использует Git Bash.

Когда `make setup` спросит режим исполнения, выберите **Container sandbox**. Долговечные вызовы через `LocalSandboxProvider` работают только с явно пустым эффективным набором skills; в этом checkout встроенные skills включены по умолчанию.

> [!WARNING]
> `make dev` предназначен для доверенной сети: Gateway `8001`, frontend `3000` и локальный nginx `2026` слушают все интерфейсы хоста. Во время настройки первого admin запускайте его только на доверенной или защищённой host firewall машине.

Настройте, проверьте и запустите:

```bash
make check
make setup
make doctor
make dev
```

`make setup` записывает локальную конфигурацию, исключённую из Git. `make dev` повторно проверяет инструменты, синхронизирует зависимости и запускает Gateway, frontend и nginx.

`make install` необязателен и нужен contributor, которым также требуются pre-commit hooks.

Откройте [http://localhost:2026](http://localhost:2026). В новой установке завершите настройку первого admin, создайте thread и отправьте prompt.

Успех означает, что workspace потоково показывает ответ настроенной модели через управляемый Gateway жизненный цикл run.

Остановите stack из другого терминала:

```bash
make stop
```

Этот путь оценивает унаследованный workspace и локальный stack. Перейдите к runtime-пути, чтобы проверить сохранение `run_id`; успех workspace сам по себе не доказывает долговечность PostgreSQL или реальную квалификацию Kubernetes.

## Durable Invocation HTTP API

Используйте предварительную поверхность `/api/runtime/v1`, когда доверенному backend-сервису нужны `ensure → observe`.

Она использует строгие записи из [`deerflow-runtime-api`](backend/packages/runtime-api/README.md), зависящего только от стандартной библиотеки.

Браузерный workspace не нужен, но checkout HartMesh должен быть настроен. При первой оценке выполните из корня репозитория:

```bash
make check
make setup
make doctor
```

Настройте учётные данные модели, выбранной в `make setup`. Если workspace уже настроен, пропустите эти команды.

При первой runtime-оценке также выберите **Container sandbox**. Для долговечного исполнения Local provider требует пустой эффективный набор skills.

Как указано выше, `make dev` слушает все интерфейсы хоста на портах `8001`, `3000` и `2026`; используйте доверенную или защищённую host firewall машину.

Если корневой `.env` уже задаёт непустой `DEER_FLOW_INTERNAL_AUTH_TOKEN`, используйте это значение в клиентском терминале.

Удалите или закомментируйте пустое присваивание: загрузка `.env` переопределяет shell export ниже. В противном случае создайте token перед запуском HartMesh:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Copy this token into the trusted client terminal:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

Во втором терминале экспортируйте напечатанный token и запустите клиент, использующий только стандартную библиотеку:

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

Новый ключ возвращает `201 created`; идентичный повтор — `200 known` с тем же `run_id`. Если оставить ключ и изменить сообщение, вернётся типизированный `409 conflict`, а не другая работа под одним ключом.

Пример аутентифицирует встроенный сервис `gateway-internal` и намеренно не использует делегирование человека. Не передавайте внутренний token браузеру или недоверенному клиенту.

Специфичное для runtime делегирование владельца повторно проверяет `X-DeerFlow-Owner-User-Id` по существующему локальному пользователю.

См. [проекцию principal](backend/app/gateway/services.py) и [тесты идентичности](backend/tests/test_invocation_identity_separation.py).

DTO, cursor paging, типизированные ошибки и ограждённая отмена описаны в [контракте runtime](backend/packages/runtime-api/README.md) и [HTTP-справочнике](backend/docs/API.md#durable-invocation-runtime-api).

Ответ на уточнение запускает **новый вызов в том же thread DeerFlow**.

После оценки остановите HartMesh из корня репозитория:

```bash
make stop
```

## Эксплуатационная ценность

### Безопасный повтор приёма

Стабильный внешний ключ и полное каноническое намерение вызывающего сходятся к одному сохранённому вызову. Одинаковое намерение возвращает строку в любом состоянии, изменённое конфликтует, а worker присоединяет только создатель.

Гарантия действует, пока обычная строка run сохранена. Она не дедуплицирует произвольные внешние побочные эффекты.

Свидетельства: [`idempotency.py`](backend/app/runtime/idempotency.py) и [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py).

### Одна граница приёма

HTTP-маршруты create, stream и wait, задачи, аутентифицированные нативные каналы и встроенные сервисы входят в один `InvocationRuntime`. Аутентификация, подтверждение и долговечность ingress зависят от источника.

Свидетельства: [`invocation.py`](backend/app/runtime/invocation.py) и [матрица завершённости](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix).

### Зафиксированный материал принятого исполнения

Приём фиксирует revision агента, generation расширений, доверенный контекст, свидетельства ограничений, эффективные пакеты skills и профиль execution/projection.

Одно неизменяемое дерево принятых skills используется prompt ведущего и subagent, активацией, policy и чтением sandbox.

Непустые принятые skills используют поддерживаемый accepted-only профиль: локальный container-backed AIO или Kubernetes с ограждённой проекцией `rwx_verified_copy_v2`.

`LocalSandboxProvider`, E2B, custom и другие remote-профили остаются empty-only. Офлайн-свидетельства проекции не подтверждают реальную межузловую квалификацию.

Свидетельства: исходники принятого исполнения в [`runtime/`](backend/packages/harness/deerflow/runtime/) — `accepted_invocation.py`, `agent_revision.py` и `skill_snapshot.py`.

Удалённые свидетельства: [`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) и [тесты проекции](backend/tests/test_kubernetes_accepted_skill_projection.py).

### Политика следует за исполнением

HartMesh разделяет effective subject, acting service и source evidence.

Аутентифицированные сервисы ограничены owner scope, пока оператор не выдаст конечную область наблюдения. Grant ограничивает обнаружение; текущая авторизация решает, что вернуть, и отмена не наследует grant.

Когда оператор включает авторизацию операций вызова или настраивает authoritative v2 constraints, названные операции закрываются при ошибке. То же относится к обязательным capability health и MCP preparation.

Авторизация и контроль операций вызова отключены по умолчанию, а `required_capabilities` по умолчанию пуст.

Необязательный observational middleware и старый изменяемый через API MCP interceptor сохраняют fail-open или warning-and-skip.

Проверки owner и route остаются, но HartMesh не поставляет универсальную policy организации и не гарантирует произвольные third-party tools.

Свидетельства: [контракт расширений](backend/packages/extension-api/README.md), [авторизация](backend/app/runtime/authorization.py), [ограничения](backend/app/runtime/constraints.py) и [видимость](backend/app/runtime/visibility.py).

### Переносимая интеграция runtime

`deerflow-runtime-api` только на стандартной библиотеке определяет строгие неизменяемые записи и один `DurableInvocationPort`: `ensure`, `observe` вызова/контекста, ограждённый `control` и `capabilities`.

Аутентифицированный HTTP и встроенный в приложение adapter используют одни записи и conformance suite. Синхронный `DeerFlowClient` не является долговечным adapter; v1 не предоставляет broker push, export или retirement контекста.

Свидетельства: [пакет runtime](backend/packages/runtime-api/README.md) и [соответствие transport](backend/tests/test_runtime_api_conformance.py).

### Транзакционная целостность жизненного цикла

С SQL store состояние и безопасное событие фиксируются атомарно под одной версией. Ограниченное наблюдение использует authoritative snapshot и возвращает типизированные результаты для урезанной, будущей или противоречивой истории.

PostgreSQL repeatable-read и межсессионное поведение можно заявлять для релиза только после прохождения внешнего PostgreSQL gate.

Свидетельства: [`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) и [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py).

Контрактные тесты: [атомарный store жизненного цикла](backend/tests/test_invocation_lifecycle_store.py) и [типизированные запросы](backend/tests/test_invocation_lifecycle_query.py).

### Долговечный подписанный GitHub ingress

С проверкой HMAC и receipts PostgreSQL подписанный GitHub ingress сохраняет ограниченные привязки источника до подтверждения.

Leases и fences могут вернуть истёкший lease после прерывания, сохранить FIFO занятого thread и сойтись к тому же принятому вызову.

Заявление ограничено проверенным подписанным GitHub ingress с PostgreSQL; другие и локальные каналы остаются best-effort.

Свидетельства: [store receipts](backend/app/channels/inbound_receipts.py) и [тесты receipts](backend/tests/test_durable_inbound_receipts.py).

## Границы развёртывания и долговечности

Прочитайте эти ограничения до следования любому руководству:

- Проверенная топология содержит ровно одну реплику Gateway.
- Не заявляются active-active Gateway HA, HA планировщика или rollout без простоя.
- Безопасный повтор приёма не означает универсальное exactly-once внешних эффектов.
- С долговечным хранилищем вызовов восстановление после потери процесса сохраняет authoritative terminal evidence, но не продолжает прозрачно каждый graph или tool call.
- Memory является process-local, SQLite — node-durable для состояния вызова, PostgreSQL — shared-durable store.
- Ingress нативных каналов с Memory и SQLite остаётся best-effort.
- Долговечный нативный ingress сейчас означает проверенную подписанную GitHub-доставку с PostgreSQL.
- Непустые принятые skills требуют поддерживаемого accepted-only пути sandbox.
- Квалификация Kubernetes/PostgreSQL требует точного прошедшего свидетельства для названных image, chart, configuration, schema, topology, scope и сценариев.
- Собранный или пропущенный opt-in gate не является прохождением.

Для проверенного снимка исходников внешние PostgreSQL/Kubernetes opt-in gates не были настроены, и точный прошедший артефакт квалификации отсутствовал.

Это непройденные release gates, а не свидетельство отсутствия реализованного поведения.

| Режим | Заявленная граница |
| --- | --- |
| `local_development` | Разрешает process-local state без заявления о долговечности. |
| `durable_production` | Отклоняет process-local состояние вызовов при старте и readiness. |
| Helm `local_evaluation` | Настройки оценки с одним Gateway; явно unqualified. |
| Helm `durable_one_replica` | Требует images по digest, PostgreSQL/shared state, безопасные probes и shutdown timing; остаётся unqualified без точного прошедшего свидетельства. |

Административный отчёт разделяет уровень хранения, здоровье, происхождение и квалификацию.

Указанная ссылка квалификации остаётся `operator_asserted`; только офлайн-верификатор может установить `external_evidence_verified` по точному свидетельству. Переносимые capabilities не несут заявлений о развёртывании.

`GET /health` сообщает о жизни процесса. `GET /ready` — ограниченный сигнал ready/not-ready. Администраторы проверяют хранение и квалификацию через `GET /api/runtime/v1/deployment`.

Свидетельства: [отчёт о развёртывании](backend/app/runtime/deployment.py), [проверка квалификации](backend/scripts/verify_qualification_evidence.py) и [контракт Helm](deploy/helm/deer-flow/README.md).

С долговечным хранилищем, если активный вызов потерян вместе с процессом, восстановление записывает authoritative terminal evidence, например `stop_reason=orphan_recovered`.

Идентичный повтор возвращает этот сохранённый терминальный run. Чтобы продолжить продуктовое намерение, нужен новый вызов в новом generation процесса.

См. [authoritative lifecycle и восстановление](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery).

### Граница безопасности

Compose stacks публикуют только nginx на `127.0.0.1:2026` по умолчанию. Локальный `make dev` слушает все интерфейсы на `8001`, `3000` и `2026`; не считайте, что у него такая же защита порта.

Агенты HartMesh могут выполнять команды и читать или писать файлы, разрешённые настроенными инструментами. Изоляция зависит от provider: `LocalSandboxProvider` разделяет host identity Gateway и не является границей изоляции ОС.

Классификация команд и переписывание путей — defense in depth. Для недоверенной работы используйте поддерживаемый изолированный provider.

Завершите настройку первого admin до того, как сервис станет доступен за пределами loopback.

Администраторы могут настраивать stdio MCP processes и доверенные Python plugins, поэтому административный доступ эквивалентен исполнению кода.

Контракт одного Gateway, проекция принятых skills, credentials и точная процедура квалификации описаны в [руководстве Helm](deploy/helm/deer-flow/README.md).

## Модель расширений

HartMesh сохраняет модель DeerFlow для skills, инструментов, MCP servers, custom agents и middleware.

Независимый от host пакет [`deerflow-extension-api`](backend/packages/extension-api/README.md) добавляет типизированные контракты авторизации, идентичности и вклада в доверенный контекст.

Он также охватывает ограничивающие constraints, здоровье capabilities и обязательную подготовку MCP.

Python plugins — доверенный код оператора, загружаемый при старте из верхнеуровневого `plugins:` в `config.yaml`. Список намеренно находится вне изменяемого через API `extensions_config.json`, который управляет MCP и skills.

Принятый вызов фиксирует замороженный при запуске generation расширений. Изменения skills влияют на последующий приём; изменения plugins требуют перезапуска Gateway. Ни то, ни другое не меняет уже принятую работу.

## Совместимость, базовая версия upstream и статус релиза

HartMesh сохраняет существующие namespaces `deerflow.*`, имена пакетов, переменные `DEER_FLOW_*`, идентификаторы Docker/Helm, filesystem paths и совместимые поверхности Gateway.

Продукт сравнивается по фиксированному локальному диапазону `e16ef2969b1446162e19af7bdde1446674851e66...ca2400f3059b3ac93249473e97ed83c5296fb0f0`.

При аудите репозитория 2026-08-09 отдельно проверенный snapshot `deerflow/main` был `e401ae2d7b8e4fc73fc82a1143c989c54f3f4de6`, на один upstream-only commit дальше общей базы.

Это контекст, а не указанная выше базовая версия; HartMesh не заявляет постоянное превосходство над upstream.

Репозиторий пока не документирует cadence синхронизации HartMesh, окно совместимости API/config/database, период поддержки, порядок приёма security fixes или политику upstream contributions.

Эти hashes подтверждают происхождение, а не обещание сопровождения.

Граф Alembic в этом checkout линеен: `0011_mcp_tasks` → `0011_accepted_invocation` → миграции вызовов до `0019_inbound_event_identity`.

Операторам PostgreSQL следует остановить writers и сделать backup перед rollback; руководство по миграциям находится в [backend/AGENTS.md](backend/AGENTS.md).

Источники версии указывают `2.1.0`, но ни один tag не содержит проверенную реализацию HartMesh; строка версии не устанавливает релиз HartMesh.

[RELEASING.md](RELEASING.md) описывает унаследованную механику тегов DeerFlow, а не собственный канал релизов HartMesh.

## Документация

- [Долговечный runtime вызовов](backend/docs/INVOCATION_RUNTIME.md) — гарантии, свидетельства, восстановление и отложенная область
- [Runtime API](backend/packages/runtime-api/README.md) — DTO и `DurableInvocationPort`
- [Gateway API](backend/docs/API.md) — аутентифицированное HTTP-поведение
- [Extension API](backend/packages/extension-api/README.md) — политики и границы доверия
- [Развёртывание Helm](deploy/helm/deer-flow/README.md) — режимы с одним Gateway и квалификация
- [Конфигурация](config.example.yaml) — настройки оператора
- [Руководство backend](backend/AGENTS.md) и [руководство frontend](frontend/AGENTS.md) — архитектура и тесты

## Поддержка и безопасность

Запустите локальную диагностику из корня:

```bash
make doctor
make support-bundle
```

Проверьте созданные материалы перед отправкой.

Репозиторий пока не документирует собственный issue tracker, канал релизов или закрытый маршрут сообщения об уязвимостях HartMesh.

[CONTRIBUTING.md](CONTRIBUTING.md) и [SECURITY.md](SECURITY.md) сохраняют upstream-направления ByteDance DeerFlow; это не собственная поддержка HartMesh.

Не публикуйте credentials, tokens, частные prompts, данные клиентов или детали уязвимостей. Считайте внутренние tokens, webhook secrets, provider keys и database credentials секретами.

## Участие в разработке

Для локальной работы следуйте унаследованным правилам в [CONTRIBUTING.md](CONTRIBUTING.md) и ближайшем [AGENTS.md](AGENTS.md), где указаны команды и владение модулями.

## Лицензия

HartMesh сохраняет [лицензию MIT](LICENSE) DeerFlow и существующие уведомления.

## Благодарности

HartMesh существует благодаря ByteDance и участникам DeerFlow, открывшим основу агентов, которую он расширяет. Мы также благодарим экосистемы LangChain, LangGraph и open-source агентов.

Эксплуатационные заявления, статус релиза, квалификация и границы поддержки downstream-проекта HartMesh остаются его ответственностью.
