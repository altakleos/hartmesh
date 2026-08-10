# HartMesh

[English](README.md) | [简体中文](README_zh.md) | [日本語](README_ja.md) | [Français](README_fr.md) | [Русский](README_ru.md) | Español | [Português](README_pt.md) | [Deutsch](README_de.md)

## La capa de ejecución confiable para agentes DeerFlow.

HartMesh es una distribución de DeerFlow orientada a operaciones que ofrece invocaciones seguras ante reintentos, límites de políticas aplicables, evidencia de ciclo de vida inspeccionable y garantías de despliegue que indican exactamente su alcance.

Se apoya en el espacio de trabajo, los sandboxes, la memoria, las skills, las herramientas, los subagentes, las tareas programadas y los canales de DeerFlow.

> [!IMPORTANT]
> **Estado: prelanzamiento.** Este árbol de código contiene el runtime implementado y evidencia contractual sin conexión, pero todavía no existe una etiqueta de versión de HartMesh que lo incluya.
>
> La calificación exacta de un despliegue real sigue siendo una puerta independiente vinculada al artefacto.

HartMesh es una distribución descendente independiente de [DeerFlow](https://github.com/bytedance/deer-flow), de ByteDance. No es una versión oficial de DeerFlow ni está afiliada o respaldada por ByteDance.

[**Evaluar la versión preliminar**](#inicio-rápido-del-espacio-de-trabajo) · [**Inspeccionar la evidencia**](backend/docs/INVOCATION_RUNTIME.md) · [**Explorar el contrato del runtime**](backend/packages/runtime-api/README.md)

## Por qué existe HartMesh

Los agentes son fáciles de iniciar y difíciles de operar.

Los clientes reintentan, las políticas cambian, las skills evolucionan, los procesos fallan y varios servicios pueden necesitar observar el mismo trabajo.

HartMesh añade un límite de invocación confiable alrededor de DeerFlow para que el trabajo aceptado pueda reintentarse, gobernarse, inspeccionarse y controlarse de forma coherente.

Dentro de un mismo ámbito autenticado y mientras se conserve la fila normal de la ejecución, repetir la misma solicitud estricta con una clave externa hace que HartMesh devuelva el `run_id` retenido.

Cambiar la intención canónica de ejecución bajo esa clave produce un conflicto.

Esto es admisión segura ante reintentos, no ejecución exactamente una vez del modelo, herramientas, proveedores u otros efectos secundarios externos.

HartMesh está pensado principalmente para:

- desarrolladores de plataformas que integran trabajo de DeerFlow en API, servicios, tareas programadas o canales;
- operadores que evalúan una topología gobernada y durable con un solo Gateway; y
- equipos que ejecutan flujos programados o firmados de GitHub de alto valor.

## Basado en DeerFlow, reforzado para operaciones

DeerFlow aporta la base del agente: espacio de trabajo, arnés de LangGraph, sandboxes, memoria, skills, herramientas, subagentes, tareas programadas y canales nativos.

HartMesh conserva esa experiencia y añade el plano de control para aceptar, gobernar, observar y controlar trabajo de larga duración.

Esta comparación corresponde a una instantánea fija. Consulta [compatibilidad y procedencia](#compatibilidad-línea-base-upstream-y-estado-de-la-versión) para ver los commits exactos; no es una afirmación sobre todas las versiones futuras de upstream.

| Lo que conservas de la línea base | Lo que HartMesh añade |
| --- | --- |
| Espacio de trabajo, arnés, memoria, sandboxes, skills, herramientas, subagentes, tareas programadas y canales | Un único límite de admisión consciente del origen; la durabilidad de entrega sigue siendo específica del transporte |
| Ciclo de vida de threads/runs, rutas REST del Gateway y rutas compatibles con LangGraph | Claves externas canónicas con ámbito de origen, identidad de admisión retenida y conflicto explícito ante cambios de intención |
| Configuración de agentes, extensiones y sandbox | Material de ejecución aceptado y fijado |
| Superficies de integración del Gateway y embebidas | Registros estrictos de `ensure`, `observe` y `control` cercado |
| Operación local y con Helm | Evidencia de ciclo de vida e informes explícitos de persistencia, topología y calificación |

Conservas la base de agentes y la superficie de compatibilidad de DeerFlow. Obtienes un plano de control de invocaciones respaldado por evidencia.

Sigues sin obtener alta disponibilidad activa-activa del Gateway, alta disponibilidad del planificador, reanudación universal tras fallos ni efectos secundarios externos exactamente una vez.

## ¿Qué ocurre cuando…?

| Escenario | Comportamiento de HartMesh |
| --- | --- |
| [Un cliente reintenta tras perder la respuesta](backend/tests/test_invocation_idempotency.py) | Una intención canónica igual devuelve la ejecución aceptada; una intención distinta entra en conflicto. |
| [Una skill cambia después de la admisión](backend/tests/test_accepted_skill_snapshots.py) | La invocación aceptada conserva un árbol de skills capturado para el agente y el sandbox; el trabajo posterior ve la edición. |
| [Un sandbox remoto ejecuta skills aceptadas](backend/tests/test_kubernetes_accepted_skill_projection.py) | La proyección compatible `rwx_verified_copy_v2` vincula y revalida evidencia de skills y aislamiento antes del trabajo del grafo/modelo; la calificación real entre nodos sigue vinculada al artefacto. |
| [Un servicio actúa por una persona u observa a otro propietario](backend/tests/test_service_observation_grants.py) | El sujeto humano, el servicio actuante y la evidencia de origen permanecen separados; la observación entre propietarios exige una concesión finita y autorización vigente. |
| [Una capacidad de política requerida no está sana](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | La disponibilidad y una admisión realmente nueva fallan de forma cerrada; una repetición igual reutiliza la evidencia sellada, mientras la autorización actual de observación sigue controlando la divulgación. |
| [Un operador pregunta qué es durable o está calificado](backend/app/runtime/deployment.py) | El informe identifica la persistencia; una referencia declarada es una afirmación del operador y solo la evidencia exacta cotejada por el [verificador sin conexión](backend/scripts/verify_qualification_evidence.py) permite una calificación independiente. |
| [El historial se poda o resulta incoherente](backend/tests/test_invocation_lifecycle_query.py) | La observación acotada devuelve resultados tipados de cursor o integridad en vez de mostrar silenciosamente un historial inválido. |
| [Una entrega firmada de GitHub se interrumpe o su thread está ocupado](backend/tests/test_durable_inbound_receipts.py) | La recuperación de recibos respaldada por PostgreSQL puede reclamar un lease expirado, conservar el aplazamiento FIFO y converger en la misma ejecución aceptada. |

<!-- Demostración futura: añadir una captura de terminal de 30–60 segundos que muestre una invocación con clave, una respuesta perdida simulada, un reintento igual que devuelve el mismo run_id, un conflicto por cambio de intención y la observación del ciclo de vida. -->

## Elige tu ruta

| Ruta | Ideal para | Primera comprobación |
| --- | --- | --- |
| [Espacio de trabajo](#inicio-rápido-del-espacio-de-trabajo) | Desarrolladores que evalúan el espacio de trabajo completo de DeerFlow con controles HartMesh | Una respuesta del modelo mediante el punto de entrada local unificado |
| [Integración del runtime](#api-http-de-invocaciones-durables) | Plataformas que integran trabajo de agentes en servicios, tareas programadas o canales | Solicitudes iguales con clave devuelven un `run_id`, seguido de observación del ciclo de vida |

Los operadores que evalúan un despliegue gobernado pueden ir directamente a los [límites de despliegue y durabilidad](#límites-de-despliegue-y-durabilidad).

> [!CAUTION]
> **Conoce el límite antes de desplegar.**
>
> - La topología validada tiene exactamente una réplica de Gateway.
> - No se afirma alta disponibilidad activa-activa del Gateway, alta disponibilidad del planificador ni despliegue sin tiempo de inactividad.
> - La admisión segura ante reintentos no implica efectos externos exactamente una vez ni reanudación universal tras fallos.
> - La memoria local y la entrada de canales mediante SQLite siguen siendo de mejor esfuerzo.
> - PostgreSQL y evidencia exacta verificada de forma independiente son obligatorios para las afirmaciones correspondientes de durabilidad compartida y calificación.

## Inicio rápido del espacio de trabajo

Trabaja desde el checkout actual de HartMesh en la raíz del repositorio.

Para esta ruta preliminar necesitas Python 3.12+, Node.js 22+, pnpm o Corepack, `uv`, GNU Make, nginx, Docker o Apple Container, credenciales de modelo y aproximadamente 4 núcleos de CPU y 8 GB de RAM. El desarrollo local en Windows utiliza Git Bash.

Cuando `make setup` solicite el modo de ejecución, elige **Container sandbox**. Las invocaciones durables que usan `LocalSandboxProvider` solo funcionan con un conjunto efectivo de skills explícitamente vacío; este checkout habilita las skills integradas de forma predeterminada.

> [!WARNING]
> `make dev` es una ruta de desarrollo para redes de confianza: Gateway `8001`, frontend `3000` y nginx local `2026` escuchan en todas las interfaces. Úsala solo en una máquina de confianza o protegida por firewall mientras completas la configuración del primer administrador.

Configura, diagnostica e inicia:

```bash
make check
make setup
make doctor
make dev
```

`make setup` escribe la configuración local ignorada por Git. `make dev` vuelve a comprobar las herramientas, sincroniza dependencias e inicia Gateway, frontend y nginx.

`make install` es opcional para colaboradores que también quieran hooks de pre-commit.

Abre [http://localhost:2026](http://localhost:2026). En una instalación nueva, completa la configuración del primer administrador, crea un thread y envía un prompt.

El éxito significa que el espacio de trabajo transmite la respuesta del modelo configurado mediante el ciclo de vida respaldado por Gateway.

Detén el stack desde otra terminal:

```bash
make stop
```

Esto evalúa el espacio de trabajo heredado y el stack local. Continúa con la ruta del runtime para verificar el comportamiento del `run_id` retenido de HartMesh; el éxito del espacio de trabajo por sí solo no demuestra durabilidad PostgreSQL ni calificación real de Kubernetes.

## API HTTP de invocaciones durables

Usa la superficie preliminar `/api/runtime/v1` cuando un servicio backend de confianza necesite `ensure → observe`.

Utiliza los registros estrictos de [`deerflow-runtime-api`](backend/packages/runtime-api/README.md), que solo depende de la biblioteca estándar.

Esta ruta no requiere el espacio de trabajo del navegador, pero sí un checkout configurado de HartMesh. Desde la raíz del repositorio, quienes evalúan por primera vez deben ejecutar:

```bash
make check
make setup
make doctor
```

Configura las credenciales del modelo seleccionado por `make setup`. Si ya configuraste el espacio de trabajo, omite estos comandos.

Quienes evalúan el runtime por primera vez también deben elegir **Container sandbox**. El proveedor Local exige un conjunto efectivo de skills vacío para la ejecución durable.

Como se indicó antes, `make dev` escucha en todas las interfaces en los puertos `8001`, `3000` y `2026`; usa una máquina de confianza o protegida por firewall.

Si el archivo `.env` de la raíz ya define un `DEER_FLOW_INTERNAL_AUTH_TOKEN` no vacío, usa ese valor en la terminal cliente.

Elimina o comenta una asignación vacía, porque la carga de `.env` sustituye la exportación del shell siguiente. En caso contrario, genera un token antes de iniciar HartMesh:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Copia este token en la terminal del cliente de confianza:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

En una segunda terminal, exporta el token mostrado y ejecuta este cliente que solo usa la biblioteca estándar:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN='<pega el token generado>'

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
                    "content": "Explica la admisión segura ante reintentos en una frase.",
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

Una clave nueva devuelve `201 created`; su reintento igual devuelve `200 known` con el mismo `run_id`. Cambia el mensaje conservando la clave para recibir un `409 conflict` tipado en vez de ejecutar un trabajo distinto bajo la misma clave.

Este ejemplo usa el servicio integrado `gateway-internal` y omite deliberadamente la delegación humana. Nunca expongas el token interno a un navegador o cliente no confiable.

La delegación de propietario específica del runtime vuelve a validar `X-DeerFlow-Owner-User-Id` contra un usuario local existente.

Consulta la [proyección del principal](backend/app/gateway/services.py) y las [pruebas de identidad](backend/tests/test_invocation_identity_separation.py).

Para DTO, paginación por cursor, fallos tipados y cancelación cercada, consulta el [contrato del runtime](backend/packages/runtime-api/README.md) y la [referencia HTTP](backend/docs/API.md#durable-invocation-runtime-api).

Una respuesta a una solicitud de aclaración inicia una **nueva invocación en el mismo thread de DeerFlow**.

Detén HartMesh desde la raíz del repositorio al finalizar la evaluación:

```bash
make stop
```

## Valor operativo

### Admisión segura ante reintentos

Una clave externa estable y la intención canónica completa del llamador convergen en una única invocación retenida. Una intención igual devuelve esa fila en cualquier estado del ciclo de vida; una intención modificada entra en conflicto y solo el creador adjunta un worker.

Esta garantía dura mientras se conserve la fila normal de la ejecución. No deduplica efectos secundarios externos arbitrarios.

Evidencia: [`idempotency.py`](backend/app/runtime/idempotency.py) y [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py).

### Un único límite de admisión

Las rutas HTTP de creación, streaming y espera, las tareas programadas, los canales nativos autenticados y los servicios embebidos entran en el mismo `InvocationRuntime`. La autenticación del origen, la confirmación y la durabilidad de entrada siguen siendo específicas de cada origen.

Evidencia: [`invocation.py`](backend/app/runtime/invocation.py) y la [matriz de cierre](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix).

### Material de ejecución aceptado y fijado

La admisión fija la revisión del agente, la generación de extensiones, el contexto de confianza, la evidencia de restricciones, los paquetes efectivos de skills y el perfil de ejecución/proyección.

Un único árbol inmutable de skills aceptadas sirve a prompts, activación, política y lecturas de sandbox del agente principal y los subagentes.

Las skills aceptadas no vacías usan un perfil compatible exclusivamente aceptado: AIO local respaldado por contenedores o Kubernetes con la proyección cercada `rwx_verified_copy_v2`.

`LocalSandboxProvider`, E2B, perfiles personalizados y otros remotos siguen admitiendo solo conjuntos vacíos. La evidencia de proyección sin conexión no demuestra una calificación real entre nodos.

Evidencia: fuentes de ejecución aceptada en [`runtime/`](backend/packages/harness/deerflow/runtime/) — `accepted_invocation.py`, `agent_revision.py` y `skill_snapshot.py`.

Evidencia remota: [`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) y [pruebas de proyección](backend/tests/test_kubernetes_accepted_skill_projection.py).

### Política que acompaña a la ejecución

HartMesh mantiene separados el sujeto efectivo, el servicio actuante y la evidencia de origen.

Los servicios autenticados siguen limitados al propietario salvo que un operador conceda un ámbito finito de observación. La concesión limita el descubrimiento; la autorización vigente decide qué puede devolverse y la cancelación no la hereda.

Cuando un operador habilita autorización de operaciones de invocación o configura restricciones v2 autoritativas, esas operaciones fallan de forma cerrada. La salud de capacidades exigidas por el operador y la preparación MCP también.

La autorización y los controles de operaciones de invocación están desactivados por defecto, y `required_capabilities` está vacío de forma predeterminada.

El middleware observacional opcional y el interceptor MCP heredado configurable por API conservan su comportamiento abierto ante fallos o de advertir y omitir.

Se mantienen las comprobaciones de propietario y ruta, pero HartMesh no incluye una política universal de organización ni garantiza herramientas arbitrarias de terceros.

Evidencia: [contrato de extensión](backend/packages/extension-api/README.md), [autorización](backend/app/runtime/authorization.py), [restricciones](backend/app/runtime/constraints.py) y [visibilidad](backend/app/runtime/visibility.py).

### Integración portable del runtime

`deerflow-runtime-api`, que solo usa la biblioteca estándar, define registros estrictos e inmutables y un `DurableInvocationPort`: `ensure`, `observe` de invocación/contexto, `control` cercado y `capabilities`.

HTTP autenticado y el adaptador en proceso alojado por la aplicación comparten esos registros y una suite de conformidad. El `DeerFlowClient` síncrono no es un adaptador durable; v1 no ofrece push mediante broker, exportación de contexto ni retirada de contexto.

Evidencia: [paquete del runtime](backend/packages/runtime-api/README.md) y [conformidad de transportes](backend/tests/test_runtime_api_conformance.py).

### Integridad transaccional del ciclo de vida

Con el almacén SQL, un cambio de estado y su evento seguro de ciclo de vida se confirman atómicamente bajo una única versión de estado. La observación acotada usa una instantánea autoritativa y devuelve resultados tipados para historiales podados, futuros o incoherentes.

La lectura repetible y el comportamiento entre sesiones de PostgreSQL solo son afirmaciones de versión cuando pasa la puerta externa de PostgreSQL.

Evidencia: [`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) y [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py).

Pruebas contractuales: [almacén atómico de ciclo de vida](backend/tests/test_invocation_lifecycle_store.py) y [consultas tipadas](backend/tests/test_invocation_lifecycle_query.py).

### Entrada durable firmada de GitHub

Con verificación HMAC y recibos PostgreSQL, la entrada firmada de GitHub persiste vínculos de origen acotados antes de confirmar la recepción.

Los leases y fences pueden reclamar un lease expirado tras una interrupción, conservar FIFO cuando el thread está ocupado y converger en la misma invocación aceptada.

Esta afirmación se limita a entrada firmada de GitHub verificada con PostgreSQL; otros canales y rutas locales siguen siendo de mejor esfuerzo.

Evidencia: [almacén de recibos](backend/app/channels/inbound_receipts.py) y [pruebas de recibos](backend/tests/test_durable_inbound_receipts.py).

## Límites de despliegue y durabilidad

Lee estos límites antes de seguir cualquier guía de despliegue:

- La topología validada tiene exactamente una réplica de Gateway.
- No se afirma HA activa-activa del Gateway, HA del planificador ni despliegue sin tiempo de inactividad.
- La admisión segura ante reintentos no es ejecución universal exactamente una vez de efectos externos.
- Con almacenamiento durable de invocaciones, la recuperación tras pérdida de proceso conserva evidencia terminal autoritativa; no reanuda de forma transparente cada grafo o llamada de herramienta.
- La memoria es local al proceso, SQLite es durable a nivel de nodo para el estado de invocación y PostgreSQL es el almacén durable compartido.
- La entrada de canales mediante memoria y SQLite sigue siendo de mejor esfuerzo.
- La entrada nativa durable significa actualmente entrega firmada de GitHub verificada con PostgreSQL.
- Las skills aceptadas no vacías requieren una ruta de sandbox compatible exclusivamente aceptada.
- La calificación de Kubernetes/PostgreSQL requiere evidencia aprobada exacta para imagen, chart, configuración, esquema, topología, ámbito y escenarios nombrados.
- Una puerta optativa recopilada u omitida no es una calificación aprobada.

Para la instantánea de código auditada, las puertas externas optativas de PostgreSQL y Kubernetes no estaban configuradas y no había un artefacto exacto de calificación aprobado.

Son puertas de versión no superadas, no evidencia de que el comportamiento implementado esté ausente.

| Modo | Límite informado |
| --- | --- |
| `local_development` | Permite estado local al proceso sin afirmar durabilidad. |
| `durable_production` | Rechaza estado de invocación local al proceso durante el inicio y la disponibilidad. |
| Helm `local_evaluation` | Valores predeterminados para evaluar un Gateway; explícitamente no calificado. |
| Helm `durable_one_replica` | Exige imágenes fijadas por digest, PostgreSQL/estado compartido y tiempos seguros de sondas y apagado; sigue sin calificar sin evidencia exacta aprobada. |

El informe administrativo de despliegue separa nivel de persistencia, salud, procedencia y calificación.

Una referencia de calificación suministrada sigue siendo `operator_asserted`; solo el verificador sin conexión puede establecer `external_evidence_verified` para evidencia exacta. Las capacidades portables no incluyen afirmaciones de despliegue.

`GET /health` informa la vida del proceso. `GET /ready` es una señal acotada de disponibilidad. Los administradores inspeccionan persistencia y calificación en `GET /api/runtime/v1/deployment`.

Evidencia: [informe de despliegue](backend/app/runtime/deployment.py), [verificación de calificación](backend/scripts/verify_qualification_evidence.py) y [contrato de despliegue Helm](deploy/helm/deer-flow/README.md).

Con almacenamiento durable de invocaciones, cuando una ejecución activa se pierde con su proceso, la recuperación registra evidencia terminal autoritativa como `stop_reason=orphan_recovered`.

Un reintento igual devuelve esa ejecución terminal retenida. Continuar la intención del producto exige una nueva invocación bajo la nueva generación del proceso.

Consulta [ciclo de vida autoritativo y recuperación ante fallos](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery).

### Límite de seguridad

Los stacks de Compose publican solo nginx y lo vinculan a `127.0.0.1:2026` por defecto. En cambio, `make dev` local escucha en todas las interfaces en los puertos `8001`, `3000` y `2026`; no lo trates como si tuviera la misma barrera de puertos publicados.

Los agentes HartMesh pueden ejecutar comandos y leer o escribir archivos permitidos por las herramientas configuradas. El aislamiento depende del proveedor: `LocalSandboxProvider` comparte la identidad del host Gateway y no es un límite de aislamiento del sistema operativo.

La clasificación de comandos y la reescritura de rutas son defensa en profundidad. Usa un proveedor aislado compatible para trabajo no confiable.

Completa la configuración del primer administrador antes de hacer accesible el servicio fuera de loopback.

Los administradores pueden configurar procesos MCP por stdio y plugins Python de confianza, por lo que el acceso de administrador equivale a ejecución de código.

Consulta la [guía de despliegue Helm](deploy/helm/deer-flow/README.md) para el contrato de un solo Gateway, proyección de skills aceptadas, credenciales y procedimiento exacto de calificación.

## Modelo de extensiones

HartMesh conserva el modelo de skills, herramientas, servidores MCP, agentes personalizados y middleware de DeerFlow.

[`deerflow-extension-api`](backend/packages/extension-api/README.md), independiente del host, añade contratos tipados para autorización, identidad y contribución de contexto confiable.

También cubre restricciones limitantes, salud de capacidades y preparación MCP requerida.

Los plugins Python son código de operador de confianza cargado al inicio desde `plugins:` en `config.yaml`. Esa lista permanece intencionadamente fuera de `extensions_config.json`, modificable mediante API y propietario de la configuración de MCP y skills.

Una invocación aceptada fija una generación de extensiones congelada al inicio. Los cambios de skills afectan admisiones posteriores; los cambios de plugins exigen reiniciar Gateway para crear una nueva generación. Ninguno cambia trabajo ya aceptado.

La integración administrada de Lark/Feishu CLI permanece limitada al usuario. Después de conectar, **Change Lark app** puede sustituir el App ID y App Secret de ese usuario sin reinstalar el paquete de skills: la CLI valida la aplicación nueva antes de activarla, elimina los tokens OAuth de la anterior e inicia la autorización de la nueva. En ejecución con sandbox, la raíz de configuración que contiene credenciales permanece en solo lectura y su subdirectorio `config/locks` se monta por separado para escrituras acotadas de coordinación de la CLI.

## Compatibilidad, línea base upstream y estado de la versión

HartMesh conserva los namespaces `deerflow.*`, nombres de paquetes, variables `DEER_FLOW_*`, identificadores Docker y Helm, rutas del sistema de archivos y superficies de compatibilidad del Gateway.

La comparación del producto usa el rango local fijo `e16ef2969b1446162e19af7bdde1446674851e66...ca2400f3059b3ac93249473e97ed83c5296fb0f0`.

`main` de HartMesh incorpora `deerflow/main` upstream hasta `17531d7c118d6111b863f945ff910a7889a235b0` (2026-08-10).

Ese punto de sincronización es contexto, no la línea base de comparación anterior, y HartMesh no hace afirmaciones permanentes de superioridad.

Este repositorio todavía no documenta una cadencia de sincronización de HartMesh, ventana de compatibilidad de API/configuración/base de datos, periodo de soporte, política de incorporación de correcciones de seguridad ni política de contribución upstream.

Trata estos hashes como procedencia, no como una promesa de mantenimiento.

El grafo Alembic es lineal en este checkout: `0011_mcp_tasks` → `0011_accepted_invocation` → migraciones de invocación hasta `0019_inbound_event_identity`.

Los operadores PostgreSQL deben detener escritores y respaldar los datos antes de revertir; usa la guía de migración de [backend/AGENTS.md](backend/AGENTS.md).

Las fuentes de versión informan `2.1.0`, pero ninguna etiqueta contiene la implementación HartMesh auditada; las cadenas de versión no establecen una versión de HartMesh.

[RELEASING.md](RELEASING.md) documenta la mecánica heredada de etiquetas DeerFlow, no un canal de versiones propio de HartMesh.

## Documentación

- [Runtime de invocaciones durables](backend/docs/INVOCATION_RUNTIME.md) — garantías, evidencia, recuperación y ámbito aplazado
- [API del runtime](backend/packages/runtime-api/README.md) — DTO y `DurableInvocationPort`
- [API del Gateway](backend/docs/API.md) — comportamiento HTTP autenticado
- [API de extensiones](backend/packages/extension-api/README.md) — límites de políticas y confianza
- [Despliegue Helm](deploy/helm/deer-flow/README.md) — modos de un Gateway y calificación
- [Configuración](config.example.yaml) — ajustes del operador
- [Guía backend](backend/AGENTS.md) y [guía frontend](frontend/AGENTS.md) — arquitectura y pruebas

## Soporte y seguridad

Ejecuta diagnósticos locales desde la raíz del repositorio:

```bash
make doctor
make support-bundle
```

Revisa el material de soporte generado antes de compartirlo.

Este repositorio todavía no documenta un gestor de incidencias, canal de versiones ni vía privada para informar vulnerabilidades propios de HartMesh.

[CONTRIBUTING.md](CONTRIBUTING.md) y [SECURITY.md](SECURITY.md) conservan los destinos upstream de ByteDance DeerFlow; esos destinos no son soporte propio de HartMesh.

No incluyas credenciales, tokens, prompts privados, datos de clientes ni detalles de vulnerabilidades en una incidencia pública. Trata los tokens internos, secretos de webhook, claves de proveedor y credenciales de base de datos como secretos.

## Contribuir

Para trabajar localmente, sigue las convenciones heredadas de [CONTRIBUTING.md](CONTRIBUTING.md) y el [AGENTS.md](AGENTS.md) más cercano para los comandos y la propiedad de módulos.

## Licencia

HartMesh conserva la [licencia MIT](LICENSE) de DeerFlow y los avisos existentes.

## Agradecimientos

HartMesh existe porque ByteDance y los colaboradores de DeerFlow publicaron la base de agentes que amplía. También agradecemos a los ecosistemas de código abierto de LangChain, LangGraph y otros.

Las afirmaciones operativas, el estado de versión, la calificación y los límites de soporte descendentes de HartMesh siguen siendo propios.
