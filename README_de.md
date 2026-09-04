# HartMesh

[English](README.md) | [简体中文](README_zh.md) | [日本語](README_ja.md) | [Français](README_fr.md) | [Русский](README_ru.md) | [Español](README_es.md) | [Português](README_pt.md) | Deutsch

## Die verlässliche Ausführungsschicht für DeerFlow-Agenten.

HartMesh ist eine auf den Betrieb ausgerichtete DeerFlow-Distribution für wiederholungssichere Aufrufe, durchsetzbare Richtliniengrenzen, überprüfbare Lebenszyklusnachweise und Bereitstellungsgarantien, die ihren Geltungsbereich exakt benennen.

Sie baut auf DeerFlows Workspace, Sandboxes, Speicher, Skills, Werkzeugen, Subagenten, Zeitplänen und Kanälen auf.

> [!IMPORTANT]
> **Status: Vorabversion.** Dieser Quellbaum enthält die implementierte Runtime und Offline-Vertragsnachweise, aber noch kein HartMesh-Release-Tag umfasst sie.
>
> Die exakte Qualifikation einer echten Bereitstellung bleibt eine separate, artefaktgebundene Freigabe.

HartMesh ist eine unabhängige Downstream-Distribution von ByteDances [DeerFlow](https://github.com/bytedance/deer-flow). Es ist kein offizielles DeerFlow-Release und weder mit ByteDance verbunden noch von ByteDance unterstützt.

[**Vorschau evaluieren**](#workspace-schnellstart) · [**Nachweise prüfen**](backend/docs/INVOCATION_RUNTIME.md) · [**Runtime-Vertrag erkunden**](backend/packages/runtime-api/README.md)

## Warum HartMesh existiert

Agenten sind leicht zu starten und schwer zu betreiben.

Clients wiederholen Anfragen, Richtlinien ändern sich, Skills entwickeln sich weiter, Prozesse fallen aus und mehrere Dienste müssen möglicherweise dieselbe Arbeit beobachten.

HartMesh legt eine verlässliche Aufrufgrenze um DeerFlow, damit angenommene Arbeit kohärent wiederholt, gesteuert, geprüft und kontrolliert werden kann.

Innerhalb eines authentifizierten Geltungsbereichs und solange der normale Ausführungsdatensatz erhalten bleibt, liefert HartMesh bei Wiederholung derselben strikten Anfrage unter demselben externen Schlüssel die gespeicherte `run_id` zurück.

Eine Änderung der kanonischen Ausführungsabsicht unter diesem Schlüssel führt zu einem Konflikt.

Das ist wiederholungssichere Annahme, nicht eine Genau-einmal-Ausführung des Modells, von Werkzeugen, Providern oder anderen externen Seiteneffekten.

HartMesh richtet sich zunächst an:

- Plattformentwickler, die DeerFlow-Arbeit in APIs, Dienste, Zeitpläne oder Kanäle einbetten;
- Betreiber, die eine gesteuerte, dauerhafte Topologie mit einem Gateway evaluieren; und
- Teams mit hochwertigen geplanten oder signierten GitHub-Abläufen.

## Auf DeerFlow aufgebaut, für den Betrieb gehärtet

DeerFlow liefert die Agentenbasis: Workspace, LangGraph-Harness, Sandboxes, Speicher, Skills, Werkzeuge, Subagenten, Zeitpläne und native Kanäle.

HartMesh behält diese Erfahrung bei und ergänzt die Kontrollebene für Annahme, Steuerung, Beobachtung und Kontrolle langlebiger Arbeit.

Dieser Vergleich gilt für einen festen Snapshot. Die genauen Commits stehen unter [Kompatibilität und Herkunft](#kompatibilität-upstream-basis-und-release-status); er ist keine Aussage über alle künftigen Upstream-Releases.

| Was von der Basis erhalten bleibt | Was HartMesh ergänzt |
| --- | --- |
| Workspace, Harness, Speicher, Sandboxes, Skills, Werkzeuge, Subagenten, Zeitpläne und Kanäle | Eine einzige quellbewusste Annahmegrenze; Zustellungsdauerhaftigkeit bleibt transportspezifisch |
| DeerFlow-Thread-/Run-Lebenszyklus, Gateway-REST-Routen und LangGraph-kompatible Routen | Kanonische externe Schlüssel mit Quellbereich, gespeicherte Annahmeidentität und expliziter Konflikt bei geänderter Absicht |
| Agenten-, Erweiterungs- und Sandbox-Konfiguration | Festgeschriebenes angenommenes Ausführungsmaterial |
| Gateway- und eingebettete Integrationsoberflächen | Strikte Datensätze für `ensure`, `observe` und eingezäuntes `control` |
| Lokaler und Helm-Betrieb | Lebenszyklusnachweise sowie explizite Angaben zu Persistenz, Topologie und Qualifikation |

Die Agentenbasis und Kompatibilitätsoberfläche von DeerFlow bleiben erhalten. Hinzu kommt eine nachweisgestützte Kontrollebene für Aufrufe.

Aktiv-aktive Gateway-HA, Scheduler-HA, universelle Wiederaufnahme nach Abstürzen und externe Seiteneffekte genau einmal sind weiterhin nicht enthalten.

## Was passiert, wenn …?

| Szenario | Verhalten von HartMesh |
| --- | --- |
| [Ein Client nach verlorener Antwort wiederholt](backend/tests/test_invocation_idempotency.py) | Gleiche kanonische Absicht liefert die angenommene Ausführung; geänderte Absicht erzeugt einen Konflikt. |
| [Ein Skill nach der Annahme geändert wird](backend/tests/test_accepted_skill_snapshots.py) | Der angenommene Aufruf behält einen erfassten Skill-Baum für Agenten- und Sandbox-Verbraucher; spätere Arbeit sieht die Änderung. |
| [Eine entfernte Sandbox angenommene Skills ausführt](backend/tests/test_kubernetes_accepted_skill_projection.py) | Die unterstützte Projektion `rwx_verified_copy_v2` bindet und prüft Skill- und Isolationsnachweise vor Graph-/Modellarbeit erneut; die echte knotenübergreifende Qualifikation bleibt artefaktgebunden. |
| [Ein Dienst für einen Menschen handelt oder einen anderen Eigentümer beobachtet](backend/tests/test_service_observation_grants.py) | Menschliches Subjekt, handelnder Dienst und Quellnachweis bleiben getrennt; eigentümerübergreifende Beobachtung verlangt eine endliche Freigabe plus aktuelle Autorisierung. |
| [Eine erforderliche Richtlinienfähigkeit ungesund wird](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | Bereitschaft und wirklich neue Annahme schlagen geschlossen fehl; gleiche Wiederholung nutzt versiegelte Annahmenachweise, während aktuelle Beobachtungsautorisierung die Offenlegung steuert. |
| [Ein Betreiber nach Dauerhaftigkeit oder Qualifikation fragt](backend/app/runtime/deployment.py) | Der Bericht benennt die Persistenz; eine deklarierte Referenz ist eine Betreiberbehauptung, und nur exakt durch den [Offline-Prüfer](backend/scripts/verify_qualification_evidence.py) abgeglichene Nachweise tragen eine unabhängige Qualifikation. |
| [Lebenszyklusverlauf bereinigt oder inkonsistent ist](backend/tests/test_invocation_lifecycle_query.py) | Begrenzte Beobachtung liefert typisierte Cursor- oder Integritätsergebnisse statt stillschweigend ungültigen Verlauf zu zeigen. |
| [Eine signierte GitHub-Zustellung unterbrochen wird oder ihr Thread belegt ist](backend/tests/test_durable_inbound_receipts.py) | PostgreSQL-gestützte Empfangswiederherstellung kann einen abgelaufenen Lease übernehmen, FIFO-Aufschub bewahren und auf dieselbe angenommene Ausführung konvergieren. |

<!-- Künftige Demo: eine 30–60 Sekunden lange Terminalaufnahme mit schlüsselgebundenem Aufruf, simuliertem Antwortverlust, gleicher Wiederholung mit derselben run_id, Konflikt bei geänderter Absicht und Lebenszyklusbeobachtung. -->

## Wähle deinen Weg

| Weg | Geeignet für | Erster Nachweis |
| --- | --- | --- |
| [Workspace](#workspace-schnellstart) | Entwickler, die den vollständigen DeerFlow-Workspace mit HartMesh-Kontrollen evaluieren | Eine Modellantwort über den einheitlichen lokalen Einstiegspunkt |
| [Runtime-Integration](#http-api-für-dauerhafte-aufrufe) | Plattformen, die Agentenarbeit in Dienste, Zeitpläne oder Kanäle einbetten | Gleiche schlüsselgebundene Anfragen liefern eine `run_id`, danach folgt Lebenszyklusbeobachtung |

Betreiber einer gesteuerten Bereitstellung können direkt zu den [Bereitstellungs- und Dauerhaftigkeitsgrenzen](#bereitstellungs--und-dauerhaftigkeitsgrenzen) springen.

> [!CAUTION]
> **Kenne die Grenze vor der Bereitstellung.**
>
> - Die validierte Topologie hat genau eine Gateway-Replik.
> - Aktiv-aktive Gateway-HA, Scheduler-HA und Rollouts ohne Ausfallzeit werden nicht zugesichert.
> - Wiederholungssichere Annahme bedeutet weder externe Seiteneffekte genau einmal noch universelle Wiederaufnahme nach Abstürzen.
> - Lokaler Speicher und SQLite-Kanaleingang bleiben Best Effort.
> - PostgreSQL und exakte, unabhängig verifizierte Nachweise sind für die entsprechenden Aussagen zu geteilter Dauerhaftigkeit und Qualifikation erforderlich.

## Workspace-Schnellstart

Arbeite im aktuellen HartMesh-Checkout vom Repository-Stamm aus.

Für diesen Vorschauweg benötigst du Python 3.12+, Node.js 22+, pnpm oder Corepack, `uv`, GNU Make, nginx, Docker oder Apple Container, Modellzugangsdaten sowie ungefähr 4 CPU-Kerne und 8 GB RAM. Lokale Entwicklung unter Windows verwendet Git Bash.

Wenn `make setup` nach dem Ausführungsmodus fragt, wähle **Container sandbox**. Dauerhafte Aufrufe mit `LocalSandboxProvider` funktionieren nur mit einer explizit leeren effektiven Skill-Menge; dieser Checkout aktiviert integrierte Skills standardmäßig.

> [!WARNING]
> `make dev` ist ein Entwicklungsweg für vertrauenswürdige Netze: Gateway `8001`, Frontend `3000` und lokales nginx `2026` lauschen auf allen Host-Schnittstellen. Verwende ihn während der Ersteinrichtung nur auf einer vertrauenswürdigen oder durch eine Host-Firewall geschützten Maschine.

Konfigurieren, diagnostizieren und starten:

```bash
make check
make setup
make doctor
make dev
```

`make setup` schreibt die von Git ignorierte lokale Konfiguration. `make dev` prüft Werkzeuge erneut, synchronisiert Abhängigkeiten und startet Gateway, Frontend und nginx.

`make install` ist optional für Mitwirkende, die auch Pre-Commit-Hooks wünschen.

Öffne [http://localhost:2026](http://localhost:2026). Schließe bei einer neuen Installation die Ersteinrichtung ab, erstelle einen Thread und sende einen Prompt.

Erfolg bedeutet, dass der Workspace die Antwort des konfigurierten Modells über den Gateway-gestützten Lebenszyklus streamt.

Beende den Stack in einem anderen Terminal:

```bash
make stop
```

Damit werden der geerbte Workspace und der lokale Stack evaluiert. Fahre mit dem Runtime-Weg fort, um HartMeshs gespeichertes `run_id`-Verhalten zu prüfen; Workspace-Erfolg allein belegt weder PostgreSQL-Dauerhaftigkeit noch echte Kubernetes-Qualifikation.

## HTTP-API für dauerhafte Aufrufe

Verwende die Voraboberfläche `/api/runtime/v1`, wenn ein vertrauenswürdiger Backend-Dienst `ensure → observe` benötigt.

Sie nutzt die strikten Datensätze der nur auf der Standardbibliothek basierenden [`deerflow-runtime-api`](backend/packages/runtime-api/README.md).

Dieser Weg benötigt keinen Browser-Workspace, aber weiterhin einen konfigurierten HartMesh-Checkout. Erstmalige Evaluatoren führen im Repository-Stamm aus:

```bash
make check
make setup
make doctor
```

Konfiguriere die Zugangsdaten für das von `make setup` ausgewählte Modell. Wenn der Workspace bereits eingerichtet ist, überspringe diese Befehle.

Erstmalige Runtime-Evaluatoren müssen ebenfalls **Container sandbox** wählen. Der Local-Provider verlangt für dauerhafte Ausführung eine leere effektive Skill-Menge.

Wie oben lauscht `make dev` auf allen Schnittstellen an den Ports `8001`, `3000` und `2026`; verwende eine vertrauenswürdige oder durch eine Host-Firewall geschützte Maschine.

Wenn `.env` im Stamm bereits einen nichtleeren `DEER_FLOW_INTERNAL_AUTH_TOKEN` definiert, verwende diesen Wert im Client-Terminal.

Entferne oder kommentiere eine leere Zuweisung, da das Laden von `.env` den folgenden Shell-Export überschreibt. Andernfalls erzeuge vor dem Start von HartMesh ein Token:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Dieses Token in das vertrauenswürdige Client-Terminal kopieren:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

Exportiere das ausgegebene Token in einem zweiten Terminal und führe diesen Client aus, der nur die Standardbibliothek verwendet:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN='<erzeugtes Token einfügen>'

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
                    "content": "Erkläre wiederholungssichere Annahme in einem Satz.",
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

Ein neuer Schlüssel liefert `201 created`; seine gleiche Wiederholung liefert `200 known` mit derselben `run_id`. Ändere die Nachricht bei gleichem Schlüssel, um einen typisierten `409 conflict` statt anderer Arbeit unter demselben Schlüssel zu erhalten.

Dieses Beispiel nutzt den integrierten Dienst `gateway-internal` und lässt menschliche Delegation absichtlich aus. Gib das interne Token niemals an einen Browser oder nicht vertrauenswürdigen Client weiter.

Die runtimespezifische Eigentümerdelegation prüft `X-DeerFlow-Owner-User-Id` erneut gegen einen vorhandenen lokalen Benutzer.

Siehe [Principal-Projektion](backend/app/gateway/services.py) und [Identitätstests](backend/tests/test_invocation_identity_separation.py).

DTOs, Cursor-Paginierung, typisierte Fehler und eingezäunte Abbrüche beschreibt der [Runtime-Vertrag](backend/packages/runtime-api/README.md), die [HTTP-Referenz](backend/docs/API.md#durable-invocation-runtime-api) die Transportoberfläche.

Eine Antwort auf eine Klärungsfrage startet einen **neuen Aufruf im selben DeerFlow-Thread**.

Beende HartMesh nach der Evaluation vom Repository-Stamm aus:

```bash
make stop
```

## Betrieblicher Nutzen

### Wiederholungssichere Annahme

Ein stabiler externer Schlüssel plus vollständige kanonische Aufruferabsicht konvergieren auf einen gespeicherten Aufruf. Gleiche Absicht liefert den Datensatz in jedem Lebenszykluszustand; geänderte Absicht erzeugt einen Konflikt, und nur der Ersteller bindet einen Worker an.

Diese Garantie gilt, solange der normale Ausführungsdatensatz erhalten bleibt. Sie dedupliziert keine beliebigen externen Seiteneffekte.

Nachweise: [`idempotency.py`](backend/app/runtime/idempotency.py) und [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py).

### Eine Annahmegrenze

HTTP-Erstellungs-, Streaming- und Wartepfade, geplante Aufgaben, authentifizierte native Kanäle und eingebettete Dienste treten in dieselbe `InvocationRuntime` ein. Quellauthentifizierung, Bestätigung und Eingangsdauerhaftigkeit bleiben quellspezifisch.

Nachweise: [`invocation.py`](backend/app/runtime/invocation.py) und die [Abschlussmatrix](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix).

### Festgeschriebenes angenommenes Ausführungsmaterial

Die Annahme schreibt Agentenrevision, Erweiterungsgeneration, vertrauenswürdigen Kontext, Einschränkungsnachweise, effektive Skill-Pakete und Ausführungs-/Projektionsprofil fest.

Ein unveränderlicher angenommener Skill-Baum dient Hauptagent und Subagenten für Prompts, Aktivierung, Richtlinie und Sandbox-Lesezugriffe.

Nichtleere angenommene Skills verwenden ein unterstütztes Accepted-only-Profil: lokales containergestütztes AIO oder Kubernetes mit der eingezäunten Projektion `rwx_verified_copy_v2`.

`LocalSandboxProvider`, E2B, benutzerdefinierte und andere entfernte Profile bleiben Empty-only. Offline-Projektionsnachweise belegen keine echte knotenübergreifende Qualifikation.

Nachweise: Quellen der angenommenen Ausführung in [`runtime/`](backend/packages/harness/deerflow/runtime/) — `accepted_invocation.py`, `agent_revision.py` und `skill_snapshot.py`.

Entfernte Nachweise: [`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) und [Projektionstests](backend/tests/test_kubernetes_accepted_skill_projection.py).

### Richtlinie folgt der Ausführung

HartMesh hält effektives Subjekt, handelnden Dienst und Quellnachweis getrennt.

Authentifizierte Dienste bleiben auf den Eigentümer begrenzt, sofern ein Betreiber keinen endlichen Beobachtungsbereich gewährt. Die Freigabe begrenzt die Suche; aktuelle Autorisierung entscheidet weiterhin über Rückgaben, und Abbruch erbt sie nicht.

Wenn ein Betreiber Aufrufoperations-Autorisierung aktiviert oder autoritative v2-Einschränkungen konfiguriert, schlagen diese Operationen geschlossen fehl. Dasselbe gilt für vom Betreiber geforderte Fähigkeitsgesundheit und MCP-Vorbereitung.

Autorisierung und Aufrufoperationskontrollen sind standardmäßig deaktiviert; `required_capabilities` ist standardmäßig leer.

Optionales Beobachtungs-Middleware und der ältere API-beschreibbare MCP-Interceptor behalten ihr Fail-open- beziehungsweise Warn-and-skip-Verhalten.

Eigentümer- und Routenprüfungen bleiben, aber HartMesh liefert weder eine universelle Organisationsrichtlinie noch Garantien für beliebige Drittanbieterwerkzeuge.

Nachweise: [Erweiterungsvertrag](backend/packages/extension-api/README.md), [Autorisierung](backend/app/runtime/authorization.py), [Einschränkungen](backend/app/runtime/constraints.py) und [Sichtbarkeit](backend/app/runtime/visibility.py).

### Portable Runtime-Integration

Die nur auf der Standardbibliothek basierende `deerflow-runtime-api` definiert strikte unveränderliche Datensätze und einen `DurableInvocationPort`: `ensure`, `observe` für Aufruf/Kontext, eingezäuntes `control` und `capabilities`.

Authentifiziertes HTTP und der von der Anwendung gehostete In-Process-Adapter teilen diese Datensätze und eine Konformitätssuite. Der synchrone `DeerFlowClient` ist kein dauerhafter Adapter; v1 bietet weder Broker-Push noch Kontextexport oder Kontextstilllegung.

Nachweise: [Runtime-Paket](backend/packages/runtime-api/README.md) und [Transportkonformität](backend/tests/test_runtime_api_conformance.py).

### Transaktionale Lebenszyklusintegrität

Mit dem SQL-Speicher werden Zustandsänderung und sicheres Lebenszyklusereignis atomar unter einer Zustandsversion bestätigt. Begrenzte Beobachtung nutzt einen autoritativen Snapshot und liefert typisierte Ergebnisse für bereinigten, zukünftigen oder inkonsistenten Verlauf.

PostgreSQL Repeatable Read und sitzungsübergreifendes Verhalten sind nur dann Release-Aussagen, wenn die externe PostgreSQL-Freigabe besteht.

Nachweise: [`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) und [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py).

Vertragstests: [atomarer Lebenszyklusspeicher](backend/tests/test_invocation_lifecycle_store.py) und [typisierte Lebenszyklusabfragen](backend/tests/test_invocation_lifecycle_query.py).

### Dauerhafter signierter GitHub-Eingang

Mit HMAC-Prüfung und PostgreSQL-Empfangsdatensätzen speichert der signierte GitHub-Eingang begrenzte Quellbindungen vor der Bestätigung.

Leases und Fences können einen abgelaufenen Lease nach Unterbrechung übernehmen, FIFO bei belegtem Thread erhalten und auf denselben angenommenen Aufruf konvergieren.

Diese Aussage gilt nur für verifizierten signierten GitHub-Eingang mit PostgreSQL; andere und lokale Kanalpfade bleiben Best Effort.

Nachweise: [Empfangsspeicher](backend/app/channels/inbound_receipts.py) und [Empfangstests](backend/tests/test_durable_inbound_receipts.py).

## Bereitstellungs- und Dauerhaftigkeitsgrenzen

Lies diese Grenzen, bevor du einer Bereitstellungsanleitung folgst:

- Die validierte Topologie hat genau eine Gateway-Replik.
- Aktiv-aktive Gateway-HA, Scheduler-HA und Rollouts ohne Ausfallzeit werden nicht zugesichert.
- Wiederholungssichere Annahme ist keine universelle Genau-einmal-Ausführung externer Seiteneffekte.
- Bei dauerhaftem Aufrufspeicher bewahrt Wiederherstellung nach Prozessverlust autoritative Endnachweise; sie setzt nicht jeden Graph- oder Werkzeugaufruf transparent fort.
- Memory ist prozesslokal, SQLite ist für Aufrufzustand knotendauerhaft und PostgreSQL ist der geteilte dauerhafte Speicher.
- Native Kanaleingänge über Memory und SQLite bleiben Best Effort.
- Dauerhafter nativer Eingang bedeutet derzeit verifizierte signierte GitHub-Zustellung mit PostgreSQL.
- Nichtleere angenommene Skills benötigen einen unterstützten Accepted-only-Sandboxpfad.
- Kubernetes-/PostgreSQL-Qualifikation erfordert exakt bestandene Nachweise für benannte Images, Chart, Konfiguration, Schema, Topologie, Umfang und Szenarien.
- Eine eingesammelte oder übersprungene optionale Freigabe ist keine bestandene Qualifikation.

Für den geprüften Quell-Snapshot waren die externen optionalen PostgreSQL- und Kubernetes-Freigaben nicht konfiguriert, und es lag kein exakt bestandenes Qualifikationsartefakt vor.

Das sind nicht bestandene Release-Freigaben, kein Beleg dafür, dass das implementierte Verhalten fehlt.

| Modus | Gemeldete Grenze |
| --- | --- |
| `local_development` | Erlaubt prozesslokalen Zustand ohne Dauerhaftigkeitsaussage. |
| `durable_production` | Lehnt prozesslokalen Aufrufzustand bei Start und Bereitschaft ab. |
| Helm `local_evaluation` | Standardwerte für eine Gateway-Evaluation; ausdrücklich nicht qualifiziert. |
| Helm `durable_one_replica` | Verlangt digest-fixierte Images, PostgreSQL/geteilten Zustand und sichere Probe-/Abschaltzeiten; bleibt ohne exakt bestandene Nachweise unqualifiziert. |

Der administrative Bereitstellungsbericht trennt Persistenzstufe, Gesundheit, Herkunft und Qualifikation.

Eine angegebene Qualifikationsreferenz bleibt `operator_asserted`; nur der Offline-Prüfer kann für exakte Nachweise `external_evidence_verified` feststellen. Portable Fähigkeiten enthalten keine Bereitstellungsaussagen.

`GET /health` meldet Prozesslebendigkeit. `GET /ready` ist ein begrenztes Bereitschaftssignal. Administratoren prüfen Persistenz und Qualifikation unter `GET /api/runtime/v1/deployment`.

Nachweise: [Bereitstellungsbericht](backend/app/runtime/deployment.py), [Qualifikationsprüfung](backend/scripts/verify_qualification_evidence.py) und [Helm-Bereitstellungsvertrag](deploy/helm/deer-flow/README.md).

Wenn bei dauerhaftem Aufrufspeicher eine aktive Ausführung mit ihrem Prozess verloren geht, schreibt die Wiederherstellung autoritative Endnachweise wie `stop_reason=orphan_recovered`.

Eine gleiche Wiederholung liefert diese gespeicherte beendete Ausführung. Die Produktabsicht fortzusetzen erfordert einen neuen Aufruf unter der neuen Prozessgeneration.

Siehe [autoritativer Lebenszyklus und Fehlerwiederherstellung](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery).

### Sicherheitsgrenze

Die Compose-Stacks veröffentlichen nur nginx und binden standardmäßig an `127.0.0.1:2026`. Lokales `make dev` lauscht dagegen an allen Host-Schnittstellen auf `8001`, `3000` und `2026`; behandle es nicht als gleichwertige Veröffentlichungsgrenze.

HartMesh-Agenten können Befehle ausführen und von konfigurierten Werkzeugen erlaubte Dateien lesen oder schreiben. Die Isolation hängt vom Provider ab: `LocalSandboxProvider` teilt die Host-Identität des Gateways und ist keine Betriebssystem-Isolationsgrenze.

Befehlsklassifikation und Pfadumschreibung sind Defense in Depth. Verwende für nicht vertrauenswürdige Arbeit einen unterstützten isolierten Provider.

Schließe die Ersteinrichtung ab, bevor der Dienst außerhalb von Loopback erreichbar wird.

Administratoren können stdio-MCP-Prozesse und vertrauenswürdige Python-Plugins konfigurieren; Administratorzugriff entspricht daher Codeausführung.

Der [Helm-Bereitstellungsleitfaden](deploy/helm/deer-flow/README.md) beschreibt den Ein-Gateway-Vertrag, angenommene Skill-Projektion, Zugangsdaten und das exakte Qualifikationsverfahren.

## Erweiterungsmodell

HartMesh bewahrt DeerFlows Modell für Skills, Werkzeuge, MCP-Server, benutzerdefinierte Agenten und Middleware.

Die hostunabhängige [`deerflow-extension-api`](backend/packages/extension-api/README.md) ergänzt typisierte Verträge für Autorisierung, Identität und Beiträge zu vertrauenswürdigem Kontext.

Sie umfasst außerdem einschränkende Limits, Fähigkeitsgesundheit und erforderliche MCP-Vorbereitung.

Python-Plugins sind vertrauenswürdiger Betreiber-Code, der beim Start aus der Liste `plugins:` in `config.yaml` geladen wird. Diese Liste bleibt absichtlich außerhalb der per API beschreibbaren `extensions_config.json`, die MCP- und Skill-Konfiguration enthält.

Ein angenommener Aufruf fixiert eine beim Start eingefrorene Erweiterungsgeneration. Skill-Änderungen betreffen spätere Annahmen; Plugin-Änderungen erfordern einen Gateway-Neustart für eine neue Generation. Bereits angenommene Arbeit wird durch beides nicht verändert.

Die verwaltete Lark/Feishu-CLI-Integration bleibt benutzerbezogen. Nach dem Verbinden kann **Change Lark app** App ID und App Secret dieses Benutzers ohne Neuinstallation des Skill-Pakets ersetzen: Die CLI prüft die neue App vor Aktivierung, entfernt OAuth-Tokens der vorherigen App und startet die Autorisierung der neuen. Bei Sandbox-Ausführung bleibt die Zugangsdaten enthaltende Konfigurationswurzel schreibgeschützt; ihr Unterverzeichnis `config/locks` wird separat für begrenzte Koordinationsschreibvorgänge der CLI eingehängt.

## Kompatibilität, Upstream-Basis und Release-Status

HartMesh bewahrt bestehende `deerflow.*`-Namespaces, Paketnamen, `DEER_FLOW_*`-Variablen, Docker-/Helm-Bezeichner, Dateisystempfade und Gateway-Kompatibilitätsoberflächen.

Der Produktvergleich verwendet den festen lokalen Bereich `e16ef2969b1446162e19af7bdde1446674851e66...4023cb434aa67011b9d18e90029f473b55323856`.

HartMesh `main` enthält Upstream `deerflow/main` bis `30788c79ffd988e110d97dd69fbc17abc50a96c6` (2026-09-02).

Dieser Synchronisierungspunkt ist Kontext, nicht die obige Vergleichsbasis, und HartMesh behauptet keine dauerhafte Überlegenheit.

Dieses Repository dokumentiert noch keinen HartMesh-Synchronisierungsrhythmus, kein API-/Konfigurations-/Datenbank-Kompatibilitätsfenster, Supportfenster, Verfahren zur Aufnahme von Sicherheitskorrekturen oder Upstream-Beitragsrichtlinie.

Behandle diese Hashes als Herkunftsnachweis, nicht als Wartungsversprechen.

Der Alembic-Graph hat einen Head: Von `0011_mcp_tasks` zweigen HartMesh-Migrationen bis `0019_inbound_event_identity` und Upstreams `0012_mcp_task_results` ab; `0020_merge_mcp_task_results` führt sie zusammen.

PostgreSQL-Betreiber sollten vor einem Rollback Schreibzugriffe stoppen und Daten sichern; nutze die Migrationshinweise in [backend/AGENTS.md](backend/AGENTS.md).

Versionsquellen melden `2.1.0`, aber kein Tag enthält die geprüfte HartMesh-Implementierung; Versionszeichenketten begründen kein HartMesh-Release.

[RELEASING.md](RELEASING.md) dokumentiert geerbte DeerFlow-Tagmechanik, keinen HartMesh-eigenen Releasekanal.

## Dokumentation

- [Runtime für dauerhafte Aufrufe](backend/docs/INVOCATION_RUNTIME.md) — Garantien, Nachweise, Wiederherstellung und zurückgestellter Umfang
- [Runtime-API](backend/packages/runtime-api/README.md) — DTOs und `DurableInvocationPort`
- [Gateway-API](backend/docs/API.md) — authentifiziertes HTTP-Verhalten
- [Erweiterungs-API](backend/packages/extension-api/README.md) — Richtlinien- und Vertrauensgrenzen
- [Helm-Bereitstellung](deploy/helm/deer-flow/README.md) — Ein-Gateway-Modi und Qualifikation
- [Konfiguration](config.example.yaml) — Betreibereinstellungen
- [Backend-Leitfaden](backend/AGENTS.md) und [Frontend-Leitfaden](frontend/AGENTS.md) — Architektur und Tests

## Support und Sicherheit

Führe lokale Diagnosen im Repository-Stamm aus:

```bash
make doctor
make support-bundle
```

Prüfe erzeugtes Supportmaterial vor der Weitergabe.

Dieses Repository dokumentiert noch keinen HartMesh-eigenen Issue-Tracker, Releasekanal oder privaten Meldeweg für Schwachstellen.

[CONTRIBUTING.md](CONTRIBUTING.md) und [SECURITY.md](SECURITY.md) behalten die Upstream-Ziele von ByteDance DeerFlow bei; diese Ziele sind kein HartMesh-eigener Support.

Veröffentliche keine Zugangsdaten, Tokens, privaten Prompts, Kundendaten oder Schwachstellendetails in öffentlichen Issues. Behandle interne Tokens, Webhook-Secrets, Provider-Schlüssel und Datenbankzugangsdaten als Geheimnisse.

## Mitwirken

Für lokale Arbeit gelten die geerbten Konventionen in [CONTRIBUTING.md](CONTRIBUTING.md) und der nächstgelegenen [AGENTS.md](AGENTS.md) für Befehle und Modulzuständigkeit.

## Lizenz

HartMesh behält DeerFlows [MIT-Lizenz](LICENSE) und bestehende Hinweise bei.

## Danksagung

HartMesh existiert, weil ByteDance und DeerFlow-Mitwirkende die Agentenbasis veröffentlicht haben, die es erweitert. Wir danken außerdem LangChain, LangGraph und den breiteren Open-Source-Ökosystemen.

HartMeshs Downstream-Betriebsaussagen, Release-Status, Qualifikation und Supportgrenzen bleiben eigenständig.
