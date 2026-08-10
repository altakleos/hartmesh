# HartMesh

[English](README.md) | [简体中文](README_zh.md) | [日本語](README_ja.md) | Français | [Русский](README_ru.md) | [Español](README_es.md) | [Português](README_pt.md) | [Deutsch](README_de.md)

## La couche d’exécution fiable pour les agents DeerFlow.

HartMesh est une distribution DeerFlow orientée exploitation pour des invocations rejouables sans ambiguïté, des politiques applicables, des preuves de cycle de vie inspectables et des garanties de déploiement à portée explicite.

Elle s’appuie sur le workspace, les sandboxes, la mémoire, les skills, les outils, les sous-agents, les tâches planifiées et les canaux de DeerFlow.

> [!IMPORTANT]
> **Statut : préversion.** Cette arborescence contient le runtime implémenté et ses preuves contractuelles hors ligne, mais aucun tag de version HartMesh ne les contient encore.
>
> La qualification exacte d’un déploiement réel reste un contrôle distinct, lié à un artefact précis.

HartMesh est une distribution dérivée indépendante de [DeerFlow](https://github.com/bytedance/deer-flow) de ByteDance. Ce n’est pas une version officielle de DeerFlow et elle n’est ni affiliée ni approuvée par ByteDance.

[**Évaluer la préversion**](#démarrage-rapide-du-workspace) · [**Examiner les preuves**](backend/docs/INVOCATION_RUNTIME.md) · [**Explorer le contrat runtime**](backend/packages/runtime-api/README.md)

## Pourquoi HartMesh existe

Les agents sont faciles à démarrer et difficiles à exploiter.

Les clients réessaient, les politiques changent, les skills évoluent, les processus échouent et plusieurs services peuvent devoir observer le même travail.

HartMesh ajoute une frontière d’invocation fiable autour de DeerFlow afin que le travail accepté puisse être retenté, gouverné, inspecté et contrôlé de façon cohérente.

Dans un même périmètre authentifié, tant que la ligne de run ordinaire est conservée, répéter la même requête stricte sous une même clé externe renvoie le `run_id` conservé.

Modifier l’intention d’exécution canonique sous cette clé provoque un conflit.

Il s’agit d’une admission sûre en cas de répétition, pas d’une exécution exactement une fois du modèle, des outils, des fournisseurs ou d’autres effets externes.

HartMesh vise d’abord :

- les développeurs de plateforme qui intègrent DeerFlow dans des API, services, tâches planifiées ou canaux ;
- les opérateurs qui évaluent une topologie gouvernée et durable à un seul Gateway ; et
- les équipes qui automatisent des workflows planifiés ou GitHub signés à forte valeur.

## Fondé sur DeerFlow, renforcé pour l’exploitation

DeerFlow fournit le socle agentique : workspace, harness LangGraph, sandboxes, mémoire, skills, outils, sous-agents, tâches planifiées et canaux natifs.

HartMesh conserve cette expérience et ajoute le plan de contrôle qui accepte, gouverne, observe et contrôle les travaux longs.

Cette comparaison vise un instantané fixe. Les commits exacts figurent dans [compatibilité et provenance](#compatibilité-référence-amont-et-statut-de-publication) ; elle ne décrit pas toutes les futures versions amont.

| Ce que vous conservez du socle | Ce que HartMesh ajoute autour |
| --- | --- |
| Workspace, harness, mémoire, sandboxes, skills, outils, sous-agents, tâches et canaux | Une frontière d’admission sensible à la source ; la durabilité de livraison dépend toujours du transport |
| Cycle de vie thread/run DeerFlow, routes REST Gateway et routes compatibles LangGraph | Clés externes canoniques par source, identité d’admission conservée et conflit explicite si l’intention change |
| Configuration des agents, extensions et sandboxes | Matériaux d’exécution acceptés et figés |
| Surfaces d’intégration Gateway et embarquée | Enregistrements stricts `ensure`, `observe` et `control` avec fencing |
| Exploitation locale et Helm | Preuves de cycle de vie et rapports explicites de persistance, topologie et qualification |

Vous conservez le socle agentique et la surface compatible de DeerFlow. Vous gagnez un plan de contrôle des invocations accompagné de preuves.

Vous n’obtenez pas de Gateway actif-actif, de haute disponibilité du scheduler, de reprise universelle après crash ni d’effets externes exactement une fois.

## Que se passe-t-il si… ?

| Scénario | Comportement de HartMesh |
| --- | --- |
| [Un client réessaie après avoir perdu la réponse](backend/tests/test_invocation_idempotency.py) | Une intention canonique identique renvoie le run accepté ; une intention modifiée provoque un conflit. |
| [Un skill change après l’admission](backend/tests/test_accepted_skill_snapshots.py) | L’invocation acceptée conserve un arbre de skills capturé commun aux consommateurs agent et sandbox ; seuls les travaux ultérieurs voient la modification. |
| [Une sandbox distante exécute les skills acceptés](backend/tests/test_kubernetes_accepted_skill_projection.py) | La projection `rwx_verified_copy_v2` prise en charge lie et revérifie les preuves de skill admis et d’isolation avant le travail graph/model ; la qualification inter-nœuds réelle reste liée à un artefact exact. |
| [Un service agit pour une personne ou observe un autre propriétaire](backend/tests/test_service_observation_grants.py) | Sujet humain, service agissant et preuve de source restent distincts ; l’observation inter-propriétaires exige un droit limité et l’autorisation courante. |
| [Une capacité de politique requise est défaillante](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | La readiness et toute admission réellement nouvelle échouent fermées ; un rejeu identique réutilise les preuves d’acceptation scellées, tandis que l’autorisation courante régit toujours la divulgation. |
| [Un opérateur demande ce qui est durable ou qualifié](backend/app/runtime/deployment.py) | Le rapport nomme le niveau de persistance ; une référence déclarée reste une assertion opérateur, et seule une preuve exacte validée par le [vérificateur hors ligne](backend/scripts/verify_qualification_evidence.py) permet une qualification indépendante. |
| [L’historique est élagué ou incohérent](backend/tests/test_invocation_lifecycle_query.py) | L’observation bornée renvoie des résultats typés de curseur ou d’intégrité au lieu d’afficher silencieusement un historique invalide. |
| [Une livraison GitHub signée est interrompue ou son thread est occupé](backend/tests/test_durable_inbound_receipts.py) | La récupération des reçus PostgreSQL peut reprendre un lease expiré, préserver le report FIFO et converger vers le même run accepté. |

<!-- Démo future : ajouter une capture terminal de 30–60 s montrant une invocation avec clé, une réponse perdue, le même run_id au rejeu, un conflit d’intention et l’observation du cycle de vie. -->

## Choisissez votre parcours

| Parcours | Pour qui | Première preuve |
| --- | --- | --- |
| [Workspace](#démarrage-rapide-du-workspace) | Développeurs évaluant le workspace DeerFlow complet avec les contrôles HartMesh | Une réponse du modèle via le point d’entrée local unifié |
| [Intégration runtime](#api-http-durable-invocation) | Plateformes intégrant le travail agentique dans des services, tâches ou canaux | Deux requêtes portant la même clé renvoient un seul `run_id`, puis le cycle de vie est observable |

Les opérateurs qui évaluent un déploiement gouverné peuvent aller directement aux [limites de déploiement et de durabilité](#limites-de-déploiement-et-de-durabilité).

> [!CAUTION]
> **Connaissez la limite avant de déployer.**
>
> - La topologie validée comporte exactement un replica Gateway.
> - Aucune haute disponibilité active-active du Gateway ou du scheduler, ni aucun rollout sans interruption, n’est revendiqué.
> - L’admission sûre en cas de rejeu ne garantit ni les effets externes exactement une fois ni la reprise universelle après crash.
> - Les entrées de canaux locales en mémoire ou SQLite restent best-effort.
> - PostgreSQL et des preuves exactes vérifiables indépendamment sont requis pour les affirmations shared-durable et de qualification ci-dessous.

## Démarrage rapide du workspace

Travaillez depuis la racine du checkout HartMesh courant.

Cette préversion nécessite Python 3.12+, Node.js 22+, pnpm ou Corepack, `uv`, GNU Make, nginx, Docker ou Apple Container, des identifiants de modèle, environ 4 cœurs CPU et 8 Go de RAM. Sous Windows, le développement local utilise Git Bash.

Quand `make setup` demande le mode d’exécution, choisissez **Container sandbox**. Avec `LocalSandboxProvider`, les invocations durables exigent un ensemble effectif de skills vide ; les skills intégrés sont activés ici par défaut.

> [!WARNING]
> `make dev` vise un réseau de confiance : Gateway `8001`, frontend `3000` et nginx `2026` écoutent sur toutes les interfaces. Pendant la création du premier admin, utilisez une machine fiable ou protégée par son firewall.

Configurez, diagnostiquez et lancez :

```bash
make check
make setup
make doctor
make dev
```

`make setup` écrit la configuration locale ignorée par Git. `make dev` revérifie les outils, synchronise les dépendances et démarre Gateway, frontend et nginx.

`make install` est facultatif pour les contributeurs qui souhaitent aussi installer les hooks pre-commit.

Ouvrez [http://localhost:2026](http://localhost:2026). Sur une nouvelle installation, terminez la création du premier admin, créez un thread et envoyez un prompt.

Le succès correspond à une réponse du modèle configuré diffusée dans le workspace via le cycle de vie géré par le Gateway.

Arrêtez la stack depuis un autre terminal :

```bash
make stop
```

Ce parcours évalue le workspace hérité et la stack locale. Continuez avec le runtime pour vérifier la conservation du `run_id` ; ce succès seul ne démontre ni la durabilité PostgreSQL ni une qualification Kubernetes réelle.

## API HTTP Durable Invocation

Utilisez la surface de préversion `/api/runtime/v1` lorsqu’un service backend de confiance a besoin de `ensure → observe`.

Elle utilise les enregistrements stricts du package [`deerflow-runtime-api`](backend/packages/runtime-api/README.md), limité à la bibliothèque standard.

Le workspace navigateur n’est pas nécessaire, mais le checkout HartMesh doit être configuré. Depuis la racine du dépôt, un nouvel évaluateur exécute :

```bash
make check
make setup
make doctor
```

Configurez les identifiants du modèle choisi par `make setup`. Si le workspace est déjà configuré, ignorez ces commandes.

Un premier essai runtime doit aussi choisir **Container sandbox**. Le provider Local exige un ensemble effectif de skills vide pour l’exécution durable.

Comme ci-dessus, `make dev` écoute sur toutes les interfaces de l’hôte aux ports `8001`, `3000` et `2026` ; utilisez une machine de confiance ou protégée par son firewall.

Si le fichier `.env` racine définit déjà une valeur non vide pour `DEER_FLOW_INTERNAL_AUTH_TOKEN`, utilisez-la dans le terminal client.

Supprimez ou commentez une affectation vide, car le chargement de `.env` remplace l’export shell ci-dessous. Sinon, générez un token avant de démarrer HartMesh :

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Copy this token into the trusted client terminal:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

Dans un second terminal, exportez le token affiché et exécutez ce client basé uniquement sur la bibliothèque standard :

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

Une clé neuve renvoie `201 created` ; son rejeu identique renvoie `200 known` avec le même `run_id`. Modifier le message tout en conservant la clé produit un `409 conflict` typé au lieu de lancer un travail différent sous cette clé.

Cet exemple authentifie le service intégré `gateway-internal` et omet volontairement la délégation humaine. N’exposez jamais le token interne à un navigateur ou à un client non fiable.

La délégation de propriétaire propre au runtime revérifie `X-DeerFlow-Owner-User-Id` par rapport à un utilisateur local existant.

Consultez la [projection du principal](backend/app/gateway/services.py) et les [tests d’identité](backend/tests/test_invocation_identity_separation.py).

Pour les DTO, la pagination par curseur, les échecs typés et l’annulation avec fencing, consultez le [contrat runtime](backend/packages/runtime-api/README.md) et la [référence HTTP](backend/docs/API.md#durable-invocation-runtime-api).

Une réponse de clarification démarre une **nouvelle invocation sur le même thread DeerFlow**.

À la fin de l’évaluation, arrêtez HartMesh depuis la racine :

```bash
make stop
```

## Valeur opérationnelle

### Admission sûre en cas de rejeu

Une clé externe stable et l’intention canonique complète convergent vers une invocation conservée. L’intention identique renvoie cette ligne dans tout état ; une modification provoque un conflit, et seul le créateur attache un worker.

La garantie dure tant que la ligne de run ordinaire est conservée. Elle ne déduplique pas les effets externes arbitraires.

Preuves : [`idempotency.py`](backend/app/runtime/idempotency.py) et [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py).

### Une frontière d’admission

Les routes HTTP create, stream et wait, les tâches, les canaux natifs authentifiés et les services embarqués entrent dans le même `InvocationRuntime`. Authentification, acquittement et durabilité restent propres à la source.

Preuves : [`invocation.py`](backend/app/runtime/invocation.py) et [matrice de clôture](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix).

### Matériaux d’exécution acceptés et figés

L’admission fige la révision de l’agent, la génération d’extensions, le contexte fiable, les preuves de contraintes, les packages de skills effectifs et le profil d’exécution/projection.

Un même arbre immuable de skills acceptés alimente les prompts lead et subagent, l’activation, la politique et les lectures sandbox.

Les skills acceptés non vides utilisent un profil accepted-only pris en charge : AIO local adossé à un conteneur ou Kubernetes avec projection `rwx_verified_copy_v2` protégée par fencing.

`LocalSandboxProvider`, E2B, les providers custom et autres profils distants restent empty-only. Les preuves de projection hors ligne n’établissent pas une qualification inter-nœuds réelle.

Preuves : sources d’exécution acceptée dans [`runtime/`](backend/packages/harness/deerflow/runtime/) — `accepted_invocation.py`, `agent_revision.py` et `skill_snapshot.py`.

Preuves distantes : [`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) et [tests de projection](backend/tests/test_kubernetes_accepted_skill_projection.py).

### Une politique qui suit l’exécution

HartMesh maintient distincts le sujet effectif, le service agissant et les preuves de source.

Les services authentifiés restent limités au propriétaire, sauf droit d’observation fini. Ce droit borne la découverte ; l’autorisation courante décide toujours des données retournées et l’annulation n’en hérite pas.

Quand un opérateur active l’autorisation des opérations d’invocation ou configure des contraintes v2 faisant autorité, ces opérations nommées échouent fermées. Les capacités requises et la préparation MCP suivent la même règle.

L’autorisation et le contrôle des opérations d’invocation sont désactivés par défaut, et `required_capabilities` vaut une liste vide.

Le middleware d’observation optionnel et l’ancien intercepteur MCP modifiable par API restent fail-open ou avertissent puis ignorent.

Les contrôles de propriétaire et de route restent actifs, mais HartMesh ne fournit pas une politique d’organisation universelle et ne garantit pas les outils tiers arbitraires.

Preuves : [contrat d’extension](backend/packages/extension-api/README.md), [autorisation](backend/app/runtime/authorization.py), [contraintes](backend/app/runtime/constraints.py) et [visibilité](backend/app/runtime/visibility.py).

### Intégration runtime portable

`deerflow-runtime-api`, limité à la bibliothèque standard, définit des enregistrements stricts et immuables et un `DurableInvocationPort` : `ensure`, `observe` pour invocation/contexte, `control` avec fencing et `capabilities`.

HTTP authentifié et l’adapter embarqué dans l’application partagent ces enregistrements et une suite de conformité. Le `DeerFlowClient` synchrone n’est pas un adapter durable ; v1 n’offre ni broker push, ni export, ni retrait de contexte.

Preuves : [package runtime](backend/packages/runtime-api/README.md) et [conformité des transports](backend/tests/test_runtime_api_conformance.py).

### Intégrité transactionnelle du cycle de vie

Avec le store SQL, changement d’état et événement sûr sont validés atomiquement sous une version. L’observation bornée utilise un snapshot faisant autorité et renvoie des résultats typés pour un historique élagué, futur ou incohérent.

Le repeatable-read PostgreSQL et le comportement inter-sessions ne deviennent des affirmations de version que si le contrôle PostgreSQL externe réussit.

Preuves : [`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) et [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py).

Tests contractuels : [store atomique](backend/tests/test_invocation_lifecycle_store.py) et [requêtes de cycle de vie typées](backend/tests/test_invocation_lifecycle_query.py).

### Entrée GitHub signée et durable

Avec vérification HMAC et reçus PostgreSQL, l’entrée GitHub signée persiste des liaisons de source bornées avant acquittement.

Les leases et fences peuvent récupérer un lease expiré après interruption, préserver le FIFO d’un thread occupé et converger vers la même invocation acceptée.

Cette affirmation se limite à l’entrée GitHub signée vérifiée avec PostgreSQL ; les autres canaux et chemins locaux restent best-effort.

Preuves : [store de reçus](backend/app/channels/inbound_receipts.py) et [tests des reçus](backend/tests/test_durable_inbound_receipts.py).

## Limites de déploiement et de durabilité

Lisez ces limites avant de suivre un guide de déploiement :

- La topologie validée comporte exactement un replica Gateway.
- Aucune HA active-active du Gateway ou du scheduler, ni aucun rollout sans interruption, n’est revendiqué.
- L’admission sûre en cas de rejeu n’est pas l’exécution universelle exactement une fois des effets externes.
- Avec un stockage d’invocations durable, la récupération après perte de processus conserve les preuves terminales faisant autorité ; elle ne reprend pas chaque appel graph ou outil.
- Memory est process-local, SQLite rend l’état d’invocation durable sur un nœud, PostgreSQL est le store shared-durable.
- L’entrée des canaux natifs avec Memory ou SQLite reste best-effort.
- L’entrée native durable désigne actuellement la livraison GitHub signée vérifiée avec PostgreSQL.
- Les skills acceptés non vides nécessitent un chemin sandbox accepted-only pris en charge.
- La qualification Kubernetes/PostgreSQL exige une preuve exacte et réussie pour l’image, le chart, la configuration, le schéma, la topologie, le périmètre et les scénarios nommés.
- Un contrôle optionnel collecté ou ignoré ne constitue pas une réussite.

Pour le snapshot audité, les contrôles externes PostgreSQL et Kubernetes n’étaient pas configurés et aucun artefact exact de qualification réussie n’était présent.

Ce sont des contrôles de publication non franchis, pas la preuve d’une absence d’implémentation.

| Mode | Limite rapportée |
| --- | --- |
| `local_development` | Autorise l’état process-local sans prétention de durabilité. |
| `durable_production` | Refuse l’état d’invocation process-local au démarrage et à la readiness. |
| Helm `local_evaluation` | Valeurs d’évaluation à un Gateway ; explicitement non qualifié. |
| Helm `durable_one_replica` | Exige des images épinglées par digest, PostgreSQL/état partagé, des probes et délais d’arrêt sûrs ; reste non qualifié sans preuve exacte réussie. |

Le rapport administrateur sépare niveau de persistance, santé, provenance et qualification.

Une référence fournie reste `operator_asserted` ; seul le vérificateur hors ligne peut établir `external_evidence_verified` pour une preuve exacte. Les capabilities portables ne contiennent aucune affirmation de déploiement.

`GET /health` indique la vie du processus. `GET /ready` donne un signal borné ready/not-ready. Les administrateurs consultent persistance et qualification via `GET /api/runtime/v1/deployment`.

Preuves : [rapport de déploiement](backend/app/runtime/deployment.py), [vérification de qualification](backend/scripts/verify_qualification_evidence.py) et [contrat Helm](deploy/helm/deer-flow/README.md).

Avec un stockage d’invocations durable, si une invocation active disparaît avec son processus, la récupération enregistre une preuve terminale faisant autorité comme `stop_reason=orphan_recovered`.

Un rejeu identique renvoie ce run terminal conservé. Poursuivre l’intention produit exige une nouvelle invocation sous la nouvelle génération du processus.

Consultez [cycle de vie faisant autorité et récupération](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery).

### Limite de sécurité

Les stacks Compose ne publient que nginx et le lient par défaut à `127.0.0.1:2026`. En local, `make dev` écoute au contraire sur toutes les interfaces pour `8001`, `3000` et `2026` ; ne lui attribuez pas la même protection de ports publiés.

Les agents HartMesh exécutent des commandes et accèdent aux fichiers permis par les outils. L’isolation dépend du provider : `LocalSandboxProvider` partage l’identité hôte du Gateway et n’est pas une frontière système.

La classification des commandes et la réécriture des chemins sont une défense en profondeur. Utilisez un provider isolé pris en charge pour du travail non fiable.

Terminez la création du premier admin avant de rendre le service accessible au-delà du loopback.

Les administrateurs peuvent configurer des processus MCP stdio et des plugins Python de confiance ; leur accès équivaut donc à l’exécution de code.

Consultez le [guide Helm](deploy/helm/deer-flow/README.md) pour le contrat à un Gateway, la projection des skills acceptés, les secrets et la procédure de qualification exacte.

## Modèle d’extension

HartMesh préserve le modèle DeerFlow de skills, outils, serveurs MCP, agents personnalisés et middlewares.

Le package indépendant de l’hôte [`deerflow-extension-api`](backend/packages/extension-api/README.md) ajoute des contrats typés pour l’autorisation, l’identité et la contribution au contexte fiable.

Il couvre aussi les contraintes restrictives, la santé des capacités et la préparation MCP requise.

Les plugins Python sont du code opérateur fiable, chargé au démarrage depuis `plugins:` dans `config.yaml`. Cette liste reste hors de `extensions_config.json`, modifiable par API et responsable des MCP et skills.

Une invocation acceptée fige une génération d’extensions au démarrage. Les changements de skills affectent les admissions futures ; ceux des plugins exigent un redémarrage Gateway. Aucun ne modifie le travail déjà accepté.

## Compatibilité, référence amont et statut de publication

HartMesh préserve les namespaces `deerflow.*`, noms de packages, variables `DEER_FLOW_*`, identifiants Docker/Helm, chemins et surfaces compatibles du Gateway.

La comparaison produit est la plage locale fixe `e16ef2969b1446162e19af7bdde1446674851e66...ca2400f3059b3ac93249473e97ed83c5296fb0f0`.

Lors de l’audit du 2026-08-09, le snapshot `deerflow/main` examiné séparément était `e401ae2d7b8e4fc73fc82a1143c989c54f3f4de6`, avec un commit amont uniquement au-delà de la base partagée.

Il s’agit de contexte, pas de la référence ci-dessus, et HartMesh ne revendique pas une supériorité permanente sur l’amont.

Ce dépôt ne documente pas encore de cadence de synchronisation HartMesh, fenêtre de compatibilité API/configuration/base, durée de support, politique d’intégration des correctifs de sécurité ou politique de contribution amont.

Ces hashes indiquent la provenance, pas une promesse de maintenance.

Le graphe Alembic est linéaire dans ce checkout : `0011_mcp_tasks` → `0011_accepted_invocation` → migrations d’invocation jusqu’à `0019_inbound_event_identity`.

Les opérateurs PostgreSQL doivent arrêter les écritures et sauvegarder avant un rollback ; consultez les migrations dans [backend/AGENTS.md](backend/AGENTS.md).

Les sources de version indiquent `2.1.0`, mais aucun tag ne contient l’implémentation HartMesh auditée ; cette chaîne n’établit pas une version HartMesh.

[RELEASING.md](RELEASING.md) décrit les mécanismes de tags DeerFlow hérités, pas un canal de publication HartMesh.

## Documentation

- [Runtime d’invocation durable](backend/docs/INVOCATION_RUNTIME.md) — garanties, preuves, récupération et périmètre différé
- [API runtime](backend/packages/runtime-api/README.md) — DTO et `DurableInvocationPort`
- [API Gateway](backend/docs/API.md) — comportement HTTP authentifié
- [API d’extension](backend/packages/extension-api/README.md) — politiques et limites de confiance
- [Déploiement Helm](deploy/helm/deer-flow/README.md) — modes à un Gateway et qualification
- [Configuration](config.example.yaml) — paramètres opérateur
- [Guide backend](backend/AGENTS.md) et [guide frontend](frontend/AGENTS.md) — architecture et tests

## Support et sécurité

Lancez les diagnostics locaux depuis la racine :

```bash
make doctor
make support-bundle
```

Relisez les éléments générés avant de les partager.

Ce dépôt ne documente pas encore de tracker, canal de publication ou voie privée de signalement de vulnérabilité propres à HartMesh.

[CONTRIBUTING.md](CONTRIBUTING.md) et [SECURITY.md](SECURITY.md) conservent les destinations amont de ByteDance DeerFlow ; elles ne constituent pas un support HartMesh.

Ne publiez pas d’identifiants, tokens, prompts privés, données client ou détails de vulnérabilité. Traitez tokens internes, secrets webhook, clés fournisseur et identifiants de base comme des secrets.

## Contribuer

Pour le travail local, suivez les conventions héritées dans [CONTRIBUTING.md](CONTRIBUTING.md) et le fichier [AGENTS.md](AGENTS.md) le plus proche pour les commandes et la responsabilité des modules.

## Licence

HartMesh conserve la [licence MIT](LICENSE) de DeerFlow et les mentions existantes.

## Remerciements

HartMesh existe grâce à ByteDance et aux contributeurs DeerFlow qui ont publié le socle agentique qu’il étend. Merci aussi aux écosystèmes LangChain, LangGraph et agents open source.

Les affirmations opérationnelles, le statut de publication, la qualification et les limites de support de HartMesh relèvent de HartMesh.
