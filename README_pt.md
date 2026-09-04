# HartMesh

[English](README.md) | [简体中文](README_zh.md) | [日本語](README_ja.md) | [Français](README_fr.md) | [Русский](README_ru.md) | [Español](README_es.md) | Português | [Deutsch](README_de.md)

## A camada de execução confiável para agentes DeerFlow.

HartMesh é uma distribuição do DeerFlow voltada a operações, com invocações seguras para repetição, limites de política aplicáveis, evidências de ciclo de vida inspecionáveis e garantias de implantação que dizem exatamente o que cobrem.

Ela se baseia no workspace, sandboxes, memória, skills, ferramentas, subagentes, agendamentos e canais do DeerFlow.

> [!IMPORTANT]
> **Status: pré-lançamento.** Esta árvore de código contém o runtime implementado e evidências contratuais offline, mas ainda não há uma tag de lançamento do HartMesh que o inclua.
>
> A qualificação exata de uma implantação real continua sendo uma etapa separada e vinculada ao artefato.

HartMesh é uma distribuição downstream independente do [DeerFlow](https://github.com/bytedance/deer-flow), da ByteDance. Não é uma versão oficial do DeerFlow e não é afiliada nem endossada pela ByteDance.

[**Avaliar a prévia**](#início-rápido-do-workspace) · [**Inspecionar as evidências**](backend/docs/INVOCATION_RUNTIME.md) · [**Explorar o contrato do runtime**](backend/packages/runtime-api/README.md)

## Por que o HartMesh existe

Agentes são fáceis de iniciar e difíceis de operar.

Clientes repetem solicitações, políticas mudam, skills evoluem, processos falham e vários serviços podem precisar observar o mesmo trabalho.

HartMesh adiciona um limite de invocação confiável ao redor do DeerFlow para que o trabalho aceito possa ser repetido, governado, inspecionado e controlado de forma coerente.

Dentro de um mesmo escopo autenticado e enquanto a linha normal da execução for mantida, repetir a mesma solicitação estrita sob uma chave externa faz o HartMesh retornar o `run_id` retido.

Alterar a intenção canônica de execução sob essa chave gera um conflito.

Isso é admissão segura para repetição, não execução exatamente uma vez do modelo, ferramentas, provedores ou outros efeitos externos.

HartMesh foi projetado principalmente para:

- desenvolvedores de plataforma que incorporam trabalho do DeerFlow em APIs, serviços, agendamentos ou canais;
- operadores que avaliam uma topologia governada e durável com um único Gateway; e
- equipes que executam fluxos agendados ou assinados pelo GitHub de alto valor.

## Construído sobre o DeerFlow, reforçado para operações

DeerFlow fornece a base do agente: workspace, harness LangGraph, sandboxes, memória, skills, ferramentas, subagentes, agendamentos e canais nativos.

HartMesh mantém essa experiência e adiciona o plano de controle para aceitar, governar, observar e controlar trabalhos de longa duração.

Esta comparação usa um snapshot fixo. Consulte [compatibilidade e proveniência](#compatibilidade-baseline-upstream-e-status-da-versão) para os commits exatos; ela não é uma afirmação sobre todas as versões futuras do upstream.

| O que você mantém do baseline | O que o HartMesh adiciona |
| --- | --- |
| Workspace, harness, memória, sandboxes, skills, ferramentas, subagentes, agendamentos e canais | Um único limite de admissão ciente da origem; a durabilidade da entrega continua específica do transporte |
| Ciclo de vida de threads/runs, rotas REST do Gateway e rotas compatíveis com LangGraph | Chaves externas canônicas com escopo de origem, identidade de admissão retida e conflito explícito para mudança de intenção |
| Configuração de agentes, extensões e sandbox | Material de execução aceito e fixado |
| Superfícies de integração do Gateway e embarcadas | Registros estritos de `ensure`, `observe` e `control` cercado |
| Operação local e Helm | Evidências de ciclo de vida e relatórios explícitos de persistência, topologia e qualificação |

Você mantém a base de agentes e a superfície de compatibilidade do DeerFlow. Você ganha um plano de controle de invocações sustentado por evidências.

Você ainda não obtém HA ativa-ativa do Gateway, HA do agendador, retomada universal após falhas ou efeitos externos exatamente uma vez.

## O que acontece quando…?

| Cenário | Comportamento do HartMesh |
| --- | --- |
| [Um cliente repete após perder a resposta](backend/tests/test_invocation_idempotency.py) | Uma intenção canônica igual retorna a execução aceita; uma intenção diferente entra em conflito. |
| [Uma skill muda após a admissão](backend/tests/test_accepted_skill_snapshots.py) | A invocação aceita mantém uma árvore de skills capturada para consumidores do agente e do sandbox; trabalhos posteriores veem a alteração. |
| [Um sandbox remoto executa skills aceitas](backend/tests/test_kubernetes_accepted_skill_projection.py) | A projeção compatível `rwx_verified_copy_v2` vincula e revalida evidências de skill e isolamento antes do trabalho do grafo/modelo; a qualificação real entre nós continua vinculada ao artefato. |
| [Um serviço atua por uma pessoa ou observa outro proprietário](backend/tests/test_service_observation_grants.py) | Sujeito humano, serviço atuante e evidência de origem permanecem separados; observação entre proprietários exige concessão finita e autorização atual. |
| [Uma capacidade de política obrigatória fica indisponível](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | Prontidão e admissão realmente nova falham de forma fechada; repetição igual reutiliza evidência aceita e selada, enquanto a autorização atual de observação continua controlando a divulgação. |
| [Um operador pergunta o que é durável ou qualificado](backend/app/runtime/deployment.py) | O relatório nomeia a persistência; uma referência declarada é afirmada pelo operador, e apenas evidência exata comparada pelo [verificador offline](backend/scripts/verify_qualification_evidence.py) sustenta qualificação independente. |
| [O histórico é podado ou inconsistente](backend/tests/test_invocation_lifecycle_query.py) | A observação limitada retorna resultados tipados de cursor ou integridade em vez de apresentar silenciosamente histórico inválido. |
| [Uma entrega assinada do GitHub é interrompida ou sua thread está ocupada](backend/tests/test_durable_inbound_receipts.py) | A recuperação de recibos baseada em PostgreSQL pode reivindicar um lease expirado, preservar adiamento FIFO e convergir na mesma execução aceita. |

<!-- Demonstração futura: adicionar captura de terminal de 30–60 segundos com invocação por chave, resposta perdida simulada, repetição igual retornando o mesmo run_id, conflito por mudança de intenção e observação do ciclo de vida. -->

## Escolha seu caminho

| Caminho | Melhor para | Primeira prova |
| --- | --- | --- |
| [Workspace](#início-rápido-do-workspace) | Desenvolvedores avaliando o workspace completo do DeerFlow com controles HartMesh | Uma resposta do modelo pelo ponto de entrada local unificado |
| [Integração do runtime](#api-http-de-invocações-duráveis) | Plataformas incorporando trabalho de agentes em serviços, agendamentos ou canais | Solicitações iguais com chave retornam um `run_id`, seguidas de observação do ciclo de vida |

Operadores avaliando uma implantação governada podem ir diretamente aos [limites de implantação e durabilidade](#limites-de-implantação-e-durabilidade).

> [!CAUTION]
> **Conheça o limite antes de implantar.**
>
> - A topologia validada tem exatamente uma réplica do Gateway.
> - Não há alegação de HA ativa-ativa do Gateway, HA do agendador ou rollout sem indisponibilidade.
> - Admissão segura para repetição não significa efeitos externos exatamente uma vez nem retomada universal após falhas.
> - Memória local e entrada de canais por SQLite continuam em melhor esforço.
> - PostgreSQL e evidência exata verificada de forma independente são obrigatórios para as respectivas alegações de durabilidade compartilhada e qualificação abaixo.

## Início rápido do workspace

Trabalhe no checkout atual do HartMesh a partir da raiz do repositório.

Para esta prévia, tenha Python 3.12+, Node.js 22+, pnpm ou Corepack, `uv`, GNU Make, nginx, Docker ou Apple Container, credenciais de modelo e aproximadamente 4 núcleos de CPU e 8 GB de RAM. O desenvolvimento local no Windows usa Git Bash.

Quando `make setup` solicitar o modo de execução, escolha **Container sandbox**. Invocações duráveis usando `LocalSandboxProvider` funcionam apenas com um conjunto efetivo de skills explicitamente vazio; este checkout habilita skills integradas por padrão.

> [!WARNING]
> `make dev` é um caminho de desenvolvimento para rede confiável: Gateway `8001`, frontend `3000` e nginx local `2026` escutam em todas as interfaces. Use somente em máquina confiável ou protegida por firewall enquanto conclui a configuração do primeiro administrador.

Configure, diagnostique e inicie:

```bash
make check
make setup
make doctor
make dev
```

`make setup` grava a configuração local ignorada pelo Git. `make dev` verifica novamente as ferramentas, sincroniza dependências e inicia Gateway, frontend e nginx.

`make install` é opcional para contribuidores que também desejam hooks de pre-commit.

Abra [http://localhost:2026](http://localhost:2026). Em uma instalação nova, conclua a configuração do primeiro administrador, crie uma thread e envie um prompt.

Sucesso significa que o workspace transmite a resposta do modelo configurado pelo ciclo de vida apoiado pelo Gateway.

Pare o stack em outro terminal:

```bash
make stop
```

Isso avalia o workspace herdado e o stack local. Continue com o caminho do runtime para verificar o comportamento de `run_id` retido do HartMesh; o sucesso do workspace, sozinho, não comprova durabilidade PostgreSQL nem qualificação real do Kubernetes.

## API HTTP de invocações duráveis

Use a superfície de pré-lançamento `/api/runtime/v1` quando um serviço backend confiável precisar de `ensure → observe`.

Ela usa os registros estritos do [`deerflow-runtime-api`](backend/packages/runtime-api/README.md), que depende apenas da biblioteca padrão.

Esse caminho não exige o workspace do navegador, mas ainda requer um checkout configurado do HartMesh. Na raiz do repositório, avaliadores iniciantes devem executar:

```bash
make check
make setup
make doctor
```

Configure as credenciais do modelo selecionado por `make setup`. Se o workspace já foi configurado, pule esses comandos.

Avaliadores iniciantes do runtime também devem escolher **Container sandbox**. O provedor Local exige um conjunto efetivo de skills vazio para execução durável.

Como acima, `make dev` escuta em todas as interfaces nas portas `8001`, `3000` e `2026`; use uma máquina confiável ou protegida por firewall.

Se o `.env` da raiz já define um `DEER_FLOW_INTERNAL_AUTH_TOKEN` não vazio, use esse valor no terminal cliente.

Remova ou comente uma atribuição vazia, pois o carregamento do `.env` substitui a exportação do shell abaixo. Caso contrário, gere um token antes de iniciar o HartMesh:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Copie este token para o terminal do cliente confiável:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

Em um segundo terminal, exporte o token exibido e execute este cliente que usa apenas a biblioteca padrão:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN='<cole o token gerado>'

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
                    "content": "Explique a admissão segura para repetição em uma frase.",
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

Uma chave nova retorna `201 created`; sua repetição igual retorna `200 known` com o mesmo `run_id`. Altere a mensagem mantendo a chave para receber um `409 conflict` tipado, em vez de trabalho diferente sob a mesma chave.

Este exemplo usa o serviço integrado `gateway-internal` e omite deliberadamente a delegação humana. Nunca exponha o token interno a um navegador ou cliente não confiável.

A delegação de proprietário específica do runtime revalida `X-DeerFlow-Owner-User-Id` contra um usuário local existente.

Consulte a [projeção do principal](backend/app/gateway/services.py) e os [testes de identidade](backend/tests/test_invocation_identity_separation.py).

Para DTOs, paginação por cursor, falhas tipadas e cancelamento cercado, consulte o [contrato do runtime](backend/packages/runtime-api/README.md) e a [referência HTTP](backend/docs/API.md#durable-invocation-runtime-api).

Uma resposta a um pedido de esclarecimento inicia uma **nova invocação na mesma thread do DeerFlow**.

Pare o HartMesh na raiz do repositório ao concluir a avaliação:

```bash
make stop
```

## Valor operacional

### Admissão segura para repetição

Uma chave externa estável e a intenção canônica completa do chamador convergem em uma única invocação retida. Intenção igual retorna essa linha em qualquer estado do ciclo de vida; intenção alterada entra em conflito, e apenas o criador anexa um worker.

Essa garantia dura enquanto a linha normal da execução for mantida. Ela não deduplica efeitos externos arbitrários.

Evidências: [`idempotency.py`](backend/app/runtime/idempotency.py) e [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py).

### Um único limite de admissão

Rotas HTTP de criação, streaming e espera, tarefas agendadas, canais nativos autenticados e serviços embarcados entram no mesmo `InvocationRuntime`. Autenticação da origem, confirmação e durabilidade de entrada continuam específicas da origem.

Evidências: [`invocation.py`](backend/app/runtime/invocation.py) e a [matriz de fechamento](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix).

### Material de execução aceito e fixado

A admissão fixa a revisão do agente, geração de extensões, contexto confiável, evidência de restrições, pacotes efetivos de skills e perfil de execução/projeção.

Uma única árvore imutável de skills aceitas atende prompts, ativação, política e leituras de sandbox do agente principal e dos subagentes.

Skills aceitas não vazias usam um perfil compatível somente-aceito: AIO local baseado em contêiner ou Kubernetes com a projeção cercada `rwx_verified_copy_v2`.

`LocalSandboxProvider`, E2B, perfis personalizados e outros remotos continuam aceitando apenas conjunto vazio. Evidência de projeção offline não estabelece qualificação real entre nós.

Evidências: fontes de execução aceita em [`runtime/`](backend/packages/harness/deerflow/runtime/) — `accepted_invocation.py`, `agent_revision.py` e `skill_snapshot.py`.

Evidência remota: [`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) e [testes de projeção](backend/tests/test_kubernetes_accepted_skill_projection.py).

### Política que acompanha a execução

HartMesh mantém separados o sujeito efetivo, o serviço atuante e a evidência da origem.

Serviços autenticados continuam limitados ao proprietário, salvo concessão de um escopo finito de observação por um operador. A concessão limita a descoberta; a autorização atual ainda decide o que pode ser retornado, e o cancelamento não a herda.

Quando um operador habilita autorização de operações de invocação ou configura restrições v2 autoritativas, essas operações falham de forma fechada. A saúde de capacidades exigidas pelo operador e a preparação MCP também.

Autorização e controles de operações de invocação ficam desativados por padrão, e `required_capabilities` é vazio por padrão.

Middleware observacional opcional e o interceptor MCP legado configurável pela API mantêm comportamento aberto em falhas ou de avisar e ignorar.

As verificações de proprietário e rota permanecem, mas o HartMesh não fornece uma política organizacional universal nem garante ferramentas arbitrárias de terceiros.

Evidências: [contrato de extensão](backend/packages/extension-api/README.md), [autorização](backend/app/runtime/authorization.py), [restrições](backend/app/runtime/constraints.py) e [visibilidade](backend/app/runtime/visibility.py).

### Integração portátil do runtime

O `deerflow-runtime-api`, que usa apenas a biblioteca padrão, define registros estritos e imutáveis e um `DurableInvocationPort`: `ensure`, `observe` de invocação/contexto, `control` cercado e `capabilities`.

HTTP autenticado e o adaptador em processo hospedado pela aplicação compartilham esses registros e uma suíte de conformidade. O `DeerFlowClient` síncrono não é um adaptador durável; v1 não oferece push por broker, exportação de contexto nem retirada de contexto.

Evidências: [pacote do runtime](backend/packages/runtime-api/README.md) e [conformidade de transporte](backend/tests/test_runtime_api_conformance.py).

### Integridade transacional do ciclo de vida

Com o armazenamento SQL, uma mudança de estado e seu evento seguro de ciclo de vida são confirmados atomicamente sob uma única versão de estado. A observação limitada usa um snapshot autoritativo e retorna resultados tipados para histórico podado, futuro ou inconsistente.

Leitura repetível e comportamento entre sessões do PostgreSQL só são alegações de versão quando a etapa externa do PostgreSQL passa.

Evidências: [`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) e [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py).

Testes contratuais: [armazenamento atômico do ciclo de vida](backend/tests/test_invocation_lifecycle_store.py) e [consultas tipadas](backend/tests/test_invocation_lifecycle_query.py).

### Entrada durável assinada do GitHub

Com verificação HMAC e recibos PostgreSQL, a entrada assinada do GitHub persiste vínculos de origem limitados antes da confirmação.

Leases e fences podem reivindicar um lease expirado após interrupção, preservar FIFO quando a thread está ocupada e convergir na mesma invocação aceita.

Essa alegação limita-se à entrada assinada e verificada do GitHub com PostgreSQL; outros canais e caminhos locais continuam em melhor esforço.

Evidências: [armazenamento de recibos](backend/app/channels/inbound_receipts.py) e [testes de recibos](backend/tests/test_durable_inbound_receipts.py).

## Limites de implantação e durabilidade

Leia estes limites antes de seguir qualquer guia de implantação:

- A topologia validada tem exatamente uma réplica do Gateway.
- Não há alegação de HA ativa-ativa do Gateway, HA do agendador ou rollout sem indisponibilidade.
- Admissão segura para repetição não é execução universal exatamente uma vez de efeitos externos.
- Com armazenamento durável de invocações, a recuperação após perda do processo preserva evidência terminal autoritativa; ela não retoma de forma transparente cada grafo ou chamada de ferramenta.
- Memória é local ao processo, SQLite é durável no nó para estado de invocação e PostgreSQL é o armazenamento durável compartilhado.
- Entrada de canais por memória e SQLite continua em melhor esforço.
- Entrada nativa durável significa atualmente entrega assinada e verificada do GitHub com PostgreSQL.
- Skills aceitas não vazias exigem um caminho de sandbox compatível somente-aceito.
- Qualificação de Kubernetes/PostgreSQL exige evidência exata aprovada para imagem, chart, configuração, esquema, topologia, escopo e cenários nomeados.
- Uma etapa optativa coletada ou ignorada não é uma qualificação aprovada.

Para o snapshot auditado, as etapas externas optativas de PostgreSQL e Kubernetes não estavam configuradas e não havia artefato exato de qualificação aprovado.

Essas são etapas de lançamento não aprovadas, não evidência de ausência do comportamento implementado.

| Modo | Limite informado |
| --- | --- |
| `local_development` | Permite estado local ao processo sem alegação de durabilidade. |
| `durable_production` | Rejeita estado de invocação local ao processo na inicialização e prontidão. |
| Helm `local_evaluation` | Padrões de avaliação com um Gateway; explicitamente não qualificado. |
| Helm `durable_one_replica` | Exige imagens fixadas por digest, PostgreSQL/estado compartilhado e tempos seguros de sondas e desligamento; continua não qualificado sem evidência exata aprovada. |

O relatório administrativo de implantação separa nível de persistência, saúde, proveniência e qualificação.

Uma referência de qualificação fornecida continua `operator_asserted`; apenas o verificador offline pode estabelecer `external_evidence_verified` para evidência exata. Capacidades portáteis não carregam alegações de implantação.

`GET /health` informa a vida do processo. `GET /ready` é um sinal limitado de prontidão. Administradores inspecionam persistência e qualificação em `GET /api/runtime/v1/deployment`.

Evidências: [relatório de implantação](backend/app/runtime/deployment.py), [verificação de qualificação](backend/scripts/verify_qualification_evidence.py) e [contrato de implantação Helm](deploy/helm/deer-flow/README.md).

Com armazenamento durável de invocações, quando uma execução ativa é perdida com seu processo, a recuperação registra evidência terminal autoritativa como `stop_reason=orphan_recovered`.

Uma repetição igual retorna essa execução terminal retida. Continuar a intenção do produto exige uma nova invocação sob a nova geração do processo.

Consulte [ciclo de vida autoritativo e recuperação de falhas](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery).

### Limite de segurança

Os stacks Compose publicam apenas nginx e o vinculam a `127.0.0.1:2026` por padrão. Já o `make dev` local escuta em todas as interfaces nas portas `8001`, `3000` e `2026`; não o trate como se tivesse a mesma barreira de portas publicadas.

Agentes HartMesh podem executar comandos e ler ou gravar arquivos permitidos pelas ferramentas configuradas. O isolamento depende do provedor: `LocalSandboxProvider` compartilha a identidade do host Gateway e não é um limite de isolamento do sistema operacional.

Classificação de comandos e reescrita de caminhos são defesa em profundidade. Use um provedor isolado compatível para trabalho não confiável.

Conclua a configuração do primeiro administrador antes de tornar o serviço acessível além do loopback.

Administradores podem configurar processos MCP por stdio e plugins Python confiáveis; portanto, acesso de administrador equivale a execução de código.

Consulte o [guia de implantação Helm](deploy/helm/deer-flow/README.md) para o contrato de um Gateway, projeção de skills aceitas, credenciais e procedimento exato de qualificação.

## Modelo de extensões

HartMesh preserva o modelo de skills, ferramentas, servidores MCP, agentes personalizados e middleware do DeerFlow.

O [`deerflow-extension-api`](backend/packages/extension-api/README.md), independente do host, adiciona contratos tipados para autorização, identidade e contribuição de contexto confiável.

Também cobre restrições limitantes, saúde de capacidades e preparação MCP obrigatória.

Plugins Python são código confiável do operador carregado na inicialização pela lista `plugins:` em `config.yaml`. Essa lista fica intencionalmente fora de `extensions_config.json`, gravável pela API e responsável pela configuração de MCP e skills.

Uma invocação aceita fixa uma geração de extensões congelada na inicialização. Mudanças de skills afetam admissões posteriores; mudanças de plugins exigem reiniciar o Gateway para criar nova geração. Nenhuma delas altera trabalho já aceito.

A integração gerenciada do Lark/Feishu CLI permanece no escopo do usuário. Após conectar, **Change Lark app** pode substituir o App ID e App Secret desse usuário sem reinstalar o pacote de skills: a CLI valida o novo aplicativo antes de ativá-lo, remove os tokens OAuth do anterior e inicia a autorização do novo. Em execução com sandbox, a raiz de configuração que contém credenciais permanece somente leitura, enquanto `config/locks` é montado separadamente para gravações limitadas de coordenação da CLI.

## Compatibilidade, baseline upstream e status da versão

HartMesh preserva namespaces `deerflow.*`, nomes de pacotes, variáveis `DEER_FLOW_*`, identificadores Docker e Helm, caminhos do sistema de arquivos e superfícies de compatibilidade do Gateway.

A comparação do produto usa o intervalo local fixo `e16ef2969b1446162e19af7bdde1446674851e66...4023cb434aa67011b9d18e90029f473b55323856`.

O `main` do HartMesh incorpora o upstream `deerflow/main` até `30788c79ffd988e110d97dd69fbc17abc50a96c6` (2026-09-02).

Esse ponto de sincronização é contexto, não o baseline de comparação acima, e o HartMesh não faz alegação permanente de superioridade.

Este repositório ainda não documenta cadência de sincronização do HartMesh, janela de compatibilidade de API/configuração/banco de dados, período de suporte, política de entrada de correções de segurança ou política de contribuição upstream.

Trate esses hashes como proveniência, não como promessa de manutenção.

O grafo Alembic tem um único head: `0011_mcp_tasks` se ramifica nas migrações HartMesh até `0019_inbound_event_identity` e em `0012_mcp_task_results` do upstream; `0020_merge_mcp_task_results` as reúne.

Operadores PostgreSQL devem interromper escritores e fazer backup dos dados antes de reverter; use as orientações de migração em [backend/AGENTS.md](backend/AGENTS.md).

As fontes de versão informam `2.1.0`, mas nenhuma tag contém a implementação HartMesh auditada; strings de versão não estabelecem um lançamento HartMesh.

[RELEASING.md](RELEASING.md) documenta a mecânica herdada de tags do DeerFlow, não um canal próprio de lançamentos HartMesh.

## Documentação

- [Runtime de invocações duráveis](backend/docs/INVOCATION_RUNTIME.md) — garantias, evidências, recuperação e escopo adiado
- [API do runtime](backend/packages/runtime-api/README.md) — DTOs e `DurableInvocationPort`
- [API do Gateway](backend/docs/API.md) — comportamento HTTP autenticado
- [API de extensões](backend/packages/extension-api/README.md) — limites de política e confiança
- [Implantação Helm](deploy/helm/deer-flow/README.md) — modos de um Gateway e qualificação
- [Configuração](config.example.yaml) — opções do operador
- [Guia backend](backend/AGENTS.md) e [guia frontend](frontend/AGENTS.md) — arquitetura e testes

## Suporte e segurança

Execute diagnósticos locais na raiz do repositório:

```bash
make doctor
make support-bundle
```

Revise o material de suporte gerado antes de compartilhá-lo.

Este repositório ainda não documenta um rastreador de problemas, canal de lançamentos ou rota privada para vulnerabilidades próprios do HartMesh.

[CONTRIBUTING.md](CONTRIBUTING.md) e [SECURITY.md](SECURITY.md) mantêm os destinos upstream do ByteDance DeerFlow; esses destinos não são suporte próprio do HartMesh.

Não inclua credenciais, tokens, prompts privados, dados de clientes ou detalhes de vulnerabilidades em uma issue pública. Trate tokens internos, segredos de webhook, chaves de provedores e credenciais de banco de dados como segredos.

## Contribuição

Para trabalhar localmente, siga as convenções herdadas em [CONTRIBUTING.md](CONTRIBUTING.md) e o [AGENTS.md](AGENTS.md) mais próximo para comandos e propriedade de módulos.

## Licença

HartMesh mantém a [licença MIT](LICENSE) do DeerFlow e os avisos existentes.

## Agradecimentos

HartMesh existe porque a ByteDance e os colaboradores do DeerFlow publicaram a base de agentes que ele amplia. Também agradecemos aos ecossistemas de código aberto LangChain, LangGraph e outros.

As alegações operacionais downstream, o status de lançamento, a qualificação e os limites de suporte do HartMesh continuam sendo próprios.
