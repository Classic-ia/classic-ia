# Arquitetura de Integração ATAK ↔ CQ

## Visão Geral

```
                       API ATAK
              (WRCAD009, WRMVE500,
               WRPRD600, WRLOG510, WREXP)
                        |
                        v
  +==========================================+
  |            n8n (Orquestrador)             |
  |                                          |
  |  [Cron 24h]   Sync Cadastros Mestres     |
  |                ├─ Fornecedores (381+)     |
  |                ├─ Motoristas (78+)        |
  |                ├─ Funcionários            |
  |                ├─ Veículos (64+)          |
  |                └─ Produtos (8+)           |
  |                                          |
  |  [Cron 30min] Sync Cargas                |
  |  [Cron 30min] Sync Embarques             |
  |  [Cron 60min] Sync Produção              |
  |  [Cron 60min] Sync Expedição             |
  |  [Cron 6h]    Score Fornecedor           |
  |  [Webhook]    Anti-Duplicidade           |
  +==========================================+
        |              |              |
        v              v              v
  +-----------+  +------------+  +----------+
  | Cadastros |  | Movimentos |  | Alertas  |
  | Mestres   |  | (Raw)      |  | & Score  |
  +-----------+  +------------+  +----------+
        |              |              |
        +------+-------+--------------+
               |
               v
  +==========================================+
  |         Supabase PostgreSQL              |
  |                                          |
  |  CADASTROS MESTRES                       |
  |    cadastros_atak (forn+mot+func)        |
  |    cq_veiculos (64 veículos)             |
  |    cq_produtos (8+ produtos)             |
  |    atak_cadastros_staging                |
  |                                          |
  |  MOVIMENTOS                              |
  |    atak_cargas_raw (com 7 vínculos FK)   |
  |    atak_embarques_raw                    |
  |    atak_producao_raw                     |
  |    atak_expedicao_raw                    |
  |                                          |
  |  CONTROLE                                |
  |    cq_validacao_divergencias             |
  |    cq_fornecedor_score                   |
  |    cq_alertas_qualidade                  |
  |    atak_sync_config / _log               |
  |                                          |
  |  TRIGGERS AUTOMÁTICOS                    |
  |    trg_validar_abc_total                 |
  |    trg_validar_contagem_frigo            |
  |    trg_resolver_vinculos_carga           |
  +==========================================+
               |
               v
  +==========================================+
  |         Classic CQ (Frontend)            |
  |                                          |
  |  Dashboard Divergências                  |
  |  Dashboard Fornecedores                  |
  |  Cargas com Vínculos Pendentes           |
  |  Fila de Inspeções                       |
  |  Alertas de Qualidade                    |
  +==========================================+
```

---

## Ordem de Execução (Dependência)

```
1° CADASTROS MESTRES (24h, 00:00)
   └─ Fornecedores → Motoristas → Funcionários → Veículos → Produtos

2° MOVIMENTOS (30-60 min, contínuo)
   └─ Cargas → Embarques → Produção → Expedição
   └─ Trigger: resolver vínculos com cadastros

3° SCORE & ALERTAS (6h)
   └─ Recalcula score de fornecedores
   └─ Gera alertas para críticos/bloqueados
```

Cadastros DEVEM ser importados ANTES dos movimentos para que os vínculos sejam resolvidos automaticamente.

---

## Fluxos n8n

### Fluxo 0: Sync Cadastros Mestres (diário, 00h)

```
Cron Diário 00h
    |
    +-> Buscar Configs de Cadastro (5 endpoints WRCAD*)
    +-> Criar Log de Sync
    |
    +-> Para cada tipo (fornecedor, motorista, funcionário, veículo, produto):
    |     +-> API ATAK - Buscar cadastros
    |     +-> Transformar → Staging (hash SHA256, normalizar campos)
    |     +-> Inserir em atak_cadastros_staging (dedup via hash)
    |
    +-> fn_promover_cadastros() → Upsert em tabelas finais
    |     ├─ cadastros_atak (fornecedor/motorista/funcionário)
    |     ├─ cq_veiculos (veículo)
    |     └─ cq_produtos (produto)
    |
    +-> Re-vincular cargas pendentes
    |     +-> Buscar vw_cargas_vinculos_pendentes
    |     +-> UPDATE para re-disparar trg_resolver_vinculos_carga
    |
    +-> Atualizar Log Final
```

**Arquivo:** `n8n_fluxo_sync_cadastros.json`

### Fluxo 1: Sync Cargas (a cada 30 min)

```
Cron 30min
    |
    +-> Buscar Config Sync (Supabase)
    +-> Criar Log de Sync
    |
    +-> API ATAK - Buscar Cargas (WRMVE500)
    |
    +-> Transformar Cargas (normalizar + hash SHA256)
    |
    +-> Upsert no Supabase (atak_cargas_raw)
    |       dedup via hash_registro UNIQUE
    |       trigger: trg_resolver_vinculos_carga (resolve FK automaticamente)
    |       trigger: trg_validar_abc_total (verifica A+B+C)
    |       trigger: trg_validar_contagem_frigo (cruza com cq_cargas)
    |
    +-> Validar Regras CQ (n8n Code)
    |       Regra 1: A+B+C = Total
    |       Regra 4: %C > limite = fornecedor crítico
    |       Regra 6: Vínculos cadastrais pendentes
    |
    +-> [Se divergência] -> Gravar em cq_validacao_divergencias
    |                    -> Gerar Alerta em cq_alertas_qualidade
    |
    +-> Atualizar Log de Sync (status final)
```

**Arquivo:** `n8n_fluxo_sync_cargas.json`

### Fluxo 2: Score Fornecedor (a cada 6h)

```
Cron 6h
    |
    +-> Listar Fornecedores Ativos (últimos 90 dias)
    +-> fn_atualizar_score_fornecedor() para cada um
    +-> Buscar Fornecedores Críticos
    +-> [Se crítico] -> Gerar Alerta
```

**Arquivo:** `n8n_fluxo_score_fornecedor.json`

### Fluxo 3: Anti-Duplicidade (webhook síncrono)

```
POST /webhook/atak-cq/check-duplicidade
    +-> Buscar por documento/PCR
    +-> [Duplicado]     -> 409 + registrar divergência
    +-> [Não duplicado] -> 200 ok
```

**Arquivo:** `n8n_fluxo_dedup_documentos.json`

---

## Vínculos Cadastrais na Carga

Cada registro em `atak_cargas_raw` possui 7 vínculos cadastrais resolvidos automaticamente:

| Campo Código | Campo ID | Tabela Destino | Tipo |
|-------------|----------|----------------|------|
| `fornecedor_codigo` | — | `cadastros_atak` (fornecedor) | Obrigatório |
| `motorista_codigo` | `motorista_id` | `cadastros_atak` (motorista) | Obrigatório |
| `cavalo_placa` | `cavalo_id` | `cq_veiculos` | Obrigatório |
| `carreta1_placa` | `carreta1_id` | `cq_veiculos` | Obrigatório |
| `carreta2_placa` | `carreta2_id` | `cq_veiculos` | Opcional |
| `recebedor_codigo` | `recebedor_id` | `cadastros_atak` (funcionário) | Obrigatório |
| `classificador_codigo` | `classificador_id` | `cadastros_atak` (funcionário) | Obrigatório |
| `produto_codigo` | `produto_id` | `cq_produtos` | Obrigatório |

O trigger `trg_resolver_vinculos_carga` resolve os IDs automaticamente no INSERT/UPDATE. Se o cadastro não existir, gera uma divergência do tipo `documento_ausente` em `cq_validacao_divergencias`.

A view `vw_cargas_vinculos_pendentes` lista todas as cargas com vínculos não resolvidos.

---

## Estrutura de Tabelas

### Cadastros Mestres (expandidos)

| Tabela | Registros | Campos Novos (sync) |
|--------|-----------|---------------------|
| `cadastros_atak` | 381 forn + 78 mot + func | cargo, setor, telefone, email, cnh, data_admissao, atak_id, sync_origem |
| `cq_veiculos` | 64 veículos | atak_id, codigo_atak, renavam, ano_fabricacao, capacidade_kg, funcao_veiculo, sync_origem |
| `cq_produtos` | 8+ produtos | atak_id, codigo_atak, unidade, peso_medio_kg, sync_origem |

### Tabelas de Integração

| Tabela | Propósito | Chave Única |
|--------|-----------|-------------|
| `atak_sync_config` | Configuração dos endpoints ATAK | `endpoint_nome` |
| `atak_sync_log` | Log de cada execução de sync | — |
| `atak_cadastros_staging` | Staging para cadastros antes de promoção | `hash_registro` |
| `atak_cargas_raw` | Dados brutos de cargas (7 vínculos FK) | `hash_registro` |
| `atak_embarques_raw` | Dados brutos de embarques | `hash_registro` |
| `atak_producao_raw` | Dados brutos de produção | `hash_registro` |
| `atak_expedicao_raw` | Dados brutos de expedição | `hash_registro` |
| `cq_validacao_divergencias` | Divergências detectadas | — |
| `cq_fornecedor_score` | Score contínuo do fornecedor | `fornecedor_codigo` |
| `n8n_webhook_tokens` | Tokens de autenticação n8n | `token` |

### Views

| View | Descrição |
|------|-----------|
| `vw_divergencias_ativas` | Divergências abertas com dados do fornecedor e score |
| `vw_fornecedores_criticos` | Fornecedores críticos/bloqueados com métricas |
| `vw_cadastros_resumo` | Totais por tipo (fornecedor, motorista, funcionário, veículo, produto) |
| `vw_motoristas_ativos` | Motoristas com CNH e placa padrão |
| `vw_funcionarios_ativos` | Funcionários por setor e cargo |
| `vw_cargas_vinculos_pendentes` | Cargas com vínculos cadastrais não resolvidos |

---

## Regras de Validação

### Regra 1: A + B + C = Total Classificado
- **Trigger:** INSERT/UPDATE em `atak_cargas_raw`
- **Gravidade:** Crítica
- **Ação:** `status_validacao = 'divergente'` + divergência

### Regra 2: Divergência Frigorífico vs Contagem Interna
- **Trigger:** INSERT em `atak_cargas_raw`
- **Condição:** `qtd_frigo` (ATAK) ≠ `qtd_classic` (cq_cargas, por PCR)
- **Gravidade:** Crítica se diferença > 10, Atenção se > 5

### Regra 3: Anti-Duplicidade de Carga/Documento
- **Mecanismo:** Hash SHA256 + UNIQUE INDEX + webhook síncrono
- **Ação:** Rejeita inserção + registra divergência

### Regra 4: Fornecedor Crítico (%C > Limite)
- **Limites configuráveis:** Atenção >10%, Crítico >15%, Bloqueio >22.5%
- **Score:** 100 - (%C × 2) - (divergências × 5)

### Regra 5: Validação de Dados Básicos
- Fornecedor obrigatório, data não futura, sem valores negativos

### Regra 6: Vínculos Cadastrais
- **Trigger:** `trg_resolver_vinculos_carga` em INSERT/UPDATE
- **Valida:** motorista, cavalo, carreta 1/2, recebedor, classificador, produto
- **Ação:** Resolve ID automaticamente ou gera divergência `documento_ausente`

### Regra 7: Validação de Cadastro
- Campos obrigatórios por tipo (CNPJ para fornecedor, CNH para motorista, setor para funcionário, placa para veículo, grupo para produto)

---

## Modelo de Sincronização Automática

### Schedule

| Fluxo | Intervalo | Endpoint ATAK | Destino |
|-------|-----------|---------------|---------|
| **Sync Cadastros** | **24h (00h)** | WRCAD009 + WRCAD* | `cadastros_atak`, `cq_veiculos`, `cq_produtos` |
| Sync Cargas | 30 min | WRMVE500 | `atak_cargas_raw` |
| Sync Embarques | 30 min | WRLOG510 | `atak_embarques_raw` |
| Sync Produção | 60 min | WRPRD600 | `atak_producao_raw` |
| Sync Expedição | 60 min | WREXP | `atak_expedicao_raw` |
| Score Fornecedor | 6h | — (interno) | `cq_fornecedor_score` + `_historico` |
| **SLA Monitor + Notificações** | **15 min** | — (interno) | `cq_fila_notificacao` → Slack/WhatsApp/Email |

### Pipeline de Dados

```
1. CADASTROS  Sync diário dos 5 cadastros mestres (staging → promoção)
2. FETCH      API ATAK retorna JSON com movimentos do período
3. HASH       SHA256(documento|pcr|fornecedor|data) para dedup
4. UPSERT     Insert com ON CONFLICT (hash_registro) = merge
5. LINK       Trigger resolve vínculos FK (motorista, veículo, funcionário, produto)
6. VALIDATE   Triggers executam regras 1-2, n8n executa regras 3-7
7. ALERT      Divergências críticas → cq_alertas_qualidade
8. SCORE      A cada 6h recalcula score de todos fornecedores
9. RE-LINK    Após sync de cadastros, re-resolve cargas pendentes
10. NOTIFY    Dashboard exibe alertas em tempo real
```

### Rastreabilidade

- `atak_sync_log` registra cada execução (início, fim, status, contagens)
- `n8n_execution_id` vincula execução do n8n ao log
- `atak_cadastros_staging` mantém histórico de ação (inserir/atualizar/ignorar/erro)
- `sync_origem` em cada cadastro indica se veio por n8n, ETL ou manual
- `atualizado_em` rastreia última modificação
- `cq_validacao_divergencias.detectado_por` identifica a origem da detecção

### Variáveis de Ambiente (n8n)

```
SUPABASE_URL=https://nvqxsulntpftcwtkjedu.supabase.co
SUPABASE_SERVICE_KEY=<service_role_key>
ATAK_API_TOKEN=<token_da_api_atak>
ATAK_API_BASE_URL=https://api.atak.com.br/v1
```

---

## Segurança

- **RLS ativo** em todas as tabelas de integração
- **service_role** usado apenas pelo n8n (nunca exposto no frontend)
- **anon key** no frontend com RLS restritivo
- **Webhook tokens** em tabela separada (`n8n_webhook_tokens`)
- **Audit trail** via `atak_sync_log` + `cq_validacao_divergencias`

---

## Módulo 3: Dashboard SLA de Sync

Monitora se todos os fluxos n8n estão rodando dentro do intervalo configurado.

### Views de SLA

| View | Descrição |
|------|-----------|
| `vw_sync_sla_status` | Status em tempo real: ok, atrasado, crítico, erro, nunca_executado |
| `vw_sync_sla_historico_24h` | Histórico das últimas 24h com intervalo real entre execuções |
| `vw_sync_sla_metricas_7d` | Métricas agregadas: taxa de sucesso, duração média, cobertura SLA |

### Classificação de SLA

| Status | Condição |
|--------|----------|
| `ok` | Última execução dentro do intervalo configurado |
| `atrasado` | Tempo desde último sync > 1.5x intervalo |
| `critico` | Tempo desde último sync > 2x intervalo |
| `erro` | Última execução terminou com erro |
| `nunca_executado` | Sem histórico de execução |

### Function

- `fn_verificar_sla_sync()` — Retorna endpoints com SLA violado (para o fluxo n8n)

---

## Módulo 4: Notificações (Slack / WhatsApp / Email)

Sistema de notificações multi-canal com fila, retry e audit trail.

### Tabelas

| Tabela | Propósito |
|--------|-----------|
| `cq_canais_notificacao` | Canais configurados (Slack, WhatsApp, Email, Webhook) |
| `cq_regras_notificacao` | Regras: quando notificar e em qual canal |
| `cq_fila_notificacao` | Fila de envio com status e retry automático |
| `cq_notificacao_log` | Log de cada tentativa de envio (audit) |

### Eventos Monitorados

| Evento | Quando dispara | Canal padrão |
|--------|---------------|--------------|
| `divergencia_critica` | Nova divergência gravidade=crítica | Slack |
| `fornecedor_bloqueado` | Status mudou para bloqueado | Slack + WhatsApp |
| `fornecedor_critico` | Status mudou para crítico | Slack |
| `sync_falhou` | Sync com status=erro | Slack |
| `sync_atrasado` | SLA violado (>1.5x intervalo) | Slack |
| `score_queda_brusca` | Score caiu >20 pontos em 24h | WhatsApp |
| `cadastro_nao_encontrado` | Vínculo cadastral não resolvido | Slack |

### Fluxo de Notificação

```
Evento → Trigger PG → fn_enfileirar_notificacao()
    → cq_fila_notificacao (status=pendente)
    → n8n (a cada 15min) busca pendentes
    → Switch por canal → Enviar (Slack/WhatsApp/Email/Webhook)
    → Registrar resultado → cq_notificacao_log
    → Se erro e tentativas < max → volta para pendente (retry)
```

---

## Módulo 5: Histórico de Score do Fornecedor

Registra snapshot do score a cada recálculo para gráfico de evolução temporal.

### Tabela

| Tabela | Propósito |
|--------|-----------|
| `cq_fornecedor_score_historico` | Snapshot completo: score, %A/%B/%C, status, variação |

### Trigger Automático

- `trg_historico_score` — A cada INSERT/UPDATE em `cq_fornecedor_score`, salva snapshot
- Calcula `variacao_score` (positivo=melhorou, negativo=piorou)
- Detecta `mudou_status` (ex: normal → crítico)
- Se queda > 20 pontos → dispara notificação `score_queda_brusca`

### Views

| View | Descrição |
|------|-----------|
| `vw_fornecedor_score_evolucao` | Série temporal por fornecedor (para gráfico) |
| `vw_fornecedor_ranking` | Ranking atual com tendência (melhorando/estável/piorando) e métricas 30d |

### Manutenção

- `fn_limpar_historico_score(365)` — Remove snapshots com mais de 365 dias

---

## Arquivos Entregues

```
integracao_atak_cq/
├── 00_migration_integracao.sql          # Tabelas raw, triggers ABC/frigo, functions, views, RLS
├── 01_migration_cadastros.sql           # Expand cadastros (5 tipos), vínculos FK, staging, promoção
├── 02_migration_guards.sql              # Constraints, auto-cálculo, imutabilidade, audit
├── 03_migration_sla_notif_historico.sql # SLA sync, notificações multi-canal, histórico score
├── n8n_fluxo_sync_cadastros.json        # Sync diário: 5 cadastros mestres + re-vínculo
├── n8n_fluxo_sync_cargas.json           # Sync 30min: cargas ATAK → CQ + validação
├── n8n_fluxo_score_fornecedor.json      # Score 6h: recalcula + alertas críticos
├── n8n_fluxo_sla_notificacoes.json      # SLA 15min + envio notificações (Slack/WhatsApp/Email)
├── n8n_fluxo_dedup_documentos.json      # Webhook: anti-duplicidade síncrono
├── regras_validacao.js                  # 7 regras JS (frontend + n8n)
└── ARQUITETURA_INTEGRACAO.md            # Este documento
```
