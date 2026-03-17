# Arquitetura de Integrao ATAK  CQ

## Viso Geral

```
                    API ATAK
           (WRCAD009, WRMVE500,
            WRPRD600, WRLOG510)
                     |
                     v
        +========================+
        |       n8n (Orquestrador)       |
        |                                |
        |  [Cron 30min] Sync Cargas      |
        |  [Cron 60min] Sync Produo   |
        |  [Cron 30min] Sync Embarques   |
        |  [Cron 6h]    Score Fornecedor |
        |  [Webhook]    Anti-Duplicidade |
        +========================+
             |           |          |
             v           v          v
    +--------+---+  +----+----+  +--+--------+
    | Raw Tables |  | Validao|  | Alertas   |
    | (staging)  |  | Engine  |  | Qualidade |
    +--------+---+  +----+----+  +--+--------+
             |           |          |
             +-----+-----+----------+
                   |
                   v
        +========================+
        |    Supabase PostgreSQL          |
        |                                |
        |  atak_cargas_raw               |
        |  atak_embarques_raw            |
        |  atak_producao_raw             |
        |  atak_expedicao_raw            |
        |  cq_validacao_divergencias     |
        |  cq_fornecedor_score           |
        |  cq_alertas_qualidade          |
        |  atak_sync_config / _log       |
        +========================+
                   |
                   v
        +========================+
        |    Classic CQ (Frontend)        |
        |                                |
        |  Dashboard Divergncias       |
        |  Dashboard Fornecedores        |
        |  Fila de Inspees            |
        |  Alertas de Qualidade          |
        +========================+
```

---

## Fluxos n8n

### Fluxo 1: Sync Cargas (a cada 30 min)

```
Cron 30min
    |
    +-> Buscar Config Sync (Supabase)
    |
    +-> Criar Log de Sync
    |
    +-> API ATAK - Buscar Cargas (WRMVE500)
    |
    +-> Transformar Cargas (normalizar + hash SHA256)
    |
    +-> Upsert no Supabase (atak_cargas_raw)
    |       dedup via hash_registro UNIQUE
    |
    +-> Validar Regras CQ
    |       Regra 1: A+B+C = Total
    |       Regra 4: %C > limite = fornecedor crtico
    |
    +-> [Se divergncia] -> Gravar em cq_validacao_divergencias
    |                    -> Gerar Alerta em cq_alertas_qualidade
    |
    +-> Atualizar Log de Sync (status final)
```

**Arquivo:** `n8n_fluxo_sync_cargas.json`

### Fluxo 2: Score Fornecedor (a cada 6h)

```
Cron 6h
    |
    +-> Listar Fornecedores Ativos (ltimos 90 dias)
    |
    +-> Para cada fornecedor:
    |       fn_atualizar_score_fornecedor()
    |       - Agregar cargas dos ltimos 90 dias
    |       - Calcular %A, %B, %C
    |       - Contar divergncias abertas
    |       - Score = 100 - (%C * 2) - (divergncias * 5)
    |       - Status: normal | ateno | crtico | bloqueado
    |
    +-> Buscar Fornecedores Crticos
    |
    +-> [Se crtico] -> Gerar Alerta em cq_alertas_qualidade
```

**Arquivo:** `n8n_fluxo_score_fornecedor.json`

### Fluxo 3: Anti-Duplicidade (webhook sncrono)

```
POST /webhook/atak-cq/check-duplicidade
    Body: { numero_documento, numero_pcr, fornecedor_codigo }
    |
    +-> Buscar em atak_cargas_raw por documento/PCR
    |
    +-> [Duplicado]     -> Registrar divergncia
    |                   -> Retornar 409 { duplicado: true }
    |
    +-> [No duplicado] -> Retornar 200 { duplicado: false }
```

**Arquivo:** `n8n_fluxo_dedup_documentos.json`

---

## Estrutura de Tabelas

### Tabelas de Integrao (novas)

| Tabela | Propsito | Chave nica |
|--------|-----------|--------------|
| `atak_sync_config` | Configurao dos endpoints ATAK | `endpoint_nome` |
| `atak_sync_log` | Log de cada execuo de sync | - |
| `atak_cargas_raw` | Dados brutos de cargas da API | `hash_registro` |
| `atak_embarques_raw` | Dados brutos de embarques | `hash_registro` |
| `atak_producao_raw` | Dados brutos de produo | `hash_registro` |
| `atak_expedicao_raw` | Dados brutos de expedio | `hash_registro` |
| `cq_validacao_divergencias` | Divergncias detectadas | - |
| `cq_fornecedor_score` | Score contnuo do fornecedor | `fornecedor_codigo` |
| `n8n_webhook_tokens` | Tokens de autenticao n8n | `token` |

### Tabelas Existentes (integradas)

| Tabela | Papel na Integrao |
|--------|---------------------|
| `cq_cargas` | Contagem interna (Classic) para cruzar com ATAK |
| `cq_alertas_qualidade` | Destino dos alertas gerados |
| `cadastros_atak` | Catlogo de fornecedores (381) para lookup |
| `registros_cq_inspecao` | Inspees que podem ser abertas automaticamente |

### Views

| View | Descrio |
|------|-----------|
| `vw_divergencias_ativas` | Divergncias abertas com dados do fornecedor e score |
| `vw_fornecedores_criticos` | Fornecedores crticos/bloqueados com mtricas |

---

## Regras de Validao

### Regra 1: A + B + C = Total Classificado
- **Trigger:** INSERT/UPDATE em `atak_cargas_raw`
- **Condio:** `class_a + class_b + class_c != total_classificado`
- **Gravidade:** Crtica
- **Ao:** Marca `status_validacao = 'divergente'` + insere em `cq_validacao_divergencias`

### Regra 2: Divergncia Frigorfico vs Contagem Interna
- **Trigger:** INSERT em `atak_cargas_raw`
- **Condio:** `qtd_frigo` (ATAK) != `qtd_classic` (cq_cargas, por PCR)
- **Gravidade:** Crtica se diferena > 10, Ateno se > 5
- **Ao:** Insere em `cq_validacao_divergencias`

### Regra 3: Anti-Duplicidade de Carga/Documento
- **Mecanismo:** Hash SHA256 de (documento + PCR + fornecedor + data)
- **Constraint:** `UNIQUE INDEX` em `hash_registro`
- **Webhook:** Verificao sncrona antes de inserir
- **Ao:** Rejeita insero + registra divergncia

### Regra 4: Fornecedor Crtico (%C > Limite)
- **Clculo:** Agregao dos ltimos 90 dias por fornecedor
- **Limites configurveis por fornecedor:**
  - Ateno: %C > 10%
  - Crtico: %C > 15% (padro `limite_pct_c`)
  - Bloqueio: %C > 22.5% (1.5x o limite)
- **Score:** 100 - (%C * 2) - (divergncias_abertas * 5)
- **Ao:** Atualiza `cq_fornecedor_score`, gera alerta em `cq_alertas_qualidade`

### Regra 5: Validao de Dados Bsicos
- Fornecedor obrigatrio
- Data no pode ser futura
- Valores numricos no podem ser negativos
- Documento de referncia obrigatrio

---

## Modelo de Sincronizao Automtica

### Schedule

| Fluxo | Intervalo | Endpoint ATAK | Destino |
|-------|-----------|---------------|---------|
| Sync Cargas | 30 min | WRMVE500 | `atak_cargas_raw` |
| Sync Embarques | 30 min | WRLOG510 | `atak_embarques_raw` |
| Sync Produo | 60 min | WRPRD600 | `atak_producao_raw` |
| Sync Fornecedores | 24h | WRCAD009 | `cadastros_atak` |
| Sync Expedio | 60 min | WREXP | `atak_expedicao_raw` |
| Score Fornecedor | 6h | - (interno) | `cq_fornecedor_score` |

### Pipeline de Dados

```
1. FETCH   API ATAK retorna JSON com dados do perodo
2. HASH    SHA256(documento|pcr|fornecedor|data) para dedup
3. UPSERT  Insert com ON CONFLICT (hash_registro) = merge
4. VALIDATE  Triggers PostgreSQL executam regras 1-2
              n8n Code nodes executam regras 3-5
5. ALERT   Divergncias crticas  cq_alertas_qualidade
6. SCORE   A cada 6h recalcula score de todos fornecedores
7. NOTIFY  Dashboard exibe alertas em tempo real
```

### Controle de Erros

- Cada sync gera um registro em `atak_sync_log`
- Status: `em_execucao  sucesso | erro | parcial`
- Retry automtico no n8n (configurao nativa)
- `n8n_execution_id` vincula execuo do n8n ao log

### Variveis de Ambiente (n8n)

```
SUPABASE_URL=https://nvqxsulntpftcwtkjedu.supabase.co
SUPABASE_SERVICE_KEY=<service_role_key>
ATAK_API_TOKEN=<token_da_api_atak>
ATAK_API_BASE_URL=https://api.atak.com.br/v1
```

---

## Segurana

- **RLS ativo** em todas as tabelas de integrao
- **service_role** usado apenas pelo n8n (nunca exposto no frontend)
- **anon key** no frontend com RLS restritivo
- **Webhook tokens** em tabela separada (`n8n_webhook_tokens`)
- **Audit trail** via `atak_sync_log` + `cq_validacao_divergencias`

---

## Arquivos Entregues

```
integracao_atak_cq/
  00_migration_integracao.sql          # Tabelas, triggers, functions, views, RLS
  n8n_fluxo_sync_cargas.json           # Fluxo principal: sync cargas ATAK  CQ
  n8n_fluxo_score_fornecedor.json      # Fluxo: recalcula score + alertas
  n8n_fluxo_dedup_documentos.json      # Fluxo: webhook anti-duplicidade
  regras_validacao.js                  # Mdulo JS com todas as regras
  ARQUITETURA_INTEGRACAO.md            # Este documento
```
