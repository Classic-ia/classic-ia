# F-37 — Producao (Chao de Fabrica / Industrializacao do Bucho)

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-37 |
| Titulo | Producao — Lote Base, Chao de Fabrica e Rastreabilidade Industrial |
| Modulo | CQ / Producao (Controle de Qualidade — chao de fabrica) |
| Sistema | Classic IA — Classic Importacao e Exportacao de Couros EIRELI |
| CNPJ | 08.849.964/0001-10 |
| Sede | Jardim Alegre - PR |
| Versao | 1.0 |
| Data | 28/07/2026 |
| Autor | Equipe Classic IA |
| Supabase CQ | nvqxsulntpftcwtkjedu.supabase.co |
| Dominio | app.classiccouros.com.br |

---

## 1. Identificacao

**F-37 — Producao** e o modulo responsavel pelo registro, consolidacao, produtividade e rastreabilidade da industrializacao do bucho bovino (omaso) na Classic Couros, cobrindo desde a consolidacao de multiplos lotes de materia-prima (MP) em um **lote base diario**, o apontamento de producao por colaborador no chao de fabrica, o controle de insumo (sal) da salga, ate a leitura da produtividade individual importada do relatorio **WRPRD600** do ERP **ATAK** e a documentacao do fluxo de movimentacoes (romaneios T400→T658).

O modulo compreende **7 telas principais**, **5 arquivos SQL** de migracao (backend), **3 bibliotecas JS compartilhadas** do CQ, e opera sobre o projeto Supabase CQ (`nvqxsulntpftcwtkjedu.supabase.co`). Ele **complementa** o modulo F-31 (Controle de Qualidade): o F-31 decide qualidade da MP recebida; o F-37 consome essa MP classificada na producao.

**Telas do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `lote_base_producao.html` | Consolidacao de MP em lote base diario (BCH-DDMMAAAA) + rastreabilidade | 1025 |
| 2 | `dashboard_producao.html` | Dashboard de produtividade individual (pecas/hora por funcionario/setor) | 1036 |
| 3 | `importar_producao.html` | Importacao do relatorio WRPRD600 do ATAK (XLSX) | 625 |
| 4 | `ficha_producao.html` | Ficha de producao do chao — apontamento por colaborador/palete (mobile) | 174 |
| 5 | `processo_bucho.html` | Documentacao do fluxo ATAK de industrializacao (MIA021, romaneios) | 331 |
| 6 | `sal.html` | Controle de estoque de sal (insumo da salga) | 196 |
| 7 | `dashboard_produto_estado.html` | Drill-down final: produto x fornecedor x estado (qualidade) | 348 |

**Bibliotecas compartilhadas (CQ):**

| Arquivo | Funcao |
|---------|--------|
| `config.js` | Config Supabase CQ (SB_URL `nvqxsulntpftcwtkjedu`, SB_KEY) |
| `cq_api.js` | Camada de API (`CQ_API.get/fetch/post` — fetch Supabase com JWT) |
| `cq_auth.js` | Autenticacao, sessao, perfis, `CQAuth.dadosRegistro()` (auditoria) |
| `cq_toast.js` | Notificacoes toast (`CQToast`) — usado em `lote_base_producao.html` |
| `cq_workflow.js` | Maquina de estados (carregada em `dashboard_produto_estado.html`) |

**Arquivos SQL (backend):**

| Arquivo | Objetos criados |
|---------|-----------------|
| `lote_base_producao.sql` | 3 tabelas (`cq_lote_base_producao`, `cq_lote_base_composicao`, `cq_estoque_mp`) + 6 funcoes + RLS + audit triggers |
| `SISTEMA_CONTROLE_DE_QUALIDADE/09_modulo_sal.sql` | 2 tabelas (`cq_sal_tipos`, `cq_sal_mov`) + view `cq_sal_saldo` + seed |
| `SISTEMA_CONTROLE_DE_QUALIDADE/10_modulo_ficha_producao.sql` | 1 tabela (`cq_ficha_producao`) + view `cq_ficha_prod_colaborador` |
| `fase_importacao_schema.sql` | tabelas de lote de importacao (`cq_importacoes`, `cq_importacao_erros`) |
| `migrations/rh/FUNCOES_APLICACAO.sql` | RPC `importar_producao_atak()` (lado RH/ETL — `producao_real`) |

**Tabelas nao versionadas (drift — existem em producao, DDL aplicada manualmente):** `cq_producao`, `cq_funcionario_setor` (consumidas por `dashboard_producao.html` e `importar_producao.html`).

---

## 2. Problema

A Classic Couros industrializa o bucho bovino (omaso) recebido de frigorificos, transformando materia-prima salgada/congelada em produto acabado classificado A/B/C para exportacao e mercado nacional. Antes do sistema, a producao apresentava os seguintes desafios:

1. **Modelo 1:1 rigido no ATAK** — O ERP ATAK gera 1 lote de producao para cada lote de MP. Com a **ampliacao da salga**, multiplos lotes de MP passaram a ser processados na mesma producao diaria, sem forma de consolida-los e manter rastreabilidade.
2. **Rastreabilidade fragmentada** — Impossivel responder "quais lotes de MP compoem esta producao?" (direta) ou "em quais producoes este lote de MP foi usado?" (reversa).
3. **Produtividade sem visibilidade** — A producao individual por colaborador (pecas/hora) estava presa no relatorio WRPRD600 do ATAK, sem dashboard analitico.
4. **Apontamento de chao em papel** — A producao por colaborador e por palete era anotada manualmente, sem consolidacao em tempo real.
5. **Controle de sal informal** — O sal (insumo critico da salga) nao tinha controle de saldo por tipo (fino/grosso/usado), entradas, consumo por setor.
6. **Fluxo ATAK sem documentacao acessivel** — Os codigos de romaneio (T400, T650, T653...) e regras de estoque nao estavam documentados de forma consultavel pelo operador.

**Solucao:** modulo de producao que (a) consolida MP em lote base diario com rastreabilidade bidirecional, (b) importa e visualiza produtividade WRPRD600, (c) permite apontamento de chao mobile, (d) controla estoque de sal, (e) documenta o fluxo de industrializacao do bucho.

---

## 3. Processo Operacional

### 3.1 Consolidacao de MP em Lote Base (`lote_base_producao.html`)

O fluxo novo (lote base) substitui a **entrada** do processo produtivo — daqui pra frente o fluxo ATAK segue normal a partir do T650:

```
Estoque MP (lotes 100, 101, 102...)
  ↓ descongelamento + classificacao CQ (F-31)
  ↓ consumo parcial de multiplos lotes
Lote Base BCH-11032026 (500+700+300 = 1500 pecas)
  ↓ fluxo normal ATAK (T650→T653→T654→T655→T656→T657→T630→T621)
```

**Passos (Tab 1 — Lote Base Novo):**

1. **Criar/Selecionar Lote Base:** operador informa data de producao, produto (45083 Bucho Salgado / 45084 Bucho Congelado) e observacoes. Sistema gera codigo `BCH-DDMMAAAA` via RPC `cq_criar_lote_base` (sufixo `-N` se ja existir lote no dia).
2. **Adicionar MP do Estoque:** sistema lista lotes de MP com saldo disponivel (RPC `cq_listar_estoque_mp_disponivel`, filtravel por produto/fornecedor). Operador seleciona lotes e define a quantidade a consumir de cada um (default: consumir tudo).
3. **Consumir:** para cada selecao, RPC `cq_consumir_mp_para_lote_base` debita o estoque, registra a composicao e atualiza os totais do lote base (total_pecas, total_lotes_origem).
4. **Fechar Lote Base:** RPC `cq_fechar_lote_base` marca o lote como `fechado` — nenhuma MP adicional pode ser adicionada, pronto para entrar no fluxo ATAK.

**Modelo Atual 1:1 (Tab 2):** para lotes que nao precisam de consolidacao. O registro cria um lote base com 1 lote de MP e o fecha imediatamente (encadeia `cq_criar_lote_base` → cria/verifica `cq_estoque_mp` → `cq_consumir_mp_para_lote_base` → `cq_fechar_lote_base`).

**Rastreabilidade (Tab 3):**
- **Direta (Lote Base → MP):** dado um codigo BCH, lista os lotes de MP que o compoem (RPC `cq_consultar_composicao_lote_base`).
- **Reversa (MP → Lote Base):** dado um lote de MP, mostra saldo em estoque e em quais lotes base foi consumido (RPC `cq_rastrear_mp_em_producao`).

**Lotes Abertos (Tab 4):** lista paginada (50) de todos os lotes base, filtravel por status e data, com acao "Abrir" que carrega o lote na Tab 1.

### 3.2 Fluxo ATAK de Industrializacao (`processo_bucho.html`)

Documentacao read-only do processo completo (MIA021, Redmine #60944), organizada em **7 fases** com seus romaneios. O saldo do lote so e reduzido em movimentos reais de saida (T401, T630); romaneios "sem estoque" (RAS, RAL, RSE, RAE) apenas transitam volumes entre setores. Ver secao 5 (fluxograma) e secao 16 (documentos).

### 3.3 Apontamento de Chao (`ficha_producao.html`)

Tela mobile-first para o operador lancar, no celular, a producao do dia:
1. Preenche data, setor (Sebo/Omaso/Classificacao/Salga/Outro), cliente/fornecedor, lote/palete, quantidade do lote, nº de paletes, observacao.
2. Adiciona a producao **por colaborador** (nome + pecas) em subtabela dinamica.
3. Salva em `cq_ficha_producao` (com auditoria via `CQAuth.dadosRegistro()`). O `total` e a soma das pecas dos colaboradores validos (nome preenchido e pecas > 0), independente da quantidade do lote.
4. KPI "Producao de hoje" e historico de fichas recentes atualizam em tempo real (view `cq_ficha_prod_colaborador`).

### 3.4 Controle de Sal (`sal.html`)

Gestao do insumo da salga por tipo (Sal Fino, Sal Grosso, Sal Usado):
1. Operador registra movimentacao: entrada, saida, consumo (por setor Couro/Omaso), ajuste de inventario, transferencia entre tipos.
2. Grava em `cq_sal_mov`. Saldo por tipo e calculado no banco pela view `cq_sal_saldo`.
3. KPIs de saldo atual por tipo + historico de movimentacoes recentes (100).

### 3.5 Importacao de Produtividade WRPRD600 (`importar_producao.html`)

1. Operador arrasta/seleciona o arquivo Excel **WRPRD600** exportado do ATAK.
2. Sistema calcula hash SHA-256 e avisa se o arquivo ja foi importado (`cq_importacoes.arquivo_hash`).
3. Faz o parse por blocos (data → funcionario → produtos), gera preview com KPIs (dias, funcionarios, registros, pecas, horas, pecas/hora).
4. Ao importar: cria lote em `cq_importacoes`, insere em lotes de 50 em `cq_producao` com dedup (`on_conflict=data_producao,cod_funcionario,cod_produto`, `resolution=ignore-duplicates`); erros de lote em `cq_importacao_erros`.
5. Atualiza totais do lote (importados/duplicados/erros, status).

### 3.6 Analise de Produtividade (`dashboard_producao.html`)

Dashboard sobre `cq_producao` (5 abas): Visao Geral, Por Funcionario, Tendencia, Registros, Cadastro de Setores. Inclui gestao de `cq_funcionario_setor` (mapa cracha→setor), upload de planilha semanal de setores e correcoes de nome. Ver secao 14.

### 3.7 Drill-down Qualidade x Produto x Estado (`dashboard_produto_estado.html`)

Nivel final do drill-down (UF → fornecedor → produto), sobre `registros_cq_inspecao`. Mostra taxa de aprovacao/bloqueio, media de defeitos e defeitos por produto do fornecedor na UF. Ver secao 14.

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Perfil | Descricao | Acesso ao modulo Producao |
|--------|-----------|---------------------------|
| `administrador` | Administrador CQ | Acesso total a todas as telas e acoes |
| `qualidade` | Inspetor/analista de qualidade | Criar/gerir lote base, importar, fichas, sal, dashboards |
| `industrial` | Setor industrial/producao | Lote base, importacao, dashboards de producao |
| `producao` | Operador de producao (chao) | Ficha de producao, controle de sal |
| `logistica` | Setor de logistica | Ler lote base, criar estoque MP, ler fluxo bucho |
| `diretoria` | Diretoria/gerencia | Ler lote base, dashboards, drill-down |
| `financeiro` | Setor financeiro | Drill-down produto/estado |
| `comercial` | Setor comercial | Drill-down produto/estado |

### 4.2 Permissoes por Tela (`CQAuth.init({perfisPermitidos})`)

| Tela | Perfis Permitidos |
|------|-------------------|
| `lote_base_producao.html` | administrador, qualidade, industrial |
| `dashboard_producao.html` | administrador, qualidade, industrial, diretoria |
| `importar_producao.html` | administrador, qualidade, industrial |
| `ficha_producao.html` | administrador, admin, qualidade, producao |
| `processo_bucho.html` | administrador, qualidade, logistica, industrial, diretoria |
| `sal.html` | administrador, admin, qualidade, producao |
| `dashboard_produto_estado.html` | administrador, qualidade, diretoria, financeiro, comercial |

### 4.3 Perfis RLS (backend `cq_get_perfil()`)

As tabelas do lote base restringem por perfil no proprio banco:
- **SELECT** `cq_lote_base_producao` / `cq_lote_base_composicao` / `cq_estoque_mp`: administrador, qualidade, industrial, logistica, diretoria
- **INSERT/UPDATE** lote base e composicao: administrador, qualidade, industrial
- **INSERT** estoque MP: administrador, qualidade, industrial, logistica
- **UPDATE** estoque MP: administrador, qualidade, industrial

> **Nota (divergencia RLS):** as tabelas `cq_sal_*` e `cq_ficha_producao` usam RLS permissivo (`using(true)`) e liberam **SELECT para anon** — ver RN-13 e secao 18.

---

## 5. Fluxograma

```
LOTE BASE (NOVO MODELO N:1)              FLUXO ATAK DO BUCHO (7 fases)
===========================              =============================

Estoque MP (lotes 100,101...)            Fase 1 Recebimento   T400 (MOV012) / T410 (IND049)
   | cq_estoque_mp                                 |  [CQ: inspecao de recebimento]
   v                                               v
Criar Lote Base BCH-DDMMAAAA             Fase 2 Particionamento T401 (MOV011/IND048)
   | cq_criar_lote_base                            |          T402 (RVP · MOV012/IND054)
   v                                               |          etiqueta 101-001-XXXXXX
Listar Estoque Disponivel                          v
   | cq_listar_estoque_mp_disponivel      Fase 3 Salga        T650 (RAP · IND048)
   v                                               |          T652 (RAQ · IND048)
Consumir MP (parcial)                              v
   | cq_consumir_mp_para_lote_base        Fase 4 Processamento T653 RAS · T654 RAL · T655 RSE
   | debita estoque + registra                     |          (sem estoque)
   | composicao + atualiza totais                   |  [CQ: defeitos acumulados]
   v                                               v
Fechar Lote Base                          Fase 5 Classificacao T656 (RAE·IND054) T657 (RAE·IND048)
   | cq_fechar_lote_base                            |          produtos 45084/A 45085/B 45086/C
   | status: aberto->fechado                        |  [CQ: % defeito e classe finais]
   v                                               v
Entra no fluxo ATAK (T650+)              Fase 6 Producao      OP · T630 (IND082) · T621 (IND083)
                                                   v
RASTREABILIDADE                          Fase 7 Expedicao     T658 (RCL) — Frozen Beef Omassum
  Direta:  BCH -> lotes MP                          A(45084) exportacao / B(45085) / C(45086)
  Reversa: MP -> lotes BCH

CHAO DE FABRICA                          PRODUTIVIDADE (WRPRD600)
===============                          ========================
Ficha de Producao (mobile)               Importar WRPRD600 (XLSX)
  data, setor, cliente, lote               parse blocos data/funcionario/produto
  colaboradores [{nome,pecas}]             dedup (data+func+produto)
  total = soma pecas validas               cq_importacoes + cq_producao
   |                                       |
   v                                       v
cq_ficha_producao                        Dashboard Producao (5 abas)
view cq_ficha_prod_colaborador             KPIs, rankings, tendencia, setores

CONTROLE DE SAL
===============
Movimentacao (entrada/saida/consumo/ajuste/transferencia)
   | cq_sal_mov
   v
Saldo por tipo (view cq_sal_saldo)
  saldo = inicial + entradas - saidas - consumo +/- ajuste + transf_in - transf_out
```

---

## 6. Regras de Negocio

### RN-01 — Codigo do Lote Base (BCH sequencial)

Formato `BCH-DDMMAAAA`. Se ja existir lote nao-cancelado para a data, adiciona sufixo `-N` (N = contagem + 1). Safety net garante unicidade absoluta (loop de verificacao). Gerado por `cq_criar_lote_base`.

### RN-02 — Consumo de MP: Saldo Suficiente

Ao consumir MP, `cq_consumir_mp_para_lote_base` valida em ordem:
- `p_quantidade > 0` (senao "Quantidade deve ser maior que zero")
- Lote base existe e esta `aberto` (senao erro com status atual)
- Lote de MP existe no estoque
- Lote de MP **nao** esta `bloqueado` nem `devolvido`
- `quantidade_disponivel >= p_quantidade` (senao "Saldo insuficiente: disponivel X, solicitado Y")

### RN-03 — Debito de Estoque e Status Automatico

Ao consumir, `cq_estoque_mp.quantidade_consumida += p_quantidade`. `quantidade_disponivel` e coluna **GENERATED** (`quantidade_entrada - quantidade_consumida`). Status transiciona automaticamente:
- `esgotado` quando disponivel <= 0
- `parcialmente_consumido` caso contrario

### RN-04 — Composicao Unica por Lote de Origem

Constraint `UNIQUE(lote_base_id, lote_origem_codigo)`: o mesmo lote de MP nao pode ser adicionado 2x no mesmo lote base. Em conflito, o consumo e **somado** (`quantidade_consumida += p_quantidade`) via `ON CONFLICT DO UPDATE`.

### RN-05 — Totais Consolidados do Lote Base

Apos cada consumo, `cq_lote_base_producao` recalcula:
- `total_pecas` = SUM(quantidade_consumida) da composicao
- `total_lotes_origem` = COUNT(DISTINCT lote_origem_codigo)

### RN-06 — Fechamento do Lote Base

`cq_fechar_lote_base` exige: lote existe, status `aberto`, e `total_pecas > 0` (senao "Lote base nao tem nenhuma MP consumida"). Apos fechar (`status='fechado'`, `fechado_por`, `fechado_em`), nenhuma MP adicional pode ser consumida (RN-02 barra por status).

### RN-07 — Estados do Lote Base

`aberto` → `fechado` → `em_producao` → `finalizado`; ou `cancelado`. CHECK constraint na coluna `status`. Apenas `aberto` aceita consumo de MP e fechamento.

### RN-08 — Estados do Estoque de MP

`aguardando_inspecao` (recebido, aguardando CQ) → `disponivel` (liberado) → `parcialmente_consumido` → `esgotado`; ou `bloqueado` / `devolvido`. Apenas lotes com `quantidade_disponivel > 0` e status fora de (`bloqueado`,`devolvido`,`esgotado`) aparecem em `cq_listar_estoque_mp_disponivel`.

### RN-09 — Modelo 1:1 (Producao Individual)

No modelo atual, cada lote de MP gera 1 lote base fechado imediatamente. A tela cria estoque MP se nao existir (`cq_estoque_mp` com status `disponivel`), consome e fecha. Observacao gravada com prefixo `[1:1]`.

### RN-10 — Deduplicacao de Producao (WRPRD600)

Chave unica de producao: `(data_producao, cod_funcionario, cod_produto)`. Upsert com `on_conflict` + `resolution=ignore-duplicates`: registros ja existentes (mesmo funcionario + data + produto) sao ignorados. Duplicados = tamanho do lote − inseridos retornados.

### RN-11 — Deduplicacao por Arquivo (Hash)

Hash SHA-256 do arquivo gravado em `cq_importacoes.arquivo_hash`, verificado antes da importacao. Se ja importado, exibe aviso (data/lote) mas **nao bloqueia** — os registros serao ignorados por RN-10.

### RN-12 — Parse do WRPRD600

Leitura posicional por blocos (col A=0 ... D=3):
- Linha `DATA DE PRODUCAO` → estado corrente de data (DD/MM/YYYY ou serial Excel)
- Linha `FUNCIONARIO` → regex `(\d{4,6})\s*-\s*(.+)` → cod + nome
- Linha de dado: A=cod produto (>= 3 digitos), B=nome produto, C=horas (HH:MM ou decimal BR), D=quantidade
- Descarta linhas `TOTAL`, `HORAS TRABALHADAS`, `QTDE/HORA`, vazias, e registros com `horas<=0 && quantidade<=0`
- `qtde_hora = quantidade / horas` (2 casas); `console.warn` se `qtde_hora > 1000` (anomalia)

### RN-13 — Saldo de Sal Calculado no Banco

`cq_sal_saldo` (view): `saldo = saldo_inicial + entradas − saidas − consumo ± ajuste + transferencias_recebidas − transferencias_enviadas`. Sinais:
- `entrada` +qtd | `saida` −qtd | `consumo` −qtd | `ajuste` +qtd (diferenca com sinal) | `transferencia` −qtd na origem e +qtd no `tipo_sal_destino_id`

### RN-14 — Movimentacao de Sal: Validacoes

- Quantidade > 0, **exceto** `ajuste` (permite qualquer valor, inclusive 0/negativo — diferenca de inventario)
- `transferencia`: `tipo_sal_id` (origem) != `tipo_sal_destino_id` ("Origem e destino devem ser diferentes")
- Campos condicionais: `setor` so em `consumo`; `tipo_sal_destino_id` so em `transferencia`; `origem_destino` so em entrada/saida (senao null); `unidade` fixa em `'kg'`
- 3 tipos padrao seed idempotente: Sal Fino, Sal Grosso, Sal Usado

### RN-15 — Ficha de Producao: Total por Colaborador

Ao salvar, so entram colaboradores com `nome` preenchido **E** `pecas > 0`. `total` = soma das pecas filtradas, recalculado **independente** de `quantidade` (qtd do lote) — nao ha validacao cruzada entre `quantidade` e `total`. Validacao: `cliente` e `lote` nao vazios e `quantidade > 0`.

### RN-16 — Setor do Registro de Producao (fallback em cadeia)

No dashboard: `setor = r.setor || setoresMap[cod_funcionario] || 'Sem setor'` (setor gravado no registro; senao cadastro `cq_funcionario_setor`; senao "Sem setor").

### RN-17 — Produtividade (pecas/hora)

`pecas_hora = quantidade / horas` quando `horas > 0`, senao 0. Usado em KPIs, rankings e graficos de tendencia.

### RN-18 — Cadastro de Setores (cracha 5 digitos)

`cq_funcionario_setor` chaveado por `cod_funcionario`. Cracha deve casar `/^\d{5}$/` (5 digitos). Upload da planilha semanal casa o nome da aba contra `SETOR_MAP` por substring e extrai cracha via regex `(\d{5})`. Lista fixa de 12 setores: Producao Maq, Bate Sal, Classificacao, Lavacao, Sebo, Conf. Salga, Salga, Qualidade, Miudos, Descarregamento, Qualidade Omaso, Couro.

### RN-19 — Romaneios sem Estoque (Fluxo ATAK)

Romaneios RAS (T653), RAL (T654), RSE (T655), RAE (T656/T657) transitam volumes entre setores **sem alterar o saldo** do lote. O saldo so e reduzido em movimentos reais de saida: **T401** (particionamento) e **T630** (saida para producao). Defeitos no RESUMO DO LOTE refletem o total acumulado desde o inicio do processamento, nao por etapa.

### RN-20 — Codigos de Produto por Classe

Apos a classificacao (fase 5), novos codigos de produto: **45084 (Classe A)**, **45085 (Classe B)**, **45086 (Classe C)** — Frozen Beef Omassum. Destino comercial: A predominantemente exportacao, B nacional/exportacao, C nacional.

> **Nota (colisao de codigo):** em `lote_base_producao.html` o codigo `45084` aparece como "Bucho Bovino Congelado" (materia-prima), enquanto no fluxo ATAK (`processo_bucho.html`) `45084` e "Classe A". Divergencia de catalogo a unificar (ver SIGA-05).

### RN-21 — Codigo de Importacao

Formato `IMP-<YYYYMMDDHHMMSS>-<3 chars aleatorios>`. Lote gravado em `cq_importacoes` com `tipo_fonte='wrprd600'`, `tabela_destino='cq_producao'`, status `em_progresso` → `concluido` / `concluido_com_erros`.

### RN-22 — Limpeza Destrutiva de Producao

`limparProducao()` exige dupla confirmacao (`confirm()` + `prompt()` digitando `LIMPAR`), depois chama RPC `limpar_producao` (SECURITY DEFINER, bypassa RLS), exibe contagem removida e recarrega a pagina.

---

## 7. Campos

### 7.1 Lote Base — Criar/Selecionar (`lote_base_producao.html` Tab 1)

| Campo | ID | Tipo | Obrigatorio | Descricao |
|-------|----|------|-------------|-----------|
| Data de Producao | `lbDataProd` | date | Sim | Default: hoje |
| Produto | `lbProduto` | select | Nao | 45083 Bucho Salgado / 45084 Bucho Congelado |
| Observacoes | `lbObs` | text | Nao | Opcional |

**Lote Base Ativo (KPIs):** `lbCodigo`, `lbTotalPecas`, `lbTotalLotes`, `lbStatus`.

**Filtros de Estoque:** `filtProduto` (select), `filtFornecedor` (text). **Selecao de MP:** item com `quantidade_disponivel`, input de quantidade por lote (min 1, max disponivel).

### 7.2 Producao 1:1 (`lote_base_producao.html` Tab 2)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Lote de MP (Origem) | `atualLoteMP` | text | Sim |
| Data de Producao | `atualData` | date | Sim |
| Produto | `atualProduto` | select | Nao |
| Quantidade (pecas) | `atualQtd` | number (min 1) | Sim |
| Documento ATAK | `atualDocAtak` | text | Nao |
| Fornecedor | `atualFornecedor` | text | Nao |
| Observacoes | `atualObs` | text | Nao |

### 7.3 Rastreabilidade (`lote_base_producao.html` Tab 3)

| Campo | ID | Tipo | Modo |
|-------|----|------|------|
| Codigo do Lote Base | `rastreioCodigo` | text | Direta (BCH→MP) |
| Codigo do Lote de MP | `rastreioMP` | text | Reversa (MP→BCH) |

### 7.4 Lotes Abertos (`lote_base_producao.html` Tab 4)

Filtros: `filtStatusLote` (select: aberto/fechado/em_producao/finalizado/cancelado), `filtDataLote` (date).

### 7.5 Ficha de Producao (`ficha_producao.html`)

| Campo | ID | Tipo | Obrigatorio | Descricao |
|-------|----|------|-------------|-----------|
| Data | `fData` | date | Sim | Default: hoje |
| Setor | `fSetor` | select | Nao | Sebo/Omaso/Classificacao/Salga/Outro |
| Cliente / Fornecedor | `fCliente` | text | Sim | Ex.: Dasko |
| Lote / Palete | `fLote` | text | Sim | Ex.: 1638 |
| Quantidade do lote (pecas) | `fQtd` | number (step any) | Sim | Ex.: 1630 |
| Nº de paletes | `fNumPlt` | number (min 1) | Nao | Default 1 |
| Observacao | `fObs` | text | Nao | Opcional |
| Colaborador (nome) | `colabBody[i].nome` | text | — | Subtabela dinamica |
| Colaborador (pecas) | `colabBody[i].pecas` | number (step any) | — | Dispara `totalColab()` |

Total: `colabTotal`. Botoes: `+ Adicionar colaborador` (`addColab`), submit `btnSalvar`.

### 7.6 Controle de Sal (`sal.html`)

| Campo | ID | Tipo | Obrigatorio | Descricao |
|-------|----|------|-------------|-----------|
| Data | `mData` | date | Sim | Default: hoje |
| Tipo de movimentacao | `mTipo` | select | Sim | entrada/saida/consumo/ajuste/transferencia |
| Tipo de sal (origem) | `mTipoSal` | select | Sim | De `cq_sal_tipos` |
| Tipo de sal — destino | `mTipoSalDest` | select | so transferencia | De `cq_sal_tipos` |
| Setor | `mSetor` | select | so consumo | Couro / Omaso |
| Fornecedor/Destino | `mOrigem` | text | Nao | Entrada=fornecedor, Saida=cliente |
| Quantidade (kg) | `mQtd` | number (step any) | Sim (exceto ajuste) | Ajuste: "quantidade real contada" |
| Observacao | `mObs` | text | Nao | Opcional |

### 7.7 Importar Producao (`importar_producao.html`)

| Campo | ID | Tipo | Descricao |
|-------|----|------|-----------|
| Arquivo | `fileInput` | file (.xlsx,.xls) | WRPRD600 do ATAK |
| Drop zone | `dropZone` | area clicavel | Arrastar ou clicar |
| Importar | `btnImportar` | button | `importar()` |

KPIs de preview: `kpiDias`, `kpiFuncionarios`, `kpiRegistros`, `kpiPecas`, `kpiHoras`, `kpiMedia`. Resultado: `resOk`, `resSkip`, `resErr`.

### 7.8 Campos de Auditoria (todas as telas de escrita)

Auto-preenchidos por `CQAuth.dadosRegistro()`: `usuario_id`, `usuario_email`, `usuario_nome`. No lote base (backend): `criado_por` (uuid via `cq_usuarios`), `criado_por_nome`, `fechado_por`, `fechado_em`.

---

## 8. Tabelas

### 8.1 Lote Base de Producao (`lote_base_producao.sql`)

| Tabela | Descricao | Colunas-chave | RLS |
|--------|-----------|---------------|-----|
| `cq_lote_base_producao` | Lote consolidado diario (BCH-DDMMAAAA) | id, codigo (UNIQUE), data_producao, total_pecas (CHECK>=0), total_lotes_origem, produto_codigo/nome, status (CHECK 5 estados), documento_atak, ordem_producao_atak, observacoes, criado_por (FK cq_usuarios), criado_por_nome, fechado_por, fechado_em, criado_em, atualizado_em | Sim (por perfil) |
| `cq_lote_base_composicao` | Rastreabilidade MP→Producao (1 linha = 1 consumo) | id, lote_base_id (FK ON DELETE CASCADE), lote_origem_codigo, lote_origem_atak, fornecedor_codigo/nome, quantidade_consumida (CHECK>0), inspecao_id (FK registros_cq_inspecao), classificacao_mp, percentual_defeitos, criado_por, criado_em, UNIQUE(lote_base_id, lote_origem_codigo) | Sim (por perfil) |
| `cq_estoque_mp` | Saldo de MP por lote de origem | id, lote_codigo (UNIQUE), lote_atak, fornecedor_codigo/nome, produto_codigo/nome, quantidade_entrada (CHECK>0), quantidade_consumida (CHECK>=0), quantidade_disponivel (GENERATED STORED), status (CHECK 6 estados), inspecao_id (FK), classificacao, percentual_defeitos, data_liberacao, data_entrada, data_validade, criado_por, criado_em, atualizado_em | Sim (por perfil) |

Indices: `idx_lote_base_data/status/codigo`, `idx_composicao_lote_base/lote_origem/fornecedor`, `idx_estoque_mp_status/lote/fornecedor/disponivel` (parcial WHERE disponivel>0).

### 8.2 Ficha de Producao (`10_modulo_ficha_producao.sql`)

| Tabela/View | Descricao | Colunas-chave |
|-------------|-----------|---------------|
| `cq_ficha_producao` | Ficha de chao por setor/lote | id, data, setor, turno, cliente, lote, quantidade (numeric 14,2), num_paletes, colaboradores (jsonb `[{nome,pecas}]`), paletes (jsonb `[{numero,pecas}]`), lancamentos (jsonb `[{colaborador,palete,qtd,hora}]`), total, foto_url, obs, usuario_id/email/nome, criado_em, atualizado_em |
| `cq_ficha_prod_colaborador` (VIEW) | Producao por colaborador por dia | data, colaborador, pecas (SUM), fichas (COUNT DISTINCT) — via lateral jsonb_array_elements(colaboradores) |

Indices: `ix_cq_ficha_data`, `ix_cq_ficha_setor`.

### 8.3 Controle de Sal (`09_modulo_sal.sql`)

| Tabela/View | Descricao | Colunas-chave |
|-------------|-----------|---------------|
| `cq_sal_tipos` | Tipos de sal (mestre) | id, nome, nome_normalizado (UNIQUE), unidade (default kg), saldo_inicial, ativo, criado_em, atualizado_em |
| `cq_sal_mov` | Movimentacoes de sal | id, data, tipo (CHECK 5), tipo_sal_id (FK), tipo_sal_destino_id (FK), setor, origem_destino, quantidade_kg, unidade, obs, usuario_id/email/nome, criado_em, atualizado_em |
| `cq_sal_saldo` (VIEW) | Saldo por tipo | id, nome, unidade, saldo_inicial, saldo (calculado — ver RN-13) |

Seed idempotente: Sal Fino, Sal Grosso, Sal Usado.

### 8.4 Importacao (`fase_importacao_schema.sql`)

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `cq_importacoes` | Lote de importacao WRPRD600 | id, codigo_importacao, tipo_fonte ('wrprd600'), arquivo_nome, arquivo_tamanho, arquivo_hash (SHA-256), tabela_destino ('cq_producao'), total_linhas, total_importados, total_duplicados, total_erros, status, usuario_email, usuario_nome, metadados, iniciado_em, concluido_em |
| `cq_importacao_erros` | Erros por linha | id, importacao_id (FK), linha_arquivo, tipo_erro, severidade, mensagem, dados_registro |

### 8.5 Producao Individual (drift — nao versionadas no repo)

| Tabela | Descricao | Colunas usadas pelo frontend |
|--------|-----------|------------------------------|
| `cq_producao` | Producao individual (WRPRD600) | data_producao, cod_funcionario, nome_funcionario, cod_produto, nome_produto, horas, quantidade, qtde_hora, setor, origem_dado ('wrprd600'), importacao_id, arquivo_origem |
| `cq_funcionario_setor` | Mapa cracha→setor | cod_funcionario (UNIQUE), nome_funcionario, setor, atualizado_em |

### 8.6 Producao (lado RH/ETL — `TABELAS_ETL.sql`)

Complementares, usadas pelo pipeline ETL e dashboard de custos (F-14/dashboard_custos), **nao** pelas telas CQ deste modulo:

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `producao_real` | Producao ATAK por produto/dia | data, produto_codigo/nome, quantidade_kg, quantidade_pcs, unidade, horas_total, producao_hora, origem ('atak'), importacao_id |
| `producao_consolidada` | Consolidacao ATAK vs rateio | data, total_atak_kg, total_rateado, divergencia, percentual_dif, status, funcionarios_dia, horas_totais, kg_por_func, kg_por_hora |
| `producao_rateada` | Rateio por funcionario | data, funcionario_id, nome_funcionario, matricula, setor_producao, quantidade, media_hora, horas_trabalhadas, origem ('planilha'), semana_ref, importacao_id |
| `rh_producao` | Produtividade por colaborador (RH) | colaborador_id (FK rh_colaboradores), data_referencia, setor, turno, horas_trabalhadas, producao_quantidade, meta_quantidade, percentual_atingimento, observacoes, registrado_por |

---

## 9. RPCs

### RPC-01 — `cq_criar_lote_base(p_data_producao, p_produto_codigo, p_produto_nome, p_observacoes)`

**Arquivo:** `lote_base_producao.sql` | **Modo:** SECURITY DEFINER

Gera codigo `BCH-DDMMAAAA` (RN-01), cria o registro em `cq_lote_base_producao` e grava audit trail (`cq_registrar_audit`). Retorno: `{ok, lote_base_id, codigo, data_producao}` ou `{ok:false, erro}`.

### RPC-02 — `cq_consumir_mp_para_lote_base(p_lote_base_id, p_lote_mp_codigo, p_quantidade)`

**Modo:** SECURITY DEFINER. Valida saldo/status (RN-02), registra composicao (RN-04), debita estoque (RN-03), recalcula totais (RN-05), audit trail. Retorno: `{ok, lote_base, lote_mp, quantidade_consumida, saldo_restante_mp, total_lote_base}`.

### RPC-03 — `cq_fechar_lote_base(p_lote_base_id)`

**Modo:** SECURITY DEFINER. Valida (RN-06), fecha, monta composicao para retorno, audit. Retorno: `{ok, lote_base, total_pecas, total_lotes_origem, composicao[]}`.

### RPC-04 — `cq_consultar_composicao_lote_base(p_lote_base_codigo, p_lote_base_id)`

**Modo:** STABLE SECURITY DEFINER. Busca por codigo ou id. Retorno: `{ok, lote_base:{...}, composicao:[{lote_origem, lote_atak, fornecedor_codigo/nome, quantidade_consumida, classificacao_mp, percentual_defeitos, inspecao_id, criado_em}]}`.

### RPC-05 — `cq_listar_estoque_mp_disponivel(p_produto_codigo, p_fornecedor_codigo)`

**Modo:** STABLE SECURITY DEFINER. Lista lotes com `quantidade_disponivel > 0` e status fora de (bloqueado/devolvido/esgotado), ordenado por data_entrada ASC. Retorno: `{ok, estoque:[...], total_lotes}`.

### RPC-06 — `cq_rastrear_mp_em_producao(p_lote_mp_codigo)`

**Modo:** STABLE SECURITY DEFINER. Rastreabilidade reversa. Retorno: `{ok, lote_mp, estoque:{quantidade_entrada/consumida/disponivel, status}, usado_em:[{lote_base_codigo, data_producao, status, quantidade_consumida, total_pecas_lote_base}]}`.

### RPC-07 — `importar_producao_atak(p_registros jsonb)` (lado RH/ETL)

**Arquivo:** `FUNCOES_APLICACAO.sql` | **Modo:** SECURITY DEFINER. Chamada por `etl/ingestor_atak.py`. Insere em `producao_real` (data, produto_codigo/nome, quantidade_kg, quantidade_pcs, unidade, producao_hora). Retorno TABLE: `(total, inseridos, erros)`. **Nao** e usada pela tela `importar_producao.html` (que faz INSERT direto em `cq_producao`).

### RPC-08 — `limpar_producao()` (destrutiva)

**Modo:** SECURITY DEFINER (bypassa RLS). Remove todos os registros de `cq_producao`. Chamada por `importar_producao.html` apos dupla confirmacao (RN-22). Retorna contagem removida.

> **Observacao:** as telas `ficha_producao.html`, `sal.html`, `dashboard_producao.html`, `dashboard_produto_estado.html` e `processo_bucho.html` **nao** chamam RPCs — operam via GET/POST/PATCH/DELETE PostgREST direto.

---

## 10. Functions (Backend SQL)

| Funcao | Tipo | Descricao |
|--------|------|-----------|
| `cq_criar_lote_base` | SECURITY DEFINER | Cria lote base (RN-01) |
| `cq_consumir_mp_para_lote_base` | SECURITY DEFINER | Consome MP, debita estoque (RN-02/03/04/05) |
| `cq_fechar_lote_base` | SECURITY DEFINER | Fecha lote base (RN-06) |
| `cq_consultar_composicao_lote_base` | STABLE SECURITY DEFINER | Rastreabilidade direta |
| `cq_listar_estoque_mp_disponivel` | STABLE SECURITY DEFINER | Estoque disponivel (RN-08) |
| `cq_rastrear_mp_em_producao` | STABLE SECURITY DEFINER | Rastreabilidade reversa |
| `cq_registrar_audit(...)` | FUNCTION | Grava audit trail (usada pelas RPCs de lote base) |
| `cq_trigger_audit()` | TRIGGER | Audit AFTER INSERT/UPDATE/DELETE |
| `cq_get_perfil()` | STABLE | Retorna perfil do usuario (usado nas RLS de lote base) |
| `fn_set_atualizado_em()` | TRIGGER | Atualiza `atualizado_em` (sal, ficha, tipos) |
| `fn_normalizar_texto()` | IMMUTABLE | Normaliza texto (nome do tipo de sal) |
| `fn_cq_sal_tipos_norm()` | TRIGGER | Preenche `nome_normalizado` do tipo de sal |
| `importar_producao_atak(p_registros)` | SECURITY DEFINER | ETL producao_real (lado RH) |
| `limpar_producao()` | SECURITY DEFINER | Limpeza destrutiva de cq_producao |

---

## 11. Triggers

| Trigger | Tabela | Evento | Funcao |
|---------|--------|--------|--------|
| `trg_audit_trail_lote_base` | `cq_lote_base_producao` | AFTER INSERT/UPDATE/DELETE | `cq_trigger_audit()` |
| `trg_audit_trail_estoque_mp` | `cq_estoque_mp` | AFTER INSERT/UPDATE/DELETE | `cq_trigger_audit()` |
| `trg_cq_ficha_producao_atualizado` | `cq_ficha_producao` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_sal_tipos_atualizado` | `cq_sal_tipos` | BEFORE UPDATE | `fn_set_atualizado_em()` |
| `trg_cq_sal_tipos_norm` | `cq_sal_tipos` | BEFORE INSERT/UPDATE | `fn_cq_sal_tipos_norm()` |
| `trg_cq_sal_mov_atualizado` | `cq_sal_mov` | BEFORE UPDATE | `fn_set_atualizado_em()` |

---

## 12. APIs (Chamadas Frontend)

### 12.1 Supabase REST (PostgREST)

**Leitura (GET):**

| Endpoint | Tela |
|----------|------|
| `cq_lote_base_producao?status=in.(aberto,fechado,em_producao)&order=criado_em.desc&limit=20` | Lote Base (selecionar) |
| `cq_lote_base_producao?data_producao=eq.{hoje}&order=criado_em.desc` | Lote Base (hist 1:1) |
| `cq_lote_base_producao?order=criado_em.desc&limit=50[&status=eq.X][&data_producao=eq.Y]` | Lote Base (abertos) |
| `cq_estoque_mp?lote_codigo=eq.{lote}` | Lote Base 1:1 (check) |
| `cq_producao?order=data_producao.desc&limit=5000` | Dashboard Producao |
| `cq_funcionario_setor?select=cod_funcionario,setor` | Dashboard/Importar |
| `cq_funcionario_setor?order=nome_funcionario.asc` | Dashboard (cadastro) |
| `cq_ficha_prod_colaborador?data=eq.{hoje}&order=pecas.desc` | Ficha (KPI hoje) |
| `cq_ficha_producao?order=data.desc,criado_em.desc&limit=80` | Ficha (recentes) |
| `cq_sal_saldo?order=nome.asc` | Sal (saldo) |
| `cq_sal_mov?order=data.desc,criado_em.desc&limit=100` | Sal (movimentacoes) |
| `cq_sal_tipos?ativo=eq.true&order=nome.asc` | Sal (tipos) |
| `cq_importacoes?select=id,codigo_importacao,iniciado_em&arquivo_hash=eq.{hash}&limit=1` | Importar (dedup arquivo) |
| `registros_cq_inspecao?select=data_inspecao,lote_atak,...,status_final,<11 defeitos>&fornecedor(_codigo)=eq.X[&data_inspecao=gte.Y]&order=data_inspecao.desc` | Drill-down produto/estado |

**Escrita (POST/PATCH/DELETE):**

| Endpoint | Tela | Operacao |
|----------|------|----------|
| `cq_estoque_mp` (Prefer return=minimal) | Lote Base 1:1 | POST cria estoque MP |
| `cq_ficha_producao` (Prefer return=minimal) | Ficha | POST ficha |
| `cq_sal_mov` (Prefer return=minimal) | Sal | POST movimentacao |
| `cq_importacoes` (Prefer return=representation) | Importar | POST lote |
| `cq_importacoes?id=eq.{id}` | Importar | PATCH totais/status |
| `cq_importacao_erros` | Importar | POST erro |
| `cq_producao?on_conflict=data_producao,cod_funcionario,cod_produto` (resolution=ignore-duplicates) | Importar | POST em lotes de 50 |
| `cq_funcionario_setor?on_conflict=cod_funcionario` (merge-duplicates) | Dashboard | UPSERT setores |
| `cq_funcionario_setor?cod_funcionario=eq.{cod}` | Dashboard | PATCH / DELETE |
| `cq_producao?cod_funcionario=eq.{cod}[&setor=is.null]` | Dashboard | PATCH setor |

**RPCs (POST /rest/v1/rpc/):** `cq_criar_lote_base`, `cq_consumir_mp_para_lote_base`, `cq_fechar_lote_base`, `cq_consultar_composicao_lote_base`, `cq_listar_estoque_mp_disponivel`, `cq_rastrear_mp_em_producao`, `limpar_producao`.

### 12.2 Bibliotecas Externas (CDN)

| Biblioteca | Tela |
|------------|------|
| Chart.js 4 | dashboard_producao (v4), dashboard_produto_estado (v4.4.1) |
| SheetJS/XLSX 0.20.1 | dashboard_producao (upload setores), importar_producao (WRPRD600) |

---

## 13. Integracoes

### 13.1 ATAK (ERP de Gestao de Abate/Industrializacao)

- **Tipo:** ERP externo; integracao via arquivo (WRPRD600) e via n8n (F-31)
- **Fluxo documentado (MIA021, Redmine #60944):** romaneios T400→T658 (ver secao 16)
- **Relatorio WRPRD600:** producao por funcionario/produto/data/horas/quantidade, exportado em XLSX e importado em `cq_producao`
- **Codigos de rotina ATAK:** MOV011, MOV012, IND048, IND049, IND054, IND082, IND083
- **Referencias humanas (rodape processo_bucho):** Coordenador ATAK Jean Castilho; Tecnico Dormando Macedo; Usuaria Alessandra Ribeiro; Coordenadora Cliente Janaina Anacleto

### 13.2 SheetJS / XLSX

- Parse de planilhas Excel (WRPRD600 e planilha semanal de setores)
- `XLSX.read(arrayBuffer, {type:'array', cellDates:true})`, primeira aba, `sheet_to_json({header:1, defval:null, raw:true})`
- Conversao de serial Excel → data: `(val - 25569) * 86400 * 1000`

### 13.3 ETL Python (lado RH)

- `etl/ingestor_atak.py` → RPC `importar_producao_atak` → `producao_real`
- Pipeline de rateio: `producao_real` → `producao_rateada` → `producao_consolidada` (divergencia ATAK vs rateio)

### 13.4 Supabase Storage

- `cq_ficha_producao.foto_url` — foto opcional da ficha (bucket CQ)

---

## 14. Dashboards

### 14.1 Dashboard de Producao Individual (`dashboard_producao.html`)

**Fonte:** `cq_producao` (limit 5000) + `cq_funcionario_setor`.

**6 KPIs:** Registros (`kTotal`), Funcionarios (`kFunc`, distinct cod), Pecas Total (`kPecas`, Σ quantidade), Horas Total (`kHoras`, Σ horas), Pcs/Hora Media (`kMedia`), Dias (`kDias`, distinct data).

**Filtros:** `filtDe`/`filtAte` (data), `filtSetor`, `filtFunc`, `filtProd`.

**5 Abas:**
- **Visao Geral:** graficos por produto, por setor, top 10 (qtd e pcs/h), horas por setor
- **Por Funcionario:** ranking (`#`, Funcionario, Setor, Horas, Pecas, Pcs/Hora); top 15 (qtd e pcs/h)
- **Tendencia:** pecas/dia (linha), horas/dia (barra), eficiencia pcs/hora/dia (linha)
- **Registros:** tabela (Data, Cod, Funcionario, Setor, Produto, Horas, Qtd, Pcs/h — limite 200)
- **Cadastro Setores:** gestao `cq_funcionario_setor`, upload planilha semanal, funcionarios sem setor, correcoes de nome

**Graficos (Chart.js 4):** 10 canvases (barras horizontais, linha, barra vertical). Paleta colorblind-safe Wong (`#E69F00,#56B4E9,#009E73,...`), fonte DM Mono.

**Operacoes de cadastro:** upload XLSX de setores (upsert), `corrigirNomes()` (usa cq_producao como fonte confiavel), `importarDaProducao()` (importa nao-cadastrados como "Sem setor"), `gravarSetoresNosRegistros()`, `limparCadastroLixo()`, troca de setor com opcao de recalcular registros do periodo.

### 14.2 Ficha de Producao — KPI Hoje (`ficha_producao.html`)

**KPI "Producao de hoje":** total geral (`hojeTotal`) + subtitulo (N pecas · N colaboradores). Tabela (`#`, Colaborador, Pecas hoje) via `cq_ficha_prod_colaborador`. Historico "Fichas recentes" (Data, Cliente, Lote, Setor, Total — limit 80).

### 14.3 Saldo de Sal (`sal.html`)

Cards de saldo atual por tipo (`cq_sal_saldo`: nome, saldo em kg, unidade). Tabela de movimentacoes recentes (Data, Tipo, Sal, Setor/Destino, Origem/Destino, Qtd kg — limit 100). Sem graficos.

### 14.4 Drill-down Produto x Estado (`dashboard_produto_estado.html`)

**Fonte:** `registros_cq_inspecao` filtrado por `fornecedor(_codigo)` + periodo (URL params `uf`, `fornecedor`, `codigo`).

**4 KPIs:** Inspecoes, Taxa aprovacao (%), Taxa bloqueio (%), Media defeitos (%).

**Filtros:** Periodo (90/30/180/365/0 dias), Status (aprovado/ressalva/bloqueado).

**Grafico:** `chartDefeitos` — barras empilhadas (11 defeitos por produto).

**Tabelas:** Comparativo por Produto (Produto, Inspecoes, Aprov., Ress., Bloq., % Aprov., % Bloq. com cor por faixa, Med. Def.); Inspecoes Detalhadas (Data, Lote, Produto, Classe A/B/C, % Defeitos, Status, link Rastrear → `rastreabilidade_lote.html?lote=X` — slice 100).

---

## 15. Relatorios

### 15.1 Preview de Importacao (WRPRD600)

`importar_producao.html`: apos parse, exibe KPIs consolidados e tabela de preview (ate 30 registros) antes do commit.

### 15.2 Resultado de Importacao

Card com Importados / Ja existiam (duplicados) / Erros, persistido em `cq_importacoes` e `cq_importacao_erros`.

### 15.3 Impressao de Dashboard

`dashboard_producao.html` → botao "Imprimir" (`window.print()`).

### 15.4 Rastreabilidade (Composicao / Reversa)

Consultas renderizadas em tela (lote base ativo, composicao, uso em producoes) — sem geracao de PDF neste modulo (diferente do F-31).

---

## 16. Documentos

### 16.1 Fluxo de Industrializacao do Bucho (MIA021)

Documentado em `processo_bucho.html`. **7 fases** e romaneios:

| Fase | Etapa | Romaneios (codigo · rotina) |
|------|-------|-----------------------------|
| 1 | Recebimento | T400 (MOV012) Pedido de Compra · T410 (IND049) Entrada de Produtos · [CQ inspecao] |
| 2 | Particionamento | T401 (MOV011/IND048) Saida Parcial/Total · T402 (RVP · MOV012/IND054) Entrada de Volumes Particionados · etiqueta 101-001-XXXXXX |
| 3 | Salga | T650 (RAP · IND048) Apontamento de Salga · T652 (RAQ · IND048) Diferenca |
| 4 | Processamento | T653 (RAS) Bate Sal · T654 (RAL) Lavagem · T655 (RSE) Sebo — sem estoque · [CQ defeitos acumulados] |
| 5 | Classificacao | T656 (RAE · IND054) · T657 (RAE · IND048) · produtos 45084/A 45085/B 45086/C · [CQ classe final] |
| 6 | Producao | OP (ERPATAK) Ordem de Producao · T630 (IND082) Saida p/ Industrializacao · T621 (IND083) Entrada A/B/C |
| 7 | Expedicao | T658 (RCL) Produtos Classificados · Frozen Beef Omassum A(exportacao)/B/C(nacional) |

Referencia: MIA021 · Modulo Movimentacoes de Entrada · Redmine #60944.

### 16.2 Regras de Estoque no Fluxo

- Romaneios sem estoque (RAS/RAL/RSE/RAE) transitam sem alterar saldo (RN-19)
- Saldo reduzido apenas em T401 e T630
- Defeitos no RESUMO DO LOTE = total acumulado, nao por etapa

---

## 17. Evidencias

### 17.1 Foto da Ficha de Producao

`cq_ficha_producao.foto_url` — foto opcional do chao (upload Storage CQ).

### 17.2 Arquivo de Importacao (Hash)

`cq_importacoes.arquivo_hash` (SHA-256) + `arquivo_nome`, `arquivo_tamanho` — evidencia de origem/dedup do WRPRD600.

### 17.3 Auditoria de Movimentacoes

Campos `usuario_id/email/nome` em `cq_sal_mov` e `cq_ficha_producao` (via `CQAuth.dadosRegistro()`). Audit trail completo (antes/depois) para lote base e estoque MP.

---

## 18. Permissoes

### 18.1 RLS — Lote Base (por perfil, `cq_get_perfil()`)

Endurecido: SELECT para (administrador, qualidade, industrial, logistica, diretoria); INSERT/UPDATE restrito a (administrador, qualidade, industrial); INSERT estoque MP inclui logistica. **DELETE nao concedido** (sem policy).

### 18.2 RLS — Sal e Ficha (permissivo)

`cq_sal_tipos`, `cq_sal_mov`, `cq_ficha_producao`: SELECT/INSERT/UPDATE via `using(true)` / `with check(true)`; `cq_sal_mov` e `cq_ficha_producao` tambem tem policy **DELETE** `using(true)`. **Todas liberam SELECT para `anon`** — inconsistente com o hardening do F-31 (ver secao 24 / SIGA-04).

### 18.3 Permissoes por Perfil (Frontend)

`CQAuth.init({perfisPermitidos})` por tela (secao 4.2). `redirectLogin: true` em lote_base, importar e drill-down.

---

## 19. Auditoria

### 19.1 Audit Trail — Lote Base

`cq_registrar_audit('producao', <acao>, <tabela>, <id>, <codigo>, <antes jsonb>, <depois jsonb>, ...)` chamada em:
- `criar` (lote base criado)
- `consumir_mp` (saldo antes/depois, lote_mp, quantidade)
- `fechar_lote_base` (status aberto→fechado, total_pecas, composicao)

Triggers `trg_audit_trail_lote_base` e `trg_audit_trail_estoque_mp` (AFTER INS/UPD/DEL → `cq_trigger_audit()`).

### 19.2 Log de Importacao

`cq_importacoes` (codigo, hash, totais, status, usuario, iniciado/concluido) + `cq_importacao_erros` (linha, tipo, severidade, mensagem, dados_registro).

### 19.3 Auditoria de Campo

`atualizado_em` atualizado por trigger em sal, ficha e tipos. `usuario_*` gravado em toda escrita de sal/ficha.

---

## 20. Eventos

### 20.1 Ciclo de Vida (Lote Base)

| Evento | Trigger/Handler |
|--------|----------------|
| Lote base criado | RPC `cq_criar_lote_base` |
| MP consumida | RPC `cq_consumir_mp_para_lote_base` (debita estoque + composicao + totais) |
| Estoque esgotado | UPDATE automatico status → `esgotado` |
| Lote base fechado | RPC `cq_fechar_lote_base` |
| Audit | `cq_trigger_audit()` (AFTER) |

### 20.2 Ciclo de Vida (Producao/Sal/Ficha)

| Evento | Handler |
|--------|---------|
| Importacao WRPRD600 | POST `cq_importacoes` + INSERT lotes `cq_producao` |
| Erro de lote | POST `cq_importacao_erros` |
| Ficha salva | POST `cq_ficha_producao` |
| Movimentacao de sal | POST `cq_sal_mov` (saldo recalculado na view) |

### 20.3 Eventos de UI

| Evento | Handler |
|--------|---------|
| Trocar aba | `showTab` / `switchTab` (lazy load) |
| Selecionar MP | `toggleEstoqueItem` / `atualizarQtdSelecao` |
| Drop de arquivo | `processarArquivo` (hash + parse + preview) |
| Alterar tipo de mov. sal | `ajustarCampos()` (campos condicionais) |
| Adicionar colaborador (ficha) | `addColab()` / `totalColab()` |
| Trocar setor (dashboard) | `atualizarSetor` / `confirmarTrocaSetor` (recalcular periodo) |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico | Detalhe |
|------------|---------|---------|
| Banco de dados | Supabase PostgreSQL | `nvqxsulntpftcwtkjedu.supabase.co` (CQ) |
| Autenticacao | Supabase Auth | JWT, perfis em `cq_usuarios` |
| Storage | Supabase Storage | Foto de ficha (bucket CQ) |
| Frontend | Vercel (auto-deploy main) | `app.classiccouros.com.br` |
| ERP origem | ATAK | Relatorio WRPRD600, romaneios T400-T658 |

### 21.2 Bibliotecas Externas

Chart.js 4 / 4.4.1, SheetJS/XLSX 0.20.1.

### 21.3 Bibliotecas Internas

config.js, cq_api.js, cq_auth.js, cq_toast.js, cq_workflow.js, ds.css.

### 21.4 Dependencias de Modulo

- **F-31 (CQ):** `registros_cq_inspecao` (classificacao da MP, drill-down), `cq_usuarios`, `cq_registrar_audit`, `fn_normalizar_texto`, `fn_set_atualizado_em`
- **ETL:** `producao_real/rateada/consolidada` (rateio de custo de pessoal)

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Controle de Qualidade (inspecao MP) | F-31 CQ | Classifica a MP que o lote base consome; `inspecao_id`/`classificacao_mp` na composicao |
| Rastreabilidade de Lote | F-31 CQ | Drill-down link `rastreabilidade_lote.html?lote=X` |
| Dashboard de Custos (rateio) | F-14 / RH | `producao_rateada`/`consolidada` distribuem custo por producao |
| Ficha de Producao (KPI chao) | CQ | Complementa lote base (produtividade vs rastreabilidade) |
| Controle de Sal | CQ | Insumo da fase de Salga (T650) |
| Gestao de Colaboradores | F-10 RH | Nome/matricula dos operadores da producao |

---

## 23. Melhorias SIGA

### SIGA-01 — Unificacao do Modelo de Producao

Consolidar o modelo 1:1 e o lote base N:1 em um unico fluxo parametrizado, eliminando a criacao "sintetica" de estoque MP na Tab 2.

### SIGA-02 — Estados do Lote Base Ligados ao ATAK

Automatizar a transicao `fechado → em_producao → finalizado` a partir de eventos reais do ATAK (T630/T621), preenchendo `documento_atak`/`ordem_producao_atak`.

### SIGA-03 — RPC de Importacao WRPRD600

Migrar o INSERT direto em `cq_producao` para uma RPC transacional (validacao, dedup, log atomico) — hoje o commit acontece em lotes soltos no frontend.

### SIGA-04 — Hardening RLS de Sal/Ficha

Remover SELECT `anon` e o `using(true)` permissivo de `cq_sal_*` e `cq_ficha_producao`, alinhando ao padrao de perfis do lote base e do F-31.

### SIGA-05 — Catalogo de Produtos Unico

Resolver a colisao do codigo 45084 (Bucho Congelado vs Classe A) com um catalogo de produtos versionado, ligando MP a produto acabado.

### SIGA-06 — Cadastro Dinamico de Setores e Colaboradores

Substituir a lista fixa de setores (`SETOR_MAP`) e o cadastro por cracha por FKs para `rh_funcionarios`/`rh_setores`, eliminando a tabela drift `cq_funcionario_setor`.

---

## 24. Adequacao SIGA (Multiempresa)

### 24.1 Cenario Atual

Modulo opera exclusivamente para a Classic Couros (CNPJ 08.849.964/0001-10), focado em industrializacao de bucho/omaso bovino.

### 24.2 Alteracoes para Multiempresa

1. Coluna `empresa_id` em `cq_lote_base_producao`, `cq_estoque_mp`, `cq_producao`, `cq_ficha_producao`, `cq_sal_mov`
2. RLS por empresa: `empresa_id = get_empresa_usuario()`
3. Codigo do lote base namespaced por empresa (evitar colisao de BCH-DDMMAAAA)
4. Catalogo de produtos e setores por empresa (cortes/subprodutos distintos)
5. Fluxo ATAK parametrizavel (romaneios variam por unidade)
6. Tipos de sal e insumos por empresa
7. Dashboards com filtro por empresa no topbar

**Criterio:** operador da Classic ve so lotes base/producao da Classic; troca de empresa no topbar filtra os dados.

---

## 25. Criterios de Aceite

### CA-01 — Criar Lote Base

Informar data e produto → codigo `BCH-DDMMAAAA` gerado (sufixo se repetido no dia). Registro criado em `cq_lote_base_producao` com status `aberto`.

### CA-02 — Consumir MP com Saldo

Selecionar lote de MP com saldo, consumir quantidade valida → estoque debitado, composicao registrada, totais atualizados. Saldo insuficiente / lote bloqueado → erro claro.

### CA-03 — Fechar Lote Base

Fechar lote com `total_pecas > 0` → status `fechado`, `fechado_por/em` preenchidos, sem novos consumos. Lote vazio → recusado.

### CA-04 — Rastreabilidade Bidirecional

Direta: BCH → lista de lotes de MP com quantidades e classificacao. Reversa: lote de MP → saldo + lotes base onde foi usado.

### CA-05 — Modelo 1:1

Registrar producao individual → cria estoque MP (se ausente), consome e fecha lote base automaticamente. Historico do dia atualizado.

### CA-06 — Importar WRPRD600

Arquivo XLSX parseado → preview com KPIs. Importacao insere em `cq_producao` com dedup por (data, funcionario, produto); duplicados ignorados; log em `cq_importacoes`.

### CA-07 — Dashboard de Produtividade

6 KPIs, 5 abas, graficos, filtros por periodo/setor/funcionario/produto. Setor resolvido por fallback (RN-16). Pcs/hora conforme RN-17.

### CA-08 — Ficha de Producao

Salvar ficha com colaboradores validos (nome + pecas>0). Total = soma filtrada. KPI de hoje e historico atualizados.

### CA-09 — Controle de Sal

Registrar 5 tipos de movimentacao. Saldo por tipo calculado na view. Validacoes de RN-14.

### CA-10 — Drill-down Produto/Estado

Filtrar por UF/fornecedor/produto, exibir taxa de aprovacao/bloqueio, media de defeitos, defeitos por produto, link de rastreabilidade.

---

## 26. Casos de Teste

### CT-01 — Lote Base 3 Lotes

Criar BCH para hoje, consumir 500 do lote 100, 700 do 101, 300 do 102 → total_pecas=1500, total_lotes_origem=3. Fechar → status fechado.

### CT-02 — Saldo Insuficiente

Consumir 1000 de lote com disponivel 300 → erro "Saldo insuficiente: disponivel 300, solicitado 1000". Estoque inalterado.

### CT-03 — Lote de MP Bloqueado

Consumir de lote status `bloqueado` → erro "Lote X esta bloqueado". Nao registra composicao.

### CT-04 — Consumo Duplicado do Mesmo Lote

Consumir 200 e depois +300 do lote 100 no mesmo BCH → composicao unica com quantidade_consumida=500 (soma via ON CONFLICT).

### CT-05 — Codigo Duplicado no Dia

Criar 2 lotes base para a mesma data → segundo recebe sufixo `-2`.

### CT-06 — Fechar Lote Vazio

Fechar lote sem MP consumida → erro "Lote base nao tem nenhuma MP consumida".

### CT-07 — Rastreabilidade Reversa

Consultar lote de MP usado em 2 BCH → retorna saldo + 2 usos com quantidades.

### CT-08 — Importar WRPRD600 Novo

Arquivo novo → preview correto, N importados, 0 duplicados. Reimportar mesmo arquivo → aviso hash + todos ignorados (0 importados).

### CT-09 — Movimentacao de Sal (Transferencia)

Transferir 100kg de Sal Grosso → Sal Fino → saldo grosso −100, fino +100 (view). Origem=destino → erro.

### CT-10 — Ficha de Producao

Lancar ficha com 3 colaboradores (1 com pecas=0) → total = soma dos 2 validos. KPI de hoje atualizado.

---

## 27. Casos Extremos

### CE-01 — Quantidade Zero no Consumo

`p_quantidade <= 0` → erro "Quantidade deve ser maior que zero".

### CE-02 — Lote Base Nao-Aberto

Consumir/fechar lote `fechado`/`em_producao` → erro com status atual.

### CE-03 — Estoque Esgotado apos Consumo

Consumo que zera saldo → status `esgotado`, sai da lista de disponiveis.

### CE-04 — WRPRD600 Formato Invalido

Nenhum registro valido → alerta "Nenhum registro de producao encontrado. Verifique se o arquivo e um WRPRD600."

### CE-05 — qtde_hora Anomalo

`qtde_hora > 1000` → `console.warn` (nao bloqueia importacao).

### CE-06 — Ajuste de Sal Negativo

`tipo='ajuste'` permite quantidade 0/negativa (diferenca de inventario) — unica excecao a "quantidade > 0".

### CE-07 — Ficha sem Colaborador Valido

Todos colaboradores sem nome ou pecas=0 → total=0 gravado (sem validacao cruzada com quantidade do lote).

### CE-08 — Setor Nao Cadastrado

Registro de producao sem setor e sem cadastro → exibido como "Sem setor" e contado no badge.

### CE-09 — Limpeza de Producao Cancelada

Usuario nao digita `LIMPAR` no prompt → operacao abortada, nenhum dado removido.

### CE-10 — Drill-down sem Parametros

`dashboard_produto_estado.html` sem `fornecedor` nem `codigo` na URL → mensagem de erro com link de volta.

---

## 28. Melhorias Futuras

### MF-01 — Lote Base como PWA Offline

Apontamento de consolidacao e ficha de chao offline com sincronizacao (como o F-31).

### MF-02 — Integracao ATAK em Tempo Real

Substituir o WRPRD600 manual por API/webhook do ATAK (producao em streaming).

### MF-03 — Geracao de PDF de Rastreabilidade

Relatorio PDF da composicao do lote base (origem → produto acabado) para auditoria/exportacao.

### MF-04 — Dashboard de Rendimento

KPI de rendimento MP → produto acabado (peso entrada vs saida A/B/C) por lote base.

### MF-05 — Alerta de Validade de MP

Usar `cq_estoque_mp.data_validade` para alertar lotes proximos do vencimento antes do consumo.

### MF-06 — QR Code por Lote Base

QR unico do BCH para consulta rapida de composicao e status no chao.

### MF-07 — Metas de Produtividade

`meta_quantidade`/`percentual_atingimento` (ja em `rh_producao`) no dashboard de produtividade por setor/turno.

### MF-08 — Reconciliacao ATAK vs Lote Base

Conferir `total_pecas` do lote base contra a entrada de produto acabado no ATAK (T621).

### MF-09 — Balanca IoT na Salga

Peso automatico de sal (entrada/consumo) via balanca conectada.

### MF-10 — Realtime no Chao

Supabase Realtime para atualizar KPI de ficha e saldo de sal sem refresh.

---

*Documento gerado em 28/07/2026 — Classic IA — Producao*
*Versao 1.0 — Especificacao Funcional Completa F-37*
