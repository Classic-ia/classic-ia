# F-41 — Beneficios

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-41 |
| Titulo | Beneficios (VA, Auxilio Deslocamento, Premio Assiduidade, Plano Alianca) + Pagamentos |
| Modulo | RH (Recursos Humanos — Beneficios e Folha) |
| Sistema | Classic IA — Classic Importacao e Exportacao de Couros EIRELI |
| CNPJ | 08.849.964/0001-10 |
| Sede | Jardim Alegre - PR |
| Versao | 1.0 |
| Data | 28/07/2026 |
| Autor | Equipe Classic IA |
| Supabase RH | muiqmtnfvyffborgiwdw.supabase.co |
| Dominio | app.classiccouros.com.br |

---

## 1. Identificacao

**F-41 — Beneficios** e o modulo responsavel pela gestao dos beneficios dos colaboradores da Classic Couros — Vale Alimentacao (VA), Auxilio Deslocamento (por cidade), Premio Assiduidade e Plano Alianca — alem da consolidacao mensal para a contabilidade e da preparacao de listas de pagamento (dados bancarios, lotes, exportacao). A base de calculo e a **apuracao mensal** (competencia 27-26), que determina elegibilidade e valores proporcionais.

O modulo compreende **3 telas principais**, alimentadas por tabelas de beneficios, apuracao e pagamento no projeto Supabase RH.

**Telas do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/beneficios.html` | Gestao dos 4 beneficios por colaborador (modal + bulk) | 517 |
| 2 | `rh/exportar_pagamentos.html` | Dados bancarios, lotes e exportacao de listas de pagamento (4 abas) | 603 |
| 3 | `rh/relatorio_beneficios.html` | Relatorio consolidado mensal para a contabilidade (5 secoes) | 999 |

**Bibliotecas compartilhadas (RH):**

| Tela | Stack |
|------|-------|
| beneficios.html | config.js, rh_auth.js, api.js (`API.get/post/patch`), shell.js (`Shell.init`), ui_components.js, ds.css |
| exportar_pagamentos.html | config.js, **rh_api.js** (`RH_API`), rh_auth.js, ds.css, **SheetJS xlsx@0.18.5** |
| relatorio_beneficios.html | config.js, rh_auth.js, api.js (`API.get`), ui_components.js, page_bootstrap.js, shell.js, ds.css |

**Sem Chart.js, sem RPCs** — as 3 telas operam por PostgREST direto.

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `00_schema_supabase.sql` | rh_vale_alimentacao, rh_auxilio_deslocamento, rh_plano_alianca, rh_deslocamento_cidades, rh_apuracao_mensal |
| `TABELAS_RH_CORE.sql` | rh_beneficio (generico) |
| `schema_rh_sst_completo.sql` | rh_dados_bancarios, rh_lotes_pagamento, rh_log_exportacao (**nao deployadas em prod — ver secao 24**) |
| `TABELAS_FINANCEIRO.sql` | fp_lote_pagamento, fp_pagamento_funcionario (sistema de pagamento paralelo `fp_*`) |

---

## 2. Problema

A Classic Couros concede 4 beneficios com regras distintas de elegibilidade e proporcionalidade, dependentes da frequencia (ponto Secullum) e do local de moradia. Antes do sistema:

1. **Calculo manual de VA e assiduidade** — Proporcionalizar por dias trabalhados e aplicar perdas (faltas, atrasos, advertencias) em planilha era lento e sujeito a erro.
2. **Auxilio deslocamento por cidade** — Valores diferentes por municipio (7,6 a 320 km) sem tabela unica.
3. **Consolidacao para a contabilidade** — Fechar o mes e enviar os beneficios para a folha exigia recompor tudo manualmente.
4. **Pagamentos dispersos** — Dados bancarios (Itau, Sicredi, PIX, cheque) sem cadastro centralizado nem lotes de pagamento.

**Solucao:** modulo que (a) cadastra e ativa/desativa beneficios por colaborador, (b) usa a apuracao 27-26 para elegibilidade/valores, (c) consolida por competencia para a contabilidade, (d) organiza dados bancarios e lotes de pagamento com exportacao.

---

## 3. Processo Operacional

### 3.1 Gestao de Beneficios (`beneficios.html`)

1. Lista colaboradores ativos com o status de cada beneficio (VA, Deslocamento, Alianca).
2. Modal de detalhe por colaborador permite ativar/desativar cada beneficio:
   - **VA:** ativa (valor R$300, desconto 8% do salario) — valores fixos na criacao.
   - **Deslocamento:** seleciona cidade de origem (valor da tabela) + flag "usa transporte da empresa" (so em cidades com transporte).
   - **Alianca:** ativa (data_adesao = hoje).
3. Acoes em massa: **Ativar VA para todos** (cria/reativa VA), **Auto-config deslocamento** (atribui valor por cidade).
4. Toda alteracao gera `RHAuth.log('beneficio_atualizado', ...)`.

### 3.2 Apuracao Mensal (`rh_apuracao_mensal` — competencia 27-26)

Base de calculo de todos os beneficios. Para cada colaborador/competencia grava frequencia (dias trabalhados, faltas, atrasos, atestados, advertencias, ferias, INSS), elegibilidade e valores calculados (assiduidade, VA + desconto, deslocamento), totais e status (`rascunho → calculado → validado → pago`). O periodo apurado vai do **dia 27 do mes anterior ao dia 26 da competencia**. (Populada por processo de apuracao/importacao — o relatorio apenas le.)

### 3.3 Relatorio Consolidado (`relatorio_beneficios.html`)

1. Seleciona competencia (mes/ano).
2. Le `rh_apuracao_mensal` da competencia + tabelas de beneficios.
3. Valida o status: se ha rascunho → erro; se ha calculado → aviso; se tudo validado/pago → "pronto para envio".
4. Renderiza 5 secoes (A Assiduidade, B VA, C Deslocamento por cidade, D Alianca, E Resumo Geral) com totais bruto/desconto/liquido.
5. Exporta CSV para folha, imprime (com assinaturas RH/Diretor) ou copia o resumo.

### 3.4 Pagamentos (`exportar_pagamentos.html`)

1. **Dados Bancarios:** cadastra/edita/valida forma de pagamento por funcionario (conta_itau/conta_sicredi/pix/cheque), com campos condicionais; status_validacao pendente → validado.
2. **Lotes:** cria lote de pagamento (por competencia/forma) com contagem de funcionarios; status rascunho → em_conferencia → pronto → enviado → finalizado.
3. **Exportar:** gera lista (XLSX/CSV) por forma de pagamento ou pendentes; grava log de exportacao.
4. **Historico:** log de exportacoes.

---

## 4. Usuarios

### 4.1 Perfis e Permissao

| Tela | Controle |
|------|----------|
| beneficios.html | `Shell.init`; `visualizador` = read-only (sem botoes, modal travado, bulk bloqueado) |
| exportar_pagamentos.html | `RHAuth.init` — **sem checagem de perfil** (qualquer autenticado edita/exporta) |
| relatorio_beneficios.html | `RHAuth.init` — **sem checagem de perfil** (qualquer autenticado gera/exporta) |

### 4.2 Auditoria de Acao

`RHAuth.log`: `beneficio_atualizado`, `bulk_ativar_va`, `bulk_auto_deslocamento` (beneficios); `export_csv_beneficios`, `print_relatorio_beneficios`, `copy_resumo_beneficios` (relatorio); log de exportacao em `rh_log_exportacao` (pagamentos).

---

## 5. Fluxograma

```
GESTAO DE BENEFICIOS                    APURACAO MENSAL (27-26)
===================                     =======================
Colaboradores ativos                   Frequencia (Secullum):
   |                                      dias_trabalhados, faltas,
   v (modal por colaborador)              atrasos, atestados, advert.
VA (300 / -8% salario)                    ferias, INSS, maternidade
Deslocamento (por cidade)                    |
Alianca (custeado empresa)                   v
Premio Assiduidade (150)               Elegibilidade + valores:
   | POST/PATCH                          assiduidade (perde por falta/
   v   rh_vale_alimentacao                 atraso/advertencia/atestado)
   rh_auxilio_deslocamento                VA (proporcional dias, perde 15+ faltas)
   rh_plano_alianca                       deslocamento (suspenso ferias/INSS/faltas)
   (bulk VA / bulk desloc)                   |
                                            v
                                       status: rascunho -> calculado ->
RELATORIO CONSOLIDADO                          validado -> pago
====================                          |
Selecionar competencia                        v
   | le rh_apuracao_mensal            (base para relatorio + folha)
   v
Valida status (rascunho/calc/valid)
   |
   v
5 secoes: A Assiduidade | B VA |
  C Deslocamento(por cidade) |
  D Alianca | E Resumo
   |
   v
Export CSV folha | Imprimir | Copiar

PAGAMENTOS
==========
Dados bancarios (Itau/Sicredi/PIX/Cheque)
   | validar (pendente -> validado)
   v
Lote de pagamento (por forma/competencia)
   | rascunho -> conferencia -> pronto -> enviado -> finalizado
   v
Exportar lista (XLSX/CSV, sem CNAB, Valor vazio) + log
```

---

## 6. Regras de Negocio

### RN-01 — Vale Alimentacao (VA)

Valor mensal default R$ 300,00; desconto = **8% do salario_base** (`percentual_desconto` default 8). Liquido colaborador = valor − desconto. Regra: proporcional a dias trabalhados; **perde 100% com 15+ faltas**; nao recebe em ferias. `UNIQUE(colaborador_id)`.

### RN-02 — Auxilio Deslocamento (por cidade)

Valor por cidade de `rh_deslocamento_cidades` (Jardim Alegre 150, Lidianopolis 200, Ivaipora 200, Lunardelli 270, Sao Joao do Ivai 270, Sao Pedro do Ivai 320). Horario de transporte 08h-17h; se trabalha fora do horario, ainda recebe. **Suspenso em:** ferias, afastamento INSS >15d, licenca maternidade, >2 faltas injustificadas. `UNIQUE(colaborador_id)`.

### RN-03 — Cidades com Transporte da Empresa

Lista hardcoded no frontend: Ivaipora, Sao Joao do Ivai, Sao Pedro do Ivai, Lunardelli (match por texto normalizado). So nessas cidades o checkbox "Usa transporte da empresa (08h-17h)" aparece.

### RN-04 — Premio Assiduidade

Valor fixo R$ 150,00/mes. Elegibilidade automatica na apuracao mensal. **Perde se:** falta injustificada, >3 atrasos >10min, saida antecipada nao autorizada, advertencia/suspensao, >3 dias de atestado. Proporcional para admissao, desligamento, afastamento INSS. **Nao tem tabela dedicada** — calculado em `rh_apuracao_mensal` (elegivel_assiduidade, valor_assiduidade, motivo_perda_assiduidade).

### RN-05 — Plano Alianca

100% custeado pela empresa (sem custo para o colaborador, sem dependentes). Todos CLT elegiveis. `data_adesao` gravada = data atual ao ativar. `UNIQUE(colaborador_id)`.

### RN-06 — Custo Mensal Estimado (KPI beneficios)

`custo = Σ (VA ativo ? valor_mensal||300) + (desloc ativo ? valor_mensal) + 150 por colaborador`. **O Premio Assiduidade (150) e somado incondicionalmente para TODOS os colaboradores**, independente de elegibilidade (estimativa grosseira — ver D-05).

### RN-07 — Competencia 27-26

O periodo de apuracao vai do **dia 27 do mes anterior** ao **dia 26 da competencia** (`periodo_inicio`/`periodo_fim`). Ex: competencia 2026-03 → 27/02 a 26/03.

### RN-08 — Status da Apuracao

`rascunho → calculado → validado → pago` (CHECK). O relatorio consolidado so libera envio a contabilidade quando todos os registros estao `validado`/`pago`; se ha `rascunho` erro, se ha `calculado` aviso.

### RN-09 — Proporcionalidade de Assiduidade

No relatorio, "Proporcional=Sim" quando `elegivel_assiduidade AND dias_trabalhados < dias_uteis_periodo AND valor > 0`.

### RN-10 — Formas de Pagamento

`rh_dados_bancarios.forma_pagamento IN (conta_itau, conta_sicredi, pix, cheque)`. Campos condicionais: conta (banco/agencia/conta/tipo/titular — banco pre-preenchido Itau/Sicredi), PIX (tipo_chave cpf/email/telefone/aleatoria + chave + titular), cheque (motivo obrigatorio + responsavel). Status validacao: pendente → validado (botao) → bloqueado (estado sem botao). `UNIQUE(funcionario_id)` via drift.

### RN-11 — Lote de Pagamento

`rh_lotes_pagamento.status IN (rascunho, em_conferencia, pronto, enviado, finalizado)`. Gera lote considerando so funcionarios ativos, filtrando por forma de pagamento se informada; grava `total_funcionarios`. Nome automatico sugerido `folha_<mes>_<ano>_<empresa>`.

### RN-12 — Exportacao SEM CNAB

Nenhuma tela gera arquivo de remessa bancaria (CNAB240/400). Saidas sao XLSX/CSV genericos:
- Lista por forma (`pagamentos_<tipo>_<AAAA-MM>.xlsx|csv`) com 17 colunas (Nome, Matricula, CPF, Empresa/Filial vazios, Setor, Cargo, Forma, Banco, Agencia, Conta, Tipo_Conta, Chave_PIX, Titular, CPF_Titular, Status, Obs).
- Lote (`lote_<nome>.csv`): `Nome,CPF,Banco,Agencia,Conta,Tipo,Valor` — **coluna Valor sempre vazia** (a tela nao calcula valores).

### RN-12b — CSV para Folha (relatorio)

`beneficios_<competencia>_folha.csv`: header `NOME;CPF;PREMIACAO_ASSIDUIDADE;VALE_ALIMENTACAO;DESC_VA;AUXILIO_DESLOCAMENTO`, separador `;`, BOM UTF-8, decimais BR (virgula), CPF formatado, nome limpo de `;`/quebras.

### RN-13 — Pendentes de Dados Bancarios

Funcionario ativo sem registro em `rh_dados_bancarios` = pendente (KPI + export `pendentes` marca "SEM DADOS BANCARIOS"). Alerta de N pagamentos por cheque (verificar justificativa).

### RN-14 — Degradacao Silenciosa (tabelas fantasma)

`exportar_pagamentos.html` envolve cada leitura em try/catch retornando `[]`. Como `rh_dados_bancarios`/`rh_lotes_pagamento`/`rh_log_exportacao` nao existem em prod (DRIFT_MAP), a tela carrega vazia sem quebrar — mas nao persiste nada. Ver D-01.

### RN-15 — Merge do Relatorio

Base = `rh_apuracao_mensal` (competencia), enriquecida por `colaborador_id` com colaborador (nome/cpf/cidade/salario), VA (valor/percentual/ativo), deslocamento (cidade/valor/ativo) e alianca (ativo). Colaborador ausente → "Desconhecido". So `status in (ativo, afastado, ferias)`.

---

## 7. Campos

### 7.1 Modal de Beneficio (`beneficios.html`)

| Beneficio | Controle | ID | Tipo |
|-----------|----------|----|----|
| VA | Status | `chk-va` | checkbox |
| VA | Valor/Desconto/Liquido | — | display (fixo 300 / 8%) |
| Deslocamento | Status | `chk-dl` | checkbox |
| Deslocamento | Cidade origem | `sel-cidade` | select (de rh_deslocamento_cidades) |
| Deslocamento | Usa transporte | `chk-transporte` | checkbox (so cidades com transporte) |
| Assiduidade | Valor | — | badge fixo R$150 |
| Alianca | Status | `chk-al` | checkbox |
| Alianca | Data adesao | — | display (grava hoje) |

Bulk: `Ativar VA para todos` (`_bulkVA`), `Auto-config deslocamento` (`_bulkDesloc`). Filtro `f-busca`.

### 7.2 Dados Bancarios (`exportar_pagamentos.html`)

Forma (`filterFormaPag`), campos condicionais (banco/agencia/conta/tipo/titular/cpf | tipo_chave/chave/titular_pix | motivo/responsavel), status_validacao. Filtros: `searchDados`, `filterFormaPag`, `filterValidacao`.

### 7.3 Relatorio (`relatorio_beneficios.html`)

`selMes`, `selAno` → competencia `AAAA-MM`. Botao Gerar Relatorio.

### 7.4 Apuracao Mensal (`rh_apuracao_mensal`)

Frequencia: dias_trabalhados, dias_uteis_periodo, faltas_justificadas/injustificadas, atrasos_acima_10min, dias_atestado, saidas_antecipadas, advertencias, suspensoes, em_ferias, dias_ferias, afastado_inss, licenca_maternidade. Resultados: elegivel_assiduidade/valor_assiduidade/motivo_perda, elegivel_va/valor_va/desconto_va, elegivel_deslocamento/valor_deslocamento/motivo_suspensao, plano_alianca_ativo, total_beneficios, total_descontos, status.

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave |
|--------|-----------|---------------|
| `rh_vale_alimentacao` | VA por colaborador | colaborador_id (UNIQUE), ativo, valor_mensal (300), percentual_desconto (8) |
| `rh_auxilio_deslocamento` | Deslocamento por colaborador | colaborador_id (UNIQUE), ativo, cidade_origem, valor_mensal, usa_transporte_empresa |
| `rh_plano_alianca` | Adesao ao plano | colaborador_id (UNIQUE), ativo, data_adesao |
| `rh_deslocamento_cidades` | Tabela de valores por cidade | cidade (UNIQUE), valor, distancia_km, ativo (6 cidades seed) |
| `rh_apuracao_mensal` | Apuracao 27-26 (base de calculo) | colaborador_id, competencia (UNIQUE juntos), periodo_inicio/fim, frequencia, elegibilidade+valores, totais, status |
| `rh_beneficio` | Beneficio generico (RH core) | funcionario_id, tipo, status, data_inicio/fim, valor, documento_ref |
| `rh_dados_bancarios` | Dados bancarios (**nao deployada**) | funcionario_id, forma_pagamento (CHECK 4), banco/agencia/conta/tipo, chave_pix, status_validacao |
| `rh_lotes_pagamento` | Lote de pagamento (**nao deployada**) | nome, empresa, periodo, forma_pagamento, total_funcionarios, status (CHECK 5) |
| `rh_log_exportacao` | Log de exportacao (**nao deployada**) | usuario, tipo_lista, formato, total_registros, filtros |
| `fp_lote_pagamento` | Lote de pagamento (financeiro `fp_*`) | lote_folha_id, tipo_pagamento, data, banco, status, total_bruto/liquido, qtd, arquivo_bancario |
| `fp_pagamento_funcionario` | Pagamento por funcionario (`fp_*`) | lote_pagamento_id, funcionario_id, valor_liquido, banco/agencia/conta/chave_pix, status |

---

## 9. RPCs

**Nenhuma RPC** — o modulo opera integralmente por PostgREST direto (GET/POST/PATCH). A apuracao mensal (`rh_apuracao_mensal`) e a camada de calculo, populada por processo externo (apuracao/importacao Secullum), nao por RPC deste modulo.

---

## 10. Functions (Backend SQL)

Sem functions especificas do modulo. Triggers de `atualizado_em`/`updated_at` seguem o padrao RH. O calculo de elegibilidade/valores de assiduidade, VA e deslocamento e feito na geracao de `rh_apuracao_mensal` (fora do escopo das 3 telas, que leem/gravam mas nao calculam a apuracao).

---

## 11. Triggers

Triggers de timestamp (`atualizado_em`) nas tabelas de beneficios e apuracao (padrao RH). Sem trigger de negocio proprio (ativacao de beneficio nao dispara recalculo automatico da apuracao).

---

## 12. APIs (Chamadas Frontend)

### 12.1 beneficios.html (`API`)

- **GET:** rh_colaboradores (ativos, select id/nome/cpf/cargo/cidade/salario_base/turno), rh_vale_alimentacao, rh_auxilio_deslocamento, rh_plano_alianca, rh_deslocamento_cidades (ativo)
- **POST:** rh_vale_alimentacao, rh_auxilio_deslocamento, rh_plano_alianca
- **PATCH:** os 3 acima por `colaborador_id=eq.{id}`

### 12.2 exportar_pagamentos.html (`RH_API`)

- **GET:** rh_funcionarios, rh_dados_bancarios, rh_lotes_pagamento, rh_log_exportacao (limit 100) — todas com try/catch → []
- **POST:** rh_dados_bancarios, rh_lotes_pagamento, rh_log_exportacao
- **PATCH:** rh_dados_bancarios (`id=eq.{id}`, validar)

### 12.3 relatorio_beneficios.html (`API.get`)

- **GET:** rh_apuracao_mensal (competencia), rh_colaboradores (ativo/afastado/ferias), rh_vale_alimentacao, rh_auxilio_deslocamento, rh_plano_alianca

### 12.4 Bibliotecas Externas

SheetJS xlsx@0.18.5 (exportar_pagamentos — geracao de XLSX).

---

## 13. Integracoes

### 13.1 Secullum (Ponto)

Alimenta a frequencia de `rh_apuracao_mensal` (dias trabalhados, faltas, atrasos, atestados) — base de elegibilidade dos beneficios.

### 13.2 Contabilidade / Folha

Destino do CSV consolidado (`relatorio_beneficios`) e das listas de pagamento (`exportar_pagamentos`). Formato planilha/CSV (nao CNAB).

### 13.3 Bancos (Itau, Sicredi, PIX)

Formas de pagamento cadastradas em `rh_dados_bancarios`. **Sem integracao automatizada** (exportacao manual, sem remessa CNAB).

### 13.4 Sistema Financeiro `fp_*`

Estrutura paralela (`fp_lote_pagamento`, `fp_pagamento_funcionario`) — sistema de folha/pagamento nao usado pelas telas de beneficios (que usam as tabelas `rh_*` fantasma). Ver D-02.

---

## 14. Dashboards

### 14.1 KPIs de Beneficios (`beneficios.html`)

4 KPIs: Com Vale Alimentacao, Com Aux. Deslocamento, Com Plano Alianca, Custo Mensal Estimado (RN-06).

### 14.2 KPIs de Pagamentos (`exportar_pagamentos.html`)

6 KPIs: Conta Itau, Conta Sicredi, PIX, Cheque, Pendentes (ativos sem dados), Bloqueados.

### 14.3 KPIs do Relatorio (`relatorio_beneficios.html`)

5 KPIs: Colaboradores, Elegiveis Assiduidade, Total Bruto, Total Descontos, Total Liquido.

---

## 15. Relatorios

### 15.1 Relatorio Consolidado de Beneficios (5 secoes)

- **A — Premio Assiduidade:** Nome, Elegivel, Motivo Perda, Valor, Proporcional (+ total)
- **B — Vale Alimentacao:** Nome, Dias Trab., Valor VA, Desconto 8%, Liquido VA (+ totais)
- **C — Auxilio Deslocamento:** agrupado por cidade — Nome, Cidade, Valor Integral, Dias Trab., Valor Proporcional, Motivo Suspensao (+ subtotal por cidade + total geral)
- **D — Plano Alianca:** Nome, Adesao (+ total aderentes)
- **E — Resumo Geral:** Nome, Assiduidade, VA, Desconto VA, Deslocamento, Total Bruto, Total Desconto, Total Liquido (+ totais gerais)

### 15.2 Exportacoes

- CSV para folha (RN-12b)
- Impressao com cabecalho institucional + assinaturas RH/Diretor
- Copiar resumo (clipboard)
- Listas de pagamento XLSX/CSV (RN-12)

---

## 16. Documentos

### 16.1 Relatorio Impresso para Contabilidade

Cabecalho "Classic Importacao e Exportacao de Couros Ltda", CNPJ 08.849.964/0001-10, competencia e periodo (27-26), 5 secoes, blocos de assinatura (Responsavel RH / Diretor), rodape com versao. `window.print()`.

### 16.2 Lista de Pagamento

Planilha por forma de pagamento (dados bancarios) — documento operacional para o banco/tesouraria.

---

## 17. Evidencias

### 17.1 Log de Exportacao

`rh_log_exportacao` (usuario, tipo_lista, formato, total_registros, filtros) — evidencia de cada exportacao (best-effort; tabela fantasma).

### 17.2 Auditoria de Acao

`RHAuth.log` de ativacao de beneficio, bulk e exportacoes.

### 17.3 Validacao Bancaria

`rh_dados_bancarios.status_validacao` (pendente/validado/bloqueado) e a evidencia de conferencia dos dados de pagamento.

---

## 18. Permissoes

### 18.1 Frontend

- `beneficios.html`: read-only para `visualizador` (bloqueio de botoes/modal/bulk).
- `exportar_pagamentos.html` / `relatorio_beneficios.html`: apenas gate de autenticacao, **sem restricao de perfil** (D-04).

### 18.2 RLS

Tabelas de beneficios seguem o RLS do core RH. Tabelas fantasma (`rh_dados_bancarios`, etc.) nao estao em prod — RLS irrelevante ate deploy.

---

## 19. Auditoria

### 19.1 Log de Acao (`RHAuth.log`)

beneficio_atualizado, bulk_ativar_va, bulk_auto_deslocamento, export_csv_beneficios, print_relatorio_beneficios, copy_resumo_beneficios.

### 19.2 Validado por / em

`rh_apuracao_mensal.validado_por/validado_em` e `calculado_em` registram a trilha da apuracao.

### 19.3 Log de Exportacao

`rh_log_exportacao` (quando deployada).

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Ativar/desativar beneficio | PATCH/POST rh_vale_alimentacao/auxilio_deslocamento/plano_alianca |
| Bulk VA / Bulk deslocamento | `_bulkVA` / `_bulkDesloc` |
| Selecionar cidade | `sel-cidade` onChange → valor + transporte |
| Gerar relatorio | GET rh_apuracao_mensal + beneficios; valida status |
| Exportar CSV folha | `exportarCSV` |
| Salvar dados bancarios | POST/PATCH rh_dados_bancarios (status pendente) |
| Validar dados | PATCH status_validacao=validado |
| Gerar lote | POST rh_lotes_pagamento |
| Exportar lista/lote | XLSX/CSV + POST rh_log_exportacao |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico |
|------------|---------|
| Banco | Supabase PostgreSQL `muiqmtnfvyffborgiwdw` (RH) |
| Auth | Supabase Auth (JWT, RHAuth) |
| Frontend | Vercel (auto-deploy main) |

### 21.2 Bibliotecas

Internas: config.js, api.js/rh_api.js, rh_auth.js, shell.js, ui_components.js, page_bootstrap.js, ds.css. Externas: SheetJS xlsx@0.18.5.

### 21.3 Dependencias de Dados

rh_colaboradores, rh_apuracao_mensal (Secullum), rh_deslocamento_cidades, rh_funcionarios.

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Ponto Secullum | RH | Frequencia para apuracao (elegibilidade) |
| Absenteismo | F-14 RH | Faltas/atestados afetam assiduidade e deslocamento |
| Apuracao (contratos) | RH | Compartilha o conceito de competencia 27-26 |
| Folha de Pagamento | Financeiro `fp_*` | Destino do consolidado de beneficios |
| Gestao de Colaboradores | F-10 RH | Cadastro base (cidade, salario, status) |

---

## 23. Melhorias SIGA

### SIGA-01 — Deploy das Tabelas de Pagamento

`rh_dados_bancarios`, `rh_lotes_pagamento`, `rh_log_exportacao` estao no schema mas nao em prod — a tela de pagamentos e fantasma. Decidir entre deployar `rh_*` ou migrar a tela para `fp_*`.

### SIGA-02 — Unificar Sistemas de Pagamento

Existem dois modelos paralelos (`rh_*` nao deployado e `fp_*` financeiro). Consolidar em um.

### SIGA-03 — Geracao de Remessa CNAB

Implementar CNAB240 (Itau/Sicredi) e arquivo PIX em lote, com valores — hoje a exportacao e planilha sem valores.

### SIGA-04 — Restricao de Perfil em Pagamentos

`exportar_pagamentos` e `relatorio_beneficios` nao checam perfil — restringir a rh/administrador/financeiro.

### SIGA-05 — Corrigir Custo Estimado

O KPI de custo soma R$150 de assiduidade para todos incondicionalmente; usar elegibilidade real da apuracao.

### SIGA-06 — Parametrizar Valores

VA (300), desconto (8%), assiduidade (150) e cidades hardcoded no frontend — mover para tabela de parametros por empresa.

### SIGA-07 — Padronizar Cliente de API

`exportar_pagamentos` usa `rh_api.js`; alinhar ao `api.js` das demais telas.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | `exportar_pagamentos` usa `rh_dados_bancarios`/`rh_lotes_pagamento`/`rh_log_exportacao` — **nao deployadas em prod** (DRIFT_MAP) | Tela fantasma: carrega vazia, nao persiste (try/catch → []) |
| D-02 | Dois sistemas de pagamento paralelos: `rh_*` (nao deployado) vs `fp_*` (financeiro) | Duplicidade estrutural; folha usa `fp_*` |
| D-03 | Exportacao sem CNAB/remessa; coluna Valor sempre vazia no lote | Lista nao serve para debito bancario direto |
| D-04 | `exportar_pagamentos`/`relatorio_beneficios` sem checagem de perfil | Dados bancarios/financeiros a qualquer autenticado (LGPD) |
| D-05 | KPI de custo soma R$150 de assiduidade para todos incondicionalmente | Custo estimado superestimado |
| D-06 | Valores (300/8%/150) e cidades com transporte hardcoded | Nao parametrizavel |
| D-07 | `exportar_pagamentos` usa `rh_api.js` em vez de `api.js` | Inconsistencia de stack |
| D-08 | Modal de VA nao permite editar valor/percentual (fixos na criacao) | Rigidez; excecoes exigem SQL |

### 24.2 Multiempresa

1. `empresa_id` em rh_vale_alimentacao, rh_auxilio_deslocamento, rh_plano_alianca, rh_apuracao_mensal, dados bancarios/lotes
2. RLS por empresa
3. Tabela de valores de beneficio por empresa (VA, assiduidade, cidades)
4. Relatorio e lotes filtrados por empresa/estabelecimento
5. Convencao coletiva por empresa (regras de perda/proporcionalidade)

---

## 25. Criterios de Aceite

### CA-01 — Ativar Beneficio

Ativar VA para um colaborador → registro com 300/8%; badge "Sim"; custo estimado atualiza.

### CA-02 — Deslocamento por Cidade

Selecionar cidade → valor da tabela preenchido; checkbox de transporte so em cidades elegiveis.

### CA-03 — Bulk

Ativar VA para todos → cria/reativa; conta created/activated. Auto-config deslocamento → atribui por cidade, pula sem valor.

### CA-04 — Apuracao 27-26

Relatorio calcula periodo 27 do mes anterior a 26 da competencia.

### CA-05 — Status da Apuracao

Rascunho → erro; calculado → aviso; validado/pago → "pronto para envio".

### CA-06 — Relatorio 5 Secoes

A/B/C/D/E com totais bruto/desconto/liquido corretos; deslocamento agrupado por cidade.

### CA-07 — Export CSV Folha

CSV com header correto, separador `;`, BOM, decimais BR, CPF formatado.

### CA-08 — Dados Bancarios

Cadastrar por forma (campos condicionais); validar (pendente → validado); pendentes = ativos sem dados.

### CA-09 — Lote e Exportacao

Criar lote por forma/competencia; exportar XLSX/CSV; log de exportacao.

### CA-10 — Read-only

Visualizador em beneficios.html sem acoes de escrita.

---

## 26. Casos de Teste

### CT-01 — VA com Desconto

Salario 2000, VA ativo → desconto 160 (8%), liquido 140.

### CT-02 — Deslocamento Ivaipora

Cidade Ivaipora → valor 200; checkbox transporte disponivel.

### CT-03 — Deslocamento Sem Valor

Colaborador em cidade sem cadastro → bulk pula (skipped).

### CT-04 — Assiduidade Perdida

Colaborador com advertencia → elegivel_assiduidade=false, motivo registrado, valor 0.

### CT-05 — Relatorio Rascunho

Competencia com apuracao em rascunho → erro "Finalize o calculo e valide".

### CT-06 — Relatorio Validado

Todos validado/pago → alerta OK; CSV gerado.

### CT-07 — Deslocamento por Cidade (Secao C)

3 colaboradores em 2 cidades → subtotais por cidade + total geral.

### CT-08 — Dados Bancarios PIX

Forma PIX → bloco PIX (tipo_chave + chave + titular); status pendente.

### CT-09 — Export Pendentes

Funcionario ativo sem dados → linha "SEM DADOS BANCARIOS".

### CT-10 — Lote Automatico

Gerar lote automatico → nome folha_<mes>_<ano>, funcionarios ativos contados.

---

## 27. Casos Extremos

### CE-01 — Colaborador sem Cidade

Deslocamento nao configuravel (sem valor).

### CE-02 — Tabela Fantasma

exportar_pagamentos carrega vazio (try/catch → []) sem quebrar; nada persiste.

### CE-03 — Apuracao Inexistente

Relatorio sem apuracao → "Execute a apuracao mensal primeiro".

### CE-04 — Colaborador Desconhecido

Apuracao com colaborador_id sem match → nome "Desconhecido".

### CE-05 — Cheque sem Motivo

Motivo da excecao obrigatorio para forma cheque.

### CE-06 — Custo Superestimado

KPI soma 150 para todos, mesmo inelegiveis (D-05).

### CE-07 — VA em Ferias

Regra documenta perda em ferias, mas o valor real vem da apuracao (nao do toggle).

### CE-08 — 15+ Faltas

VA perde 100% (aplicado na apuracao).

### CE-09 — Valor de Lote Vazio

Export de lote com coluna Valor em branco (D-03).

### CE-10 — Bloqueado sem Botao

Estado `bloqueado` existe nos filtros mas nenhum botao o define no frontend.

---

## 28. Melhorias Futuras

### MF-01 — Remessa Bancaria Automatizada

CNAB240/PIX em lote com valores e retorno bancario.

### MF-02 — Portal do Colaborador

Consulta de beneficios e contracheque pelo proprio colaborador.

### MF-03 — Calculo de Apuracao no Sistema

RPC de apuracao mensal (elegibilidade/valores) integrada ao ponto, com trilha.

### MF-04 — Historico de Beneficios

Versionamento de valores e adesoes ao longo do tempo (hoje 1 registro por colaborador).

### MF-05 — Beneficios Flexiveis

Catalogo de beneficios opcionais (VR, plano de saude, creche) por adesao.

### MF-06 — Integracao com eSocial (S-1010/S-1200)

Rubricas de beneficios no eSocial.

### MF-07 — Simulador de Custo

Projecao de custo de beneficios por cenario (novas contratacoes, reajustes).

### MF-08 — Conciliacao de Pagamento

Cruzar lote enviado x retorno bancario x apuracao para fechar o ciclo.

---

*Documento gerado em 28/07/2026 — Classic IA — Beneficios*
*Versao 1.0 — Especificacao Funcional Completa F-41*
