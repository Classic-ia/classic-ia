# F-14 -- Especificacao Funcional: Absenteismo

---

## 1. Identificacao

| Campo | Valor |
|-------|-------|
| **Nome** | Absenteismo |
| **Codigo** | F-14 |
| **Modulo** | RH |
| **Submodulo** | Processos / Absenteismo |
| **Paginas legado** | `rh/absenteismo.html` (dashboard + CRUD), `rh/ocorrencias.html` (importacao em lote + lancamento manual), `rh/relatorio_atestados.html` (relatorio analitico) |
| **Objetivo** | Registrar, classificar, calcular e analisar todas as ausencias dos colaboradores (faltas justificadas/injustificadas, atestados medicos, afastamentos INSS, licencas maternidade/paternidade, advertencias, suspensoes), produzindo indicadores de absenteismo por funcionario, setor e empresa, com impacto direto no calculo de beneficios (assiduidade, VA, deslocamento) e alimentando o Motor de Decisao (motor_decisoes) e o Dashboard Gerencial. |

---

## 2. Problema que Resolve

### 2.1 Contexto Trabalhista Brasileiro

O absenteismo e um dos maiores custos ocultos para industrias brasileiras. A CLT (Consolidacao das Leis do Trabalho) preve:

- **Art. 473**: Faltas justificadas legais (casamento, obito, doacao sangue, etc.)
- **Art. 131**: Faltas que nao reduzem ferias (atestado medico, servico militar, etc.)
- **Art. 474**: Suspensao disciplinar de ate 30 dias
- **Lei 8.213/91 (INSS)**: Primeiros 15 dias de afastamento por doenca sao pagos pelo empregador; a partir do 16o dia, o INSS assume via auxilio-doenca (B31/B91)
- **NR-7 (PCMSO)**: Obriga controle de atestados e exames ocupacionais
- **NR-17 (Ergonomia)**: Absenteismo por LER/DORT deve ser monitorado
- **eSocial**: Eventos S-2230 (afastamento temporario) exigem rastreabilidade total

### 2.2 Processo que Atende

- Registro centralizado de todas as ausencias (6 tipos distintos)
- Importacao em massa de atestados e advertencias por nome de arquivo PDF
- Integracao com API Convenia para sincronizacao de afastamentos
- Calculo automatico do percentual de absenteismo (formula padrao OIT/ABNT)
- Identificacao de setores criticos e funcionarios recorrentes
- Calculo de impacto em beneficios de assiduidade (perda se >3 dias atestado ou qualquer advertencia/suspensao)
- Alimentacao do Motor de Decisao para planos de acao automaticos
- Relatorio analitico com cruzamento por CID, medico emissor, local de atendimento e dia da semana

### 2.3 Dor que Resolve

- **Antes**: Controle manual em planilhas, sem visibilidade de recorrencia. Perda de assiduidade calculada manualmente pela contadora.
- **Depois**: Sistema automatizado com KPIs em tempo real, alertas de padrao suspeito (medicos com alta emissao), e integracao direta com apuracao de beneficios.

---

## 3. Processo Operacional Completo

### 3.1 Fluxo de Registro

1. **Origem do dado**: O fato gerador (falta, atestado, advertencia, afastamento) ocorre no dia-a-dia da operacao.
2. **Entrada no sistema**: Via um dos 4 canais:
   - **Canal A**: Importacao em lote (drag-and-drop de PDFs de atestados/advertencias) -- `ocorrencias.html`, aba "Atestados" ou "Advertencias"
   - **Canal B**: Lancamento manual individual -- `ocorrencias.html`, aba "Lancamento Manual"
   - **Canal C**: CRUD direto via modal -- `absenteismo.html`, botao "+ Nova Ocorrencia"
   - **Canal D**: Sincronizacao automatica via API Convenia -- `absenteismo.html`, botao "Sincronizar Convenia"
3. **Matching de colaborador**:
   - Canal A: Parser de nome do arquivo PDF + fuzzy matching com base de colaboradores (5 niveis: exato, contem, nome_sobrenome, primeiro_nome, manual)
   - Canal B: Busca com autocomplete (debounce 200ms, minimo 2 caracteres)
   - Canal C: Select dropdown com todos os funcionarios
   - Canal D: Match por CPF ou nome com base Convenia
4. **Pre-visualizacao e validacao**: Canais A e D exibem preview tabular antes do salvamento, com contagem de matched/erros.
5. **Persistencia**: INSERT na tabela `rh_ocorrencias` via `API.post('rh_ocorrencias', payload)`.
6. **Deduplicacao**: Canal D verifica duplicatas por (colaborador_id + data_inicio + tipo) antes do insert.
7. **Calculo de KPIs**: Apos salvamento, recalcula KPIs no frontend (faltas no mes, atestados no mes, % absenteismo, setor critico).
8. **Impacto em beneficios**: O Resumo Mensal (`ocorrencias.html`, aba "Resumo Mensal") calcula quem perde assiduidade, alimentando `rh_apuracao_mensal`.
9. **Alimentacao do Motor de Decisao**: Views `vw_absenteismo_real`, `vw_absenteismo_por_setor`, `vw_absenteismo_recorrencia` sao consumidas pela RPC `motor_decisoes()`.
10. **Relatorio analitico**: RPC `relatorio_atestados_dashboard()` consome `vw_atestados_analise` para gerar analises por CID, medico, local e dia da semana.

### 3.2 Fluxo de Apuracao Mensal (Impacto em Beneficios)

1. RH seleciona a competencia (mes/ano) na aba "Resumo Mensal" de `ocorrencias.html`.
2. Sistema carrega todas as ocorrencias da competencia e todos os colaboradores ativos.
3. Agrupa por colaborador: soma dias de atestado, quantidade de advertencias, dias de suspensao.
4. Aplica regra de perda de assiduidade: `diasAtestado > 3 OR qtdAdvertencias > 0 OR diasSuspensao > 0 => PERDE`.
5. Exibe KPIs de resumo (total dias atestado, total advertencias, colaboradores com ocorrencias, % que perdem assiduidade).
6. Exibe tabela detalhada por colaborador com impacto individualizado.

---

## 4. Usuarios

| Perfil | Acoes Permitidas |
|--------|-----------------|
| **administrador** | CRUD completo em ocorrencias; visualizacao de todos os KPIs, relatorios e dados sensiveis (CID, medico, custos); importacao em lote; sincronizacao Convenia; exclusao de registros; acesso a relatorio de atestados com analise por medico; acesso ao Motor de Decisao |
| **rh** | CRUD completo em ocorrencias; visualizacao de todos os KPIs e relatorios; importacao em lote; sincronizacao Convenia; exclusao de registros; acesso a relatorio de atestados; acesso ao Motor de Decisao |
| **gestor** | Leitura de KPIs agregados do seu setor (sem nomes individuais em dados de saude); acesso ao relatorio de atestados com visao gestor (dados agregados, sem CID/medico/nomes); contagem de atestados dos ultimos 30 dias do setor via `gestor_dashboard()` |
| **gestor_confianca** | Mesmas permissoes do gestor, acrescido de visualizacao de atestados por funcionario nos ultimos 6 meses (contagem, sem CID) via `gestor_confianca_dashboard()` |
| **visualizador** | Sem acesso a pagina de absenteismo (route filtrada no shell.js por `perfis: ['administrador','rh']`) |
| **sst** | Leitura de ocorrencias (policy `ocorr_select_admin_rh` inclui perfil 'sst') para correlacao com acidentes e restricoes medicas |

### 4.1 Controle de Acesso por Pagina

| Pagina | Perfis com Acesso (shell.js) |
|--------|------------------------------|
| `absenteismo.html` | administrador, rh |
| `ocorrencias.html` | administrador, rh |
| `relatorio_atestados.html` | administrador, rh, gestor |

---

## 5. Fluxograma

### 5.1 Fluxo Principal -- Registro de Ocorrencia (absenteismo.html)

```
[Inicio] --> [Usuario abre absenteismo.html]
  --> [Shell.init autentica usuario]
  --> [loadAll(): carrega ocorrencias + funcionarios + setores em paralelo]
  --> [populateColaboradorSelect(): preenche dropdown do modal]
  --> [Auto-seleciona competencia do mes mais recente com dados]
  --> [renderKPIs() + renderTable()]
  --> [Usuario interage]
    --> [?] Filtrar?
      --> [SIM] --> [Altera fBusca/fTipo/fCompetencia] --> [renderTable()] --> [Volta para interacao]
      --> [NAO] --> [?] Nova Ocorrencia?
        --> [SIM] --> [openModal()] --> [Preenche campos obrigatorios: Funcionario, Tipo, Data Inicio]
          --> [autoFillCompetencia(): YYYY-MM da data inicio]
          --> [calcDias(): calculo automatico de dias se data fim informada]
          --> [salvar()] --> [?] id preenchido?
            --> [SIM] --> [API.patch('rh_ocorrencias?id=eq.' + id, payload)]
            --> [NAO] --> [API.post('rh_ocorrencias', payload)]
          --> [?] res.ok?
            --> [SIM] --> [closeModal()] --> [Recarrega ocorrencias] --> [renderKPIs() + renderTable()]
            --> [NAO] --> [alert('Erro ao salvar: ' + res.error)]
        --> [NAO] --> [?] Editar?
          --> [SIM] --> [editarOcorrencia(id)] --> [openModal(data)] --> [mesmo fluxo de salvar]
        --> [?] Sincronizar Convenia?
          --> [SIM] --> Fluxo 5.2
        --> [?] Ordenar?
          --> [SIM] --> [Click em th.sortable] --> [Alterna sortCol/sortDir] --> [renderTable()]
```

### 5.2 Fluxo de Sincronizacao Convenia

```
[Click btnSyncConvenia]
  --> [?] Token Convenia configurado?
    --> [NAO] --> [confirm()] --> [Redireciona para convenia_integracao.html]
    --> [SIM] --> [Loop paginado: GET /api/v3/leaves?page=N&per_page=100]
      --> [Indexa colaboradores por CPF e nome]
      --> [Para cada leave:]
        --> [Mapeia tipo (tipoMap)]
        --> [Calcula dias]
        --> [Match por CPF ou nome]
        --> [?] Encontrou colaborador?
          --> [NAO] --> [semFunc++] --> [CONTINUE]
          --> [SIM] --> [?] Duplicata? (API.get check)]
            --> [SIM] --> [dup++] --> [CONTINUE]
            --> [NAO] --> [API.post('rh_ocorrencias', payload)]
              --> [?] res.ok?
                --> [SIM] --> [inseridos++]
                --> [NAO] --> [erro++]
      --> [Exibe status final: inseridos, duplicatas, sem match, erros]
      --> [loadAll() + renderKPIs() + renderTable()]
```

### 5.3 Fluxo de Importacao em Lote (ocorrencias.html)

```
[Inicio: Tab Atestados ou Advertencias]
  --> [Drop/Click zona de arquivos OU Cola texto]
  --> [Click "Importar"]
  --> [Parse de nomes de arquivo (parseAtestadoFilename / parseAdvertenciaFilename)]
    --> [Regex NOME DDMMAAAA [a DDMMAAAA]]
    --> [matchColaborador(): fuzzy match 5 niveis]
    --> [Auto-detecta categoria (ate_2_dias / 3_dias_ou_mais)]
    --> [Auto-detecta competencia pelo mes da data]
  --> [renderPreview(): tabela com matched/erros/selecao manual]
  --> [?] Existem registros sem match?
    --> [SIM] --> [Exibe dropdown para selecao manual do colaborador]
  --> [Click "Salvar no banco"]
  --> [Loop: API.post('rh_ocorrencias', payload) para cada valido com match]
  --> [Toast: X salvo(s), Y erro(s)]
  --> [RHAuth.log('importar_atestados/importar_advertencias', { quantidade })]
```

---

## 6. Regras de Negocio

### Calculo do Indicador de Absenteismo

**RN-01**: O percentual de absenteismo e calculado pela formula:
```
% Absenteismo = (Total de dias perdidos no mes / (Funcionarios ativos * 22 dias uteis)) * 100
```
Onde:
- `Total de dias perdidos` = soma do campo `dias` de TODAS as ocorrencias do mes (todos os tipos)
- `Funcionarios ativos` = contagem de registros em `rh_funcionarios` com `status = 'ativo'`
- `22` = constante fixa representando dias uteis medios do mes

**RN-02**: A competencia de referencia para KPIs e automaticamente definida como o mes mais recente com dados. Se nao houver dados, usa o mes corrente (currentYM()).

### Classificacao por Tipo

**RN-03**: Os tipos de ocorrencia aceitos no `absenteismo.html` sao:
| Tipo (valor) | Label exibido | Cor badge |
|--------------|---------------|-----------|
| `atestado` | Atestado | badge-atestado |
| `falta_justificada` | Falta Justificada | badge-falta_justificada |
| `falta_injustificada` | Falta Injustificada | badge-falta_injustificada |
| `afastamento_inss` | Afastamento INSS | badge-afastamento_inss |
| `licenca_maternidade` | Lic. Maternidade | badge-licenca_maternidade |
| `licenca_paternidade` | Lic. Paternidade | badge-licenca_paternidade |

**RN-04**: Os tipos aceitos no `ocorrencias.html` (tabela original) diferem:
| Tipo (valor) | Label exibido |
|--------------|---------------|
| `atestado` | Atestado Medico |
| `advertencia_verbal` | Adv. Verbal |
| `advertencia_escrita` | Adv. Escrita |
| `suspensao` | Suspensao |

**RN-05**: A tabela `rh_ocorrencias` possui CHECK constraint original: `tipo IN ('atestado','advertencia_verbal','advertencia_escrita','suspensao')`. O `absenteismo.html` insere tipos expandidos (`falta_justificada`, `falta_injustificada`, `afastamento_inss`, `licenca_maternidade`, `licenca_paternidade`) que exigem alteracao do CHECK constraint na base.

### Impacto em Assiduidade

**RN-06**: Regra de perda de beneficio de assiduidade (implementada em `ocorrencias.html`, tab Resumo Mensal):
```
PERDE ASSIDUIDADE SE:
  - dias_atestado > 3 (mais de 3 dias de atestado no mes)
  - OR qtd_advertencias > 0 (qualquer advertencia verbal ou escrita)
  - OR dias_suspensao > 0 (qualquer suspensao)
```

**RN-07**: O motivo da perda de assiduidade e registrado como texto composto, exemplo: `"Atestado > 3 dias, 1 advertencia(s)"`.

### Calculo de Dias

**RN-08**: Quando Data Inicio e Data Fim sao informados, o numero de dias e calculado automaticamente:
```
dias = Math.round((dataFim - dataInicio) / (1000 * 60 * 60 * 24)) + 1
```
O +1 garante que o dia de inicio conta (inclusivo).

**RN-09**: Se apenas Data Inicio e informada (sem Data Fim), o padrao e `dias = 1`.

### Competencia

**RN-10**: A competencia e auto-preenchida com os 7 primeiros caracteres da Data Inicio (YYYY-MM). Se nao informada manualmente, e derivada: `data_inicio.slice(0, 7)`.

### Setor Critico

**RN-11**: O "Setor Critico" e definido como o setor com maior soma de dias perdidos na competencia corrente. O calculo e:
1. Para cada ocorrencia do mes, busca o `setor_id` do funcionario via `funcMap[colaborador_id].setor_id`
2. Acumula dias por setor
3. O setor com `maxDias` e exibido como setor critico

### Custo Empresarial (vw_absenteismo_real)

**RN-12**: O custo real para a empresa por afastamento e calculado pela view `vw_absenteismo_real`:
```
salario_dia = salario_base / 22
dias_empresa = MIN(dias_afastamento, 14)  -- empresa paga ate 14 dias
dias_inss = MAX(dias_afastamento - 14, 0)  -- INSS assume a partir do 15o dia

custo_empresa_real = dias_empresa * salario_dia * fator_encargo
  onde fator_encargo = 1.5 (se dias <= 3, curto prazo) ou 1.3 (se dias > 3)

custo_empresa_bruto = dias_empresa * salario_dia
```

### Classificacao de Faixa (vw_absenteismo_real)

**RN-13**: Os afastamentos sao classificados em faixas:
| Faixa | Criterio |
|-------|----------|
| `curto` | dias <= 3 |
| `medio` | 4 <= dias <= 14 |
| `longo_inss` | dias > 14 |

### Classificacao de Recorrencia (vw_absenteismo_recorrencia)

**RN-14**: Funcionarios sao classificados por recorrencia de atestados:
| Classificacao | Criterio |
|---------------|----------|
| RECORRENTE CRITICO | count >= 10 AND curtos >= 8 |
| RECORRENTE | count >= 5 |
| AFASTAMENTO PROLONGADO | sum(dias) >= 30 |
| NORMAL | demais |

**RN-15**: A taxa de atestados por mes e calculada: `count / MAX(meses entre primeiro e ultimo atestado, 1)`.

### Importacao de Atestados por Arquivo

**RN-16**: O parser de nome de arquivo aceita os padroes:
- `NOME DDMMAAAA a DDMMAAAA.pdf` (periodo)
- `NOME DDMMAAAA.pdf` (data unica)
- Fallback: qualquer sequencia de 8 digitos no nome

**RN-17**: O matching de colaborador usa 5 niveis de prioridade:
1. **Exato**: nome normalizado do arquivo == nome normalizado do colaborador
2. **Contem**: nome do arquivo contem o nome completo do colaborador
3. **Parcial**: nome do colaborador contem o nome do arquivo (min 3 chars)
4. **Nome + Sobrenome**: primeiro e ultimo nome coincidem
5. **Primeiro nome**: apenas primeiro nome coincide (min 4 chars)

**RN-18**: A normalizacao de texto remove acentos, converte para maiuscula, remove caracteres especiais.

**RN-19**: Para advertencias importadas por arquivo, o tipo fixo e `advertencia_escrita` e `dias = 0`.

### Sincronizacao Convenia

**RN-20**: O mapeamento de tipo da Convenia segue prioridade:
| Texto Convenia (parcial, case-insensitive) | Tipo mapeado |
|-------------------------------------------|--------------|
| atestad, medical, doenc | atestado |
| maternidade, maternity | licenca_maternidade |
| paternidade, paternity | licenca_paternidade |
| inss, previdenc | afastamento_inss |
| injustif | falta_injustificada |
| falta, justif | falta_justificada |
| (default) | atestado |

**RN-21**: A deduplicacao Convenia verifica: `colaborador_id + data_inicio + tipo`. Se ja existe registro com mesma combinacao, pula o registro.

**RN-22**: O token Convenia e armazenado em `localStorage['convenia_config']` como JSON `{ token: '...' }`.

**RN-23**: A paginacao Convenia busca ate 30 paginas com 100 registros por pagina (maximo 3.000 registros por sincronizacao).

### Relatorio de Atestados (relatorio_atestados.html)

**RN-24**: Medicos com 5+ atestados emitidos OU media de dias >= 10 sao classificados como "Investigar" (isSuspect).

**RN-25**: Alertas de medicos sao gerados quando um CRM possui 3+ atestados no periodo de 2 anos (ano selecionado e anterior).

**RN-26**: O mapeamento de CID para grupo e feito pela primeira letra:
| Prefixo CID | Grupo |
|-------------|-------|
| M | Musculoesqueletico |
| K | Digestivo |
| J | Respiratorio |
| R | Sintomas gerais |
| S, T | Trauma/Lesao |
| F | Saude mental |
| G | Neurologico |
| H | Olhos/Ouvido |
| N | Geniturinario |
| Z | Fatores saude |
| outros | Outro |

**RN-27**: O local de atendimento e inferido por pattern matching no texto extraido do OCR:
| Texto (ILIKE) | Local |
|--------------|-------|
| santa casa | Santa Casa |
| hospital municipal | Hospital Municipal |
| UBS, unidade basica | UBS |
| UPA | UPA |
| clinica | Clinica |
| hospital | Hospital |
| jardim alegre | Hospital Jardim Alegre |

**RN-28**: A visao do gestor no relatorio de atestados e restrita: secoes de CID, medico, funcionarios individuais e alertas sao ocultadas. Apenas dados agregados (por setor, por gravidade, por dia da semana) sao exibidos.

**RN-29**: Na apuracao mensal (rh_apuracao_mensal), a competencia segue o calendario 27-26 (ex: competencia 2026-03 = periodo 2026-02-27 a 2026-03-26).

---

## 7. Campos

### 7.1 Tabela `rh_ocorrencias`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra | Valor Padrao | Mascara | Observacoes |
|---|-------|----------|-------------|--------|-----------|-------|--------------|---------|-------------|
| 1 | `id` | UUID | Sim (auto) | Sistema | - | PK auto-gerada | `gen_random_uuid()` | - | Chave primaria |
| 2 | `colaborador_id` | UUID | Sim | Usuario/Sistema | FK para `rh_colaboradores(id)` / `rh_funcionarios(id)` | Funcionario deve existir na base | - | - | Referencia cruzada: `absenteismo.html` usa `rh_funcionarios.id`, `ocorrencias.html` usa `rh_colaboradores.id` |
| 3 | `tipo` | TEXT | Sim | Usuario/Sistema | CHECK: `atestado`, `advertencia_verbal`, `advertencia_escrita`, `suspensao` (constraint original). `absenteismo.html` tambem insere `falta_justificada`, `falta_injustificada`, `afastamento_inss`, `licenca_maternidade`, `licenca_paternidade` | Determina classificacao e impacto | - | - | CHECK constraint precisa ser expandido |
| 4 | `data_inicio` | DATE | Sim | Usuario/Parser | Formato ISO (YYYY-MM-DD) | Deve ser data valida | - | DD/MM/YYYY (display) | Usada para derivar competencia |
| 5 | `data_fim` | DATE | Nao | Usuario/Parser | Formato ISO | Deve ser >= data_inicio se informada | NULL | DD/MM/YYYY (display) | Para atestados e suspensoes; NULL para advertencias |
| 6 | `dias` | INTEGER | Nao | Auto-calculado/Usuario | Min 1 (no form), DEFAULT 1 (SQL) | Auto-calc: `(data_fim - data_inicio) + 1`. Para advertencias: 0 | 1 | - | Pode ser editado manualmente |
| 7 | `competencia` | TEXT | Nao | Auto-preenchido | Formato YYYY-MM, maxlength 7 | Derivada de data_inicio.slice(0,7) se nao informada | - | YYYY-MM | Usada como filtro principal |
| 8 | `categoria` | TEXT | Nao | Auto-detectado | `ate_2_dias` ou `3_dias_ou_mais` | Auto-detectado com base em dias (<= 2 ou >= 3) | - | - | Apenas para tipo `atestado` (via `ocorrencias.html`) |
| 9 | `observacoes` | TEXT | Nao | Usuario/Sistema | Texto livre, textarea 3 linhas | - | NULL | - | Truncado a 40 chars na tabela com tooltip completo |
| 10 | `arquivo_nome` | TEXT | Nao | Sistema (importacao) | - | Nome original do arquivo PDF importado | NULL | - | Evidencia de rastreabilidade |
| 11 | `criado_em` | TIMESTAMPTZ | Sim (auto) | Sistema | - | - | `now()` | - | - |
| 12 | `atualizado_em` | TIMESTAMPTZ | Sim (auto) | Sistema | - | - | `now()` | - | Atualizado manualmente no payload do frontend (nao por trigger) |

### 7.2 Tabela `rh_apuracao_mensal`

| # | Campo | Tipo SQL | Obrigatorio | Origem | Validacao | Regra | Valor Padrao | Observacoes |
|---|-------|----------|-------------|--------|-----------|-------|--------------|-------------|
| 1 | `id` | UUID | Sim (auto) | Sistema | PK | - | `gen_random_uuid()` | - |
| 2 | `colaborador_id` | UUID | Sim | Sistema | FK `rh_colaboradores(id)` ON DELETE CASCADE | - | - | UNIQUE com competencia |
| 3 | `competencia` | TEXT | Sim | Sistema | Formato YYYY-MM | Ex: '2026-03' | - | Mes de pagamento |
| 4 | `periodo_inicio` | DATE | Sim | Sistema | - | Dia 27 do mes anterior | - | Calendario 27-26 |
| 5 | `periodo_fim` | DATE | Sim | Sistema | - | Dia 26 do mes corrente | - | Calendario 27-26 |
| 6 | `dias_trabalhados` | INT | Nao | Secullum | - | - | 0 | Importado do ponto |
| 7 | `dias_uteis_periodo` | INT | Nao | Calculado | - | - | 0 | Dias uteis no periodo |
| 8 | `faltas_justificadas` | INT | Nao | Ocorrencias | - | - | 0 | - |
| 9 | `faltas_injustificadas` | INT | Nao | Ocorrencias | - | - | 0 | - |
| 10 | `atrasos_acima_10min` | INT | Nao | Ponto | - | - | 0 | - |
| 11 | `dias_atestado` | INT | Nao | Ocorrencias | - | Soma dos dias de tipo 'atestado' | 0 | - |
| 12 | `saidas_antecipadas` | INT | Nao | Ponto | - | - | 0 | - |
| 13 | `advertencias` | INT | Nao | Ocorrencias | - | Contagem de advertencias | 0 | - |
| 14 | `suspensoes` | INT | Nao | Ocorrencias | - | Contagem de suspensoes | 0 | - |
| 15 | `em_ferias` | BOOLEAN | Nao | Ferias | - | - | FALSE | - |
| 16 | `dias_ferias` | INT | Nao | Ferias | - | - | 0 | - |
| 17 | `afastado_inss` | BOOLEAN | Nao | Ocorrencias | - | Se tipo = afastamento_inss | FALSE | - |
| 18 | `licenca_maternidade` | BOOLEAN | Nao | Ocorrencias | - | Se tipo = licenca_maternidade | FALSE | - |
| 19 | `elegivel_assiduidade` | BOOLEAN | Nao | Calculado | - | RN-06 | FALSE | Resultado da apuracao |
| 20 | `valor_assiduidade` | NUMERIC(10,2) | Nao | Calculado | - | - | 0 | - |
| 21 | `motivo_perda_assiduidade` | TEXT | Nao | Calculado | - | RN-07 | NULL | Descricao textual |
| 22 | `elegivel_va` | BOOLEAN | Nao | Calculado | - | - | TRUE | - |
| 23 | `valor_va` | NUMERIC(10,2) | Nao | Calculado | - | - | 0 | - |
| 24 | `desconto_va` | NUMERIC(10,2) | Nao | Calculado | - | - | 0 | - |
| 25 | `elegivel_deslocamento` | BOOLEAN | Nao | Calculado | - | - | FALSE | - |
| 26 | `valor_deslocamento` | NUMERIC(10,2) | Nao | Calculado | - | - | 0 | - |
| 27 | `motivo_suspensao_desloc` | TEXT | Nao | Calculado | - | - | NULL | - |
| 28 | `plano_alianca_ativo` | BOOLEAN | Nao | Beneficios | - | - | FALSE | - |
| 29 | `total_beneficios` | NUMERIC(10,2) | Nao | Calculado | - | Soma de beneficios | 0 | - |
| 30 | `total_descontos` | NUMERIC(10,2) | Nao | Calculado | - | Soma de descontos | 0 | - |
| 31 | `status` | TEXT | Nao | Sistema | CHECK: `rascunho`, `calculado`, `validado`, `pago` | Fluxo de aprovacao | `rascunho` | - |
| 32 | `calculado_em` | TIMESTAMPTZ | Nao | Sistema | - | Timestamp do calculo | NULL | - |
| 33 | `validado_por` | TEXT | Nao | Sistema | - | Nome do validador | NULL | - |
| 34 | `validado_em` | TIMESTAMPTZ | Nao | Sistema | - | - | NULL | - |

### 7.3 Campos do Formulario HTML (absenteismo.html -- Modal CRUD)

| # | Campo HTML | id | Tipo Input | Obrigatorio (*) | Validacao Frontend | Observacoes |
|---|-----------|-----|-----------|-----------------|-------------------|-------------|
| 1 | Funcionario | `mColaborador` | select | Sim | `!colaborador_id` => alert | Populado com todos os funcionarios |
| 2 | Tipo | `mTipo` | select (6 opcoes) | Sim | `!tipo` => alert | Tipos expandidos |
| 3 | Data Inicio | `mDataInicio` | date | Sim | `!data_inicio` => alert | Dispara autoFillCompetencia + calcDias |
| 4 | Data Fim | `mDataFim` | date | Nao | - | Dispara calcDias |
| 5 | Dias | `mDias` | number (min=1) | Nao | - | Auto-calculado, editavel |
| 6 | Competencia | `mCompetencia` | text | Nao | maxlength=7, placeholder YYYY-MM | Auto-preenchido da data inicio |
| 7 | Observacoes | `mObs` | textarea (3 rows) | Nao | - | - |

### 7.4 Campos de Filtro (absenteismo.html)

| # | Campo | id | Tipo | Acao |
|---|-------|-----|------|------|
| 1 | Buscar funcionario | `fBusca` | text | `oninput="renderTable()"` - filtra por nome (case-insensitive, substring) |
| 2 | Tipo | `fTipo` | select (7 opcoes incl. "Todos") | `onchange="renderTable()"` - filtra por tipo exato |
| 3 | Competencia | `fCompetencia` | month (YYYY-MM) | `onchange="renderTable()"` - filtra por competencia (startsWith) |

---

## 8. Tabelas e Views

### 8.1 Tabelas Utilizadas

| Tabela | Uso | Operacoes |
|--------|-----|-----------|
| `rh_ocorrencias` | Tabela principal de ocorrencias | SELECT, INSERT, UPDATE (PATCH), DELETE |
| `rh_funcionarios` | Dados de funcionarios (nome, setor_id, status, salario_base) | SELECT |
| `rh_colaboradores` | View legada de colaboradores (nome, setor, cargo, cpf, status) | SELECT |
| `rh_setores` | Nomes dos setores | SELECT |
| `rh_apuracao_mensal` | Apuracao de beneficios por competencia | SELECT, INSERT, UPDATE |
| `rh_documento_ocr` | Dados de OCR de atestados (CID, medico, CRM, local) | SELECT (via vw_atestados_analise) |
| `stg_convenia_documentos` | Staging de documentos importados da Convenia | SELECT (via vw_absenteismo_real) |
| `rh_ponto` | Registro diario de ponto | SELECT (via motor_v2) |
| `rh_ponto_totais` | Totais mensais de ponto | SELECT |
| `rh_beneficio` | Beneficios dos funcionarios | SELECT |
| `rh_audit_log` | Log de auditoria | INSERT (via trigger) |

### 8.2 Views Utilizadas

| View | Definicao | Consumida por |
|------|-----------|---------------|
| `vw_absenteismo_real` | Calcula custo real por afastamento (salario_dia, dias_empresa, dias_inss, custo_empresa_real). Fonte: `stg_convenia_documentos` JOIN `rh_funcionarios` JOIN `rh_cargos` JOIN `rh_setores`. Filtra `origem_sistema = 'manual'`. | `vw_absenteismo_recorrencia`, `motor_decisoes()` |
| `vw_absenteismo_por_setor` | Agrega atestados por setor: funcionarios_afetados, total_setor, atestados, total_dias, custo_estimado. | `motor_decisoes()` |
| `vw_absenteismo_recorrencia` | Classifica funcionarios por recorrencia de atestados (RECORRENTE CRITICO, RECORRENTE, AFASTAMENTO PROLONGADO, NORMAL). Depende de `vw_absenteismo_real`. | `motor_decisoes()` |
| `vw_atestados_analise` | UNION ALL de `rh_ocorrencias` (tipo='atestado') com `rh_documento_ocr` (tipo_documento='atestado'). Inclui CID, grupo_cid, medico_crm, local_atendimento. | `relatorio_atestados_dashboard()` |
| `vw_funcionario_base` | View simplificada de funcionarios (id, nome, cargo, setor, status) | Busca global (shell.js) |

---

## 9. Procedures/RPCs

| RPC | Chamada por | Parametros | Retorno | Descricao |
|-----|------------|-----------|---------|-----------|
| `relatorio_atestados_dashboard(p_ano, p_mes)` | `relatorio_atestados.html` | `p_ano: integer (default EXTRACT(year FROM CURRENT_DATE))`, `p_mes: integer (default NULL)` | JSONB com: resumo, por_mes, por_setor, por_cid, por_medico, por_local, por_dia_semana, por_categoria, top_funcionarios, alertas_medico | Dashboard analitico de atestados. Restrito a administrador/rh. |
| `relatorio_absenteismo_gestor(p_ano)` | `relatorio_atestados.html` (perfil gestor) | `p_ano: integer` | JSONB com: resumo, por_mes, comparativo_setores, por_dia_semana, por_gravidade, taxa_setor, taxa_empresa | Visao agregada para gestores (sem dados individuais/saude) |
| `motor_decisoes()` | `motor_decisoes.html` | Nenhum | JSONB com: problemas (recorrencia, afastamento_prolongado, setor_critico, sst_epi), excluidos_por_contexto, resumo | Consome vw_absenteismo_recorrencia e vw_absenteismo_por_setor para gerar planos de acao |
| `gestor_dashboard()` | `gestor.html` | Nenhum | JSONB incluindo `atestados_30d` | Contagem de atestados do setor nos ultimos 30 dias |
| `gestor_confianca_dashboard()` | `painel_gestao.html` | Nenhum | JSONB incluindo equipe com `atestados_6m` por funcionario | Contagem individual de atestados nos ultimos 6 meses |
| `ficha_completa(p_funcionario_id)` | `ficha.html` | `p_funcionario_id: UUID` | JSONB incluindo bloco `atestados` | Total, dias, ultimos 6 meses, ultimos 10 (admin/rh) ou resumo (gestor) |
| `ficha_360_colaborador(p_funcionario_id)` | `ficha_360.html` | `p_funcionario_id: UUID` | JSONB incluindo bloco `ocorrencias` | Total, atestados, dias, advertencias, recentes (admin/rh) ou resumo (gestor) |
| `importar_ocorrencias_lote(p_dados)` | `ingestor_atestados.py` | `p_dados: JSONB (array)` | JSONB `{importados, sem_match}` | Match por nome, INSERT em rh_ocorrencias ON CONFLICT DO NOTHING |
| `calcular_ponto_totais(p_periodo)` | `importar_secullum.html` | `p_periodo: TEXT` | JSONB | Calcula `dias_atestado` em `rh_ponto_totais` via justificativa ILIKE '%atestado%' |
| `file_ingest_atestados(p_registros)` | `importador_local.py` | `p_registros: JSONB` | TABLE(total, inseridos, erros) | Insere em `stg_convenia_documentos` com origem 'manual' |
| `contar_notificacoes()` | `notificacoes.html` | Nenhum | JSONB | Inclui notificacoes geradas por absenteismo critico |

---

## 10. Functions SQL

| Funcao | Tipo | Descricao |
|--------|------|-----------|
| `rh_perfil_atual()` | STABLE, SECURITY DEFINER | Retorna o perfil do usuario autenticado (administrador, rh, gestor, visualizador). Usada nas RPCs para controle de acesso. |
| `rh_funcionario_atual_id()` | STABLE, SECURITY DEFINER | Retorna o ID do funcionario vinculado ao usuario logado. Usada para filtrar dados de subordinados do gestor. |
| `rh_setor_do_gestor()` | STABLE, SECURITY DEFINER | Retorna o setor_id do gestor logado. Usada em `gestor_dashboard()` para filtrar atestados_30d do setor. |
| `fn_set_updated_at()` | TRIGGER | Atualiza campo `updated_at` em BEFORE UPDATE. |
| `fn_audit_trigger()` | TRIGGER, SECURITY DEFINER | Registra INSERT/UPDATE em `rh_audit_log` com dados antigos/novos e campos alterados. |
| `has_perfil(p_perfil)` | - | Verifica se usuario possui determinado perfil (usada nas RLS policies de rh_ocorrencias). |
| `get_setores_lider()` | - | Retorna array de setores do lider (usada na RLS policy `ocorr_select_lider`). |
| `get_meu_colaborador_id()` | - | Retorna ID do colaborador do usuario logado (usada na RLS policy `ocorr_select_proprio`). |

---

## 11. Triggers

| Trigger | Tabela | Evento | Funcao | Descricao |
|---------|--------|--------|--------|-----------|
| `trg_updated_*` | Todas as tabelas | BEFORE UPDATE | `fn_set_updated_at()` | Atualiza campo `updated_at` automaticamente |
| `trg_audit_*` | Todas as tabelas | AFTER INSERT OR UPDATE | `fn_audit_trigger()` | Registra mudancas no `rh_audit_log` |

**Nota**: A tabela `rh_ocorrencias` no schema original (00_schema_supabase.sql) nao possui triggers dedicados de auditoria alem da policy RLS. Os campos `criado_em` e `atualizado_em` sao preenchidos manualmente no payload do frontend (nao via trigger `fn_set_updated_at`).

---

## 12. APIs (Endpoints PostgREST)

### 12.1 absenteismo.html

| # | Metodo | Endpoint PostgREST | Descricao | Contexto |
|---|--------|-------------------|-----------|----------|
| 1 | GET | `rh_ocorrencias?order=data_inicio.desc&limit=2000` | Carrega todas as ocorrencias | `loadAll()` |
| 2 | GET | `rh_funcionarios?select=id,nome_completo,setor_id,status&order=nome_completo` | Lista funcionarios | `loadAll()` |
| 3 | GET | `rh_colaboradores?select=id,nome,setor_id,status&order=nome` | Lista colaboradores (legacy) | `loadAll()` |
| 4 | GET | `rh_setores?select=id,nome&order=nome` | Lista setores | `loadAll()` |
| 5 | POST | `rh_ocorrencias` | Cria nova ocorrencia | `salvar()` (create) |
| 6 | PATCH | `rh_ocorrencias?id=eq.{id}` | Atualiza ocorrencia existente | `salvar()` (update) |
| 7 | GET | `rh_ocorrencias?order=data_inicio.desc&limit=500` | Recarrega apos salvar | `salvar()` (reload) |
| 8 | GET | `rh_colaboradores?select=id,nome,cpf&limit=5000` | Indexa por CPF/nome para Convenia | `btnSyncConvenia` |
| 9 | GET | `rh_ocorrencias?colaborador_id=eq.{id}&data_inicio=eq.{date}&tipo=eq.{tipo}&select=id&limit=1` | Deduplicacao Convenia | `btnSyncConvenia` |
| 10 | POST | `rh_ocorrencias` | Insere ocorrencia da Convenia | `btnSyncConvenia` |
| 11 | FETCH | `https://public-api.convenia.com.br/api/v3/leaves?page={N}&per_page=100` | API externa Convenia | `btnSyncConvenia` |

### 12.2 ocorrencias.html

| # | Metodo | Endpoint PostgREST | Descricao | Contexto |
|---|--------|-------------------|-----------|----------|
| 1 | GET | `rh_colaboradores?select=id,nome,cargo,status&status=eq.ativo&order=nome` | Colaboradores ativos | `carregarColaboradores()` |
| 2 | POST | `rh_ocorrencias` | Salva atestado importado | `salvarAtestados()` |
| 3 | POST | `rh_ocorrencias` | Salva advertencia importada | `salvarAdvertencias()` |
| 4 | POST | `rh_ocorrencias` | Salva lancamento manual | `salvarManual()` |
| 5 | GET | `rh_ocorrencias?select=*,rh_colaboradores(nome,setor)&order=criado_em.desc&limit=30` | Registros recentes com JOIN | `carregarRecentes()` |
| 6 | DELETE | `rh_ocorrencias?id=eq.{id}` | Exclui registro | `deleteOcorrencia()` |
| 7 | GET | `rh_ocorrencias?select=*,rh_colaboradores(nome,setor)&competencia=eq.{comp}` | Ocorrencias do mes para resumo | `gerarResumo()` |
| 8 | GET | `rh_colaboradores?select=id,nome,setor,status&status=eq.ativo&order=nome` | Todos os colaboradores ativos | `gerarResumo()` |

### 12.3 relatorio_atestados.html

| # | Metodo | Endpoint PostgREST | Descricao | Contexto |
|---|--------|-------------------|-----------|----------|
| 1 | POST | `rpc/relatorio_atestados_dashboard` | RPC com `{p_ano, p_mes}` | `callRPC()` (admin/rh) |
| 2 | POST | `rpc/relatorio_absenteismo_gestor` | RPC com `{p_ano}` | `callRPC()` (gestor) |

---

## 13. Integracoes

### 13.1 Convenia (REST API)

| Aspecto | Detalhe |
|---------|--------|
| **URL base** | `https://public-api.convenia.com.br/api/v3` |
| **Endpoint** | `GET /leaves?page={N}&per_page=100` |
| **Autenticacao** | Header `api-token: {token}` |
| **Frequencia** | Manual (botao "Sincronizar Convenia") |
| **Dados importados** | start_date, end_date, leave_type.name, employee.cpf, employee.name, reason |
| **Configuracao** | Token em `localStorage['convenia_config']` |
| **Pagina config** | `convenia_integracao.html` |
| **Status integr.** | Funcional 85% |

### 13.2 Secullum (File Upload)

| Aspecto | Detalhe |
|---------|--------|
| **Tipo** | Upload de planilha XLSX |
| **Dados relevantes** | Dias de falta, atestado, ferias (campo `justificativa` no `rh_ponto`) |
| **Fluxo** | `importar_secullum.html` → `rh_ponto` → `calcular_ponto_totais()` → `rh_ponto_totais.dias_atestado` |
| **Impacto** | Ponto alimenta o Motor Score v2 (score_presenca) e apuracao mensal |

### 13.3 OCR de Documentos

| Aspecto | Detalhe |
|---------|--------|
| **Tabela** | `rh_documento_ocr` |
| **Dados extraidos** | CID, medico, CRM, local_atendimento, dias_afastamento, gravidade |
| **Consumido por** | `vw_atestados_analise` (UNION com `rh_ocorrencias`) |
| **Volume** | ~300 documentos processados por OCR + ~4.690 nomes de arquivo |

---

## 14. Dashboards

### 14.1 KPIs -- absenteismo.html

| KPI | id HTML | Calculo | Icone | Cor |
|-----|---------|---------|-------|-----|
| Faltas no Mes | `kFaltas` | `ocorrencias.filter(tipo.startsWith('falta')).length` | Alerta (circulo com !) | Vermelho |
| Atestados no Mes | `kAtestados` | `ocorrencias.filter(tipo === 'atestado').length` | Documento | Azul |
| % Absenteismo | `kAbsenteismo` | `(totalDiasMes / (ativos * 22)) * 100` | Pulso | Amber |
| Setor Critico | `kSetorCritico` | Setor com mais dias perdidos no mes | Pessoas | Roxo |

### 14.2 KPIs -- ocorrencias.html (Tab Resumo Mensal)

| KPI | id HTML | Calculo |
|-----|---------|---------|
| Atestados (dias) | `kpi-total-atestados` | `totalDiasAtestado` (soma) |
| Advertencias | `kpi-total-adv` | `totalAdv` (contagem) |
| Colaboradores com Ocorrencias | `kpi-colaboradores` | `totalComOcorrencias` (distintos) |
| Perdem Assiduidade | `kpi-perde` | `(totalPerde / todosColabs.length) * 100` (%) |

### 14.3 KPIs -- relatorio_atestados.html

| KPI | id HTML | Calculo |
|-----|---------|---------|
| Total Atestados | `kpi-total` | `resumo.total_atestados` |
| Dias Perdidos | `kpi-dias` | `resumo.total_dias` |
| Funcionarios Afetados | `kpi-funcs` | `resumo.total_funcionarios` (DISTINCT) |
| Media Dias / Atestado | `kpi-media` | `total_dias / total_atestados` |

### 14.4 Graficos -- relatorio_atestados.html

| Grafico | Tipo | Dados |
|---------|------|-------|
| Evolucao Mensal | Barras horizontais (CSS) | Atestados e dias por mes |
| Por Setor | Tabela com barra inline | Atestados, dias, funcionarios, media/func por setor |
| Por CID | Tabela agrupada | Grupo CID > CID > atestados + dias |
| Por Medico Emissor | Tabela com badges | CRM, atestados, dias, funcionarios, media, status (Normal/Investigar) |
| Por Local | Barras horizontais | Atestados e dias por local de atendimento |
| Top Funcionarios | Tabela rankeada | Ranking com medalhas (1o, 2o, 3o) |
| Atestados por Dia da Semana | Barras horizontais | Segunda a Sabado, com destaque em Seg/Sex |
| Alertas de Padroes Suspeitos | Cards de alerta | CRM, local, total atestados, funcionarios, media dias |

---

## 15. Relatorios

### 15.1 Relatorio de Atestados e Absenteismo (relatorio_atestados.html)

| Aspecto | Detalhe |
|---------|--------|
| **Titulo** | "Relatorio de Atestados e Absenteismo" |
| **Subtitulo** | "Analise por medico, CID, hospital e setor" |
| **Filtros** | Ano (select: 2026, 2025), Mes (select: Janeiro-Dezembro ou Todos) |
| **RPC** | `relatorio_atestados_dashboard(p_ano, p_mes)` |
| **Secoes** | 7: Evolucao Mensal, Por Setor, Por CID, Por Medico, Por Local, Top Funcionarios, Alertas |
| **Impressao** | Botao "Imprimir" (window.print()) com CSS de impressao |
| **LGPD** | Footer obrigatorio: "Este relatorio contem dados de saude protegidos pela LGPD. Uso restrito a RH/Admin." |

### 15.2 Resumo Mensal por Colaborador (ocorrencias.html, tab "Resumo Mensal")

| Aspecto | Detalhe |
|---------|--------|
| **Filtro** | Competencia (month input) |
| **Colunas** | Colaborador, Setor, Atestados (dias), Advertencias (qtd), Suspensoes (dias), Impacto Assiduidade |
| **Ordenacao** | Quem perde primeiro, depois quem tem ocorrencia, depois por nome |
| **Destaque visual** | Linhas sem ocorrencia com `opacity: 0.5` |

---

## 16. Documentos

### 16.1 Impressao

- `relatorio_atestados.html` possui botao "Imprimir" (`window.print()`) com CSS `@media print` para layout de impressao.

### 16.2 Exportacao

- **Nao implementado no legado**. Nao ha exportacao para Excel, CSV ou PDF nas paginas de absenteismo/ocorrencias.

### 16.3 Arquivos Importados

- Atestados e advertencias sao importados via drag-and-drop ou colagem de nomes de arquivo. Apenas os **nomes dos arquivos** sao lidos (nao o conteudo PDF). O nome original e armazenado em `rh_ocorrencias.arquivo_nome`.

---

## 17. Evidencias

### 17.1 Atestados Digitalizados

- O campo `arquivo_nome` em `rh_ocorrencias` armazena o nome original do arquivo PDF do atestado.
- Os arquivos fisicos ficam na pasta de rede do RH (nao no banco de dados).
- Exemplos de nomes: `JESSICA CARDOZO 19022026 a 21022026.pdf`, `MARCOS SILVA 05032026.pdf`

### 17.2 Dados de OCR

- A tabela `rh_documento_ocr` armazena texto extraido de atestados, incluindo:
  - `cid` (codigo CID-10)
  - `medico` (nome do medico)
  - `crm` (registro no conselho)
  - `texto_extraido` (texto completo do OCR)
  - `confianca_ocr` (NUMERIC 0-100, percentual de confianca)
  - `gravidade` (classificacao)

### 17.3 Historico de Importacao

- `RHAuth.log('importar_atestados', { quantidade })` registra eventos de importacao em lote.
- `RHAuth.log('importar_advertencias', { quantidade })` registra importacao de advertencias.
- `RHAuth.log('registrar_ocorrencia_manual', { tipo, colaborador_id })` registra lancamentos manuais.

---

## 18. Permissoes

### 18.1 Matriz Perfil x Acao

| Acao | administrador | rh | gestor | gestor_confianca | visualizador | sst |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| Visualizar pagina absenteismo.html | X | X | - | - | - | - |
| Visualizar pagina ocorrencias.html | X | X | - | - | - | - |
| Visualizar pagina relatorio_atestados.html | X | X | X (agregado) | - | - | - |
| Criar ocorrencia | X | X | - | - | - | - |
| Editar ocorrencia | X | X | - | - | - | - |
| Excluir ocorrencia | X | - | - | - | - | - |
| Importar lote atestados | X | X | - | - | - | - |
| Importar lote advertencias | X | X | - | - | - | - |
| Sincronizar Convenia | X | X | - | - | - | - |
| Ver KPIs gerais | X | X | - | - | - | - |
| Ver dados de saude (CID) | X | X | - | - | - | - |
| Ver medico emissor | X | X | - | - | - | - |
| Ver atestados individuais | X | X | - | - | - | - |
| Ver contagem atestados do setor | X | X | X | X | - | - |
| Ver contagem atestados por func (6m) | X | X | - | X | - | - |
| Gerar resumo mensal | X | X | - | - | - | - |
| Ver custos de absenteismo | X | X | - | - | - | - |
| Ler rh_ocorrencias (RLS) | X | X | X (setor) | X (setor) | - | X |
| Inserir rh_ocorrencias (RLS) | X | X | - | - | - | - |
| Atualizar rh_ocorrencias (RLS) | X | X | - | - | - | - |
| Deletar rh_ocorrencias (RLS) | X | - | - | - | - | - |

### 18.2 Permissoes da API Layer (api.js)

| Recurso | Perfis Autorizados |
|---------|-------------------|
| `atestado_individual` | administrador, rh |
| `cid` | administrador, rh |
| `dados_sensiveis` | administrador, rh |
| `ver_motor_decisao` | administrador, rh |

---

## 19. Auditoria

### 19.1 O que Deve Ficar Registrado

| Evento | Onde e Registrado | Detalhes |
|--------|-------------------|----------|
| Criacao de ocorrencia | `rh_audit_log` (via trigger `fn_audit_trigger`) | dados_novos em JSONB, usuario_id |
| Edicao de ocorrencia | `rh_audit_log` (via trigger) | dados_antigos, dados_novos, campos_alterados |
| Exclusao de ocorrencia | RLS policy bloqueia para nao-admin; se admin, DELETE e registrado | - |
| Importacao em lote (atestados) | `RHAuth.log('importar_atestados')` | quantidade importada |
| Importacao em lote (advertencias) | `RHAuth.log('importar_advertencias')` | quantidade importada |
| Lancamento manual | `RHAuth.log('registrar_ocorrencia_manual')` | tipo, colaborador_id |
| Sincronizacao Convenia | Nao registrado em auditoria formal (apenas log no console) | Status exibido em `syncStatus` div |
| Acesso ao relatorio de atestados | Nao registrado | - |
| Sessao expirada | `API.logEvent('SESSION_EXPIRED')` | Array em memoria, nao persistido |
| Erro de API | `API.logEvent('API_ERROR')` | Array em memoria, nao persistido |

### 19.2 Campos de Auditoria por Registro

- `rh_ocorrencias.criado_em`: Timestamp da criacao
- `rh_ocorrencias.atualizado_em`: Timestamp da ultima atualizacao (preenchido no payload, nao via trigger)
- `rh_audit_log.usuario_id`: UUID do usuario que realizou a operacao (via `auth.uid()`)
- `rh_audit_log.campos_alterados`: Array TEXT[] dos campos modificados (exclui updated_at)

---

## 20. Eventos

### 20.1 Eventos Gerados

| Evento | Gerador | Consumidor |
|--------|---------|-----------|
| INSERT em `rh_ocorrencias` | Formulario CRUD, importacao lote, Convenia | Trigger `fn_audit_trigger` → `rh_audit_log` |
| UPDATE em `rh_ocorrencias` | Edicao via modal | Trigger `fn_audit_trigger` → `rh_audit_log` |
| `importar_atestados` | `ocorrencias.html` (aba Atestados) | `RHAuth.log()` |
| `importar_advertencias` | `ocorrencias.html` (aba Advertencias) | `RHAuth.log()` |
| `registrar_ocorrencia_manual` | `ocorrencias.html` (aba Lancamento Manual) | `RHAuth.log()` |
| Notificacao de absenteismo critico | `motor_decisoes()` → `motor_v2_gerar_alertas()` | `rh_notificacao`, `motor_alerta` |

### 20.2 Eventos Consumidos

| Evento | Origem | Como Consome |
|--------|--------|-------------|
| Dados de ponto importados | `importar_secullum.html` | `rh_ponto.justificativa` identifica faltas/atestados |
| Afastamentos Convenia | API Convenia | Botao "Sincronizar Convenia" |
| Documentos OCR processados | Pipeline Python | `rh_documento_ocr` alimenta `vw_atestados_analise` |

---

## 21. Dependencias

### 21.1 Pre-requisitos para Funcionamento

| Dependencia | Modulo | Status Necessario |
|-------------|--------|-------------------|
| `rh_funcionarios` populada | Cadastro | Pelo menos 1 funcionario ativo |
| `rh_colaboradores` view/tabela | Cadastro | Necessaria para `ocorrencias.html` (rh_colaboradores com FK) |
| `rh_setores` populada | Cadastro | Para calculo de setor critico |
| `rh_ocorrencias` tabela criada | Banco | CHECK constraint deve incluir tipos expandidos |
| Supabase Auth configurado | Infra | JWT token para API |
| `rh_usuarios` com perfil | RBAC | Para controle de acesso |
| Token Convenia configurado | Integracao | Para sincronizacao (opcional) |
| Ponto importado (Secullum) | Ponto | Para apuracao completa (opcional) |
| OCR processado | Pipeline | Para relatorio por CID/medico (opcional) |
| `stg_convenia_documentos` | ETL | Para views vw_absenteismo_* (necessario para motor_decisoes) |

### 21.2 Ordem de Carga

1. FUNDACAO_BANCO_v2.sql (rh_empresas, rh_filiais, rh_setores, rh_cargos, rh_funcionarios)
2. 00_schema_supabase.sql (rh_colaboradores, rh_apuracao_mensal, rh_ocorrencias)
3. TABELAS_RH_CORE.sql (rh_documento_ocr, rh_ponto, rh_ponto_totais, rh_notificacao)
4. VIEWS_APLICACAO.sql (vw_absenteismo_real, vw_absenteismo_por_setor, vw_atestados_analise, vw_absenteismo_recorrencia)
5. FUNCOES_APLICACAO.sql (relatorio_atestados_dashboard, motor_decisoes, importar_ocorrencias_lote)
6. SEGURANCA_SUPABASE.sql (RLS policies para rh_ocorrencias)

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| **Motor Score v2** (`motor_v2.html`) | Inteligencia | `score_presenca` usa dados de ponto que refletem faltas/atestados |
| **Motor de Decisao** (`motor_decisoes.html`) | Gestao | Consome vw_absenteismo_recorrencia e vw_absenteismo_por_setor para gerar problemas e planos de acao automaticos |
| **Dashboard Gerencial** (`dashboard_gerencial.html`) | Inteligencia | Exibe taxa de absenteismo como KPI executivo |
| **Beneficios** (`beneficios.html`) | Operacional | Assiduidade (VA, deslocamento) depende do resultado da apuracao mensal |
| **Ficha 360** (`ficha_360.html`) | Pessoas | Exibe historico de ocorrencias do funcionario (bloco B6) |
| **Ficha Completa** (`ficha.html`) | Pessoas | Exibe resumo de atestados (total, dias, ultimos 6 meses) |
| **Alertas/Pendencias** (`alertas_rh.html`) | Gestao | Motor de decisao gera alertas de absenteismo critico |
| **Ponto** (`importar_secullum.html`) | Operacional | Ponto identifica dias com justificativa "atestado" via calcular_ponto_totais() |
| **ASOs** (`asos.html`) | SST | Afastamentos prolongados geram obrigacao de ASO de retorno |
| **Restricoes Medicas** (`restricoes_medicas.html`) | SST | CIDs recorrentes podem gerar restricoes medicas |
| **Saude Ocupacional** (`saude_mental.html`) | SST | CIDs do grupo F (saude mental) alimentam o modulo de saude ocupacional |
| **Desligamentos** (`desligamentos_turnover.html`) | Pessoas | Absenteismo recorrente e fator de analise de turnover |
| **Dashboard do Gestor** (`gestor.html`) | Gestao | Exibe contagem de atestados do setor nos ultimos 30 dias |

---

## 23. Melhorias para o SIGA

### MP-01: Workflow de Aprovacao de Atestados
No legado, atestados sao inseridos diretamente sem aprovacao. No SIGA, implementar fluxo:
1. Funcionario/RH registra atestado com status `pendente`
2. Medico do trabalho ou RH valida (status `validado`) ou rejeita (status `rejeitado`)
3. Atestado validado alimenta automaticamente a apuracao mensal
4. Historico de transicoes de status com `rh_processo_transicao`

### MP-02: Upload Real de Documentos
No legado, apenas o nome do arquivo e armazenado. No SIGA:
1. Upload do PDF do atestado para Supabase Storage
2. Vinculo com `rh_ocorrencias` via URL publica ou signed URL
3. OCR automatico no upload (via Edge Function ou pipeline)
4. Extracao automatica de CID, medico, CRM

### MP-03: Notificacoes Automaticas por Threshold
Implementar notificacoes automaticas quando:
- Funcionario atinge 3+ atestados no trimestre
- Setor ultrapassa 5% de absenteismo
- Medico emissor atinge threshold de investigacao
- Funcionario completa 15o dia de afastamento (limite empresa, inicio INSS)

### MP-04: Integracao Bidirecional com eSocial
Eventos S-2230 (afastamento temporario) devem ser gerados automaticamente a partir dos registros de afastamento:
- Tipo de afastamento mapeado para tabela eSocial (01 = doenca, 03 = acidente, etc.)
- CID obrigatorio para afastamentos > 15 dias
- Retorno automaticamente gera evento S-2230 com dtTermAfast

### MP-05: Calculo de Custo Real no Backend
No legado, o custo e calculado apenas na view SQL. No SIGA:
- Considerar encargos reais (FGTS, 13o proporcional, ferias proporcionais sobre periodo afastado)
- Custo de substituicao (hora extra de colegas)
- Custo de producao perdida (integrar com dados de producao)

### MP-06: Dashboard com Graficos Reais (Chart.js/D3)
No legado, os graficos sao barras CSS simples. No SIGA:
- Graficos de linha para tendencia de absenteismo mensal
- Heatmap de dias da semana vs. setor
- Grafico de pizza para distribuicao por tipo de ocorrencia
- Comparativo ano-a-ano

### MP-07: API de Validacao de CID
Integrar com tabela de CID-10 completa para:
- Autocomplete de CID no formulario
- Validacao de codigo CID
- Enriquecimento automatico do grupo/descricao

### MP-08: Prontuario Medico Ocupacional
Consolidar historico de saude do funcionario:
- Atestados + ASOs + Acidentes + Restricoes em timeline unificada
- Correlacao de CIDs ao longo do tempo
- Alertas de agravamento (ex: mesmo CID repetido 3x em 6 meses)

---

## 24. Adequacao ao SIGA

### 24.1 Multiempresa
- No legado, o sistema opera com empresa unica (Classic Couros). O SIGA deve suportar multiempresa com `empresa_id` em `rh_ocorrencias`.
- Views e KPIs devem filtrar por empresa do usuario logado.
- Relatorios devem ter filtro de empresa.

### 24.2 RBAC
- Substituir RLS policies baseadas em `has_perfil()` por sistema RBAC nativo do SIGA.
- Implementar permissoes granulares: `absenteismo.visualizar`, `absenteismo.criar`, `absenteismo.editar`, `absenteismo.excluir`, `absenteismo.importar`, `absenteismo.aprovar`, `absenteismo.relatorio`, `absenteismo.dados_saude`.
- Gestores devem ver apenas dados de seus subordinados (diretos ou hierarquicos).

### 24.3 Workflow
- Substituir INSERT direto por workflow de processos com etapas (registro → validacao → aprovacao → efetivacao).
- Usar tabela de processos (`rh_processo`) com transicoes registradas.
- Status: `rascunho` → `pendente_validacao` → `validado` → `efetivado` ou `rascunho` → `pendente_validacao` → `rejeitado`.

### 24.4 Auditoria
- Substituir `criado_em`/`atualizado_em` manuais por triggers automaticos (`fn_set_updated_at`).
- Garantir que TODA operacao (INSERT, UPDATE, DELETE) gere registro em `rh_audit_log`.
- Adicionar campo `created_by` (UUID, FK auth.users) em `rh_ocorrencias`.
- Registrar eventos de importacao em lote na tabela de auditoria (nao apenas em localStorage de eventos do frontend).

### 24.5 Event Bus
- Publicar eventos de dominio quando ocorrencia e criada/alterada:
  - `absenteismo.ocorrencia_criada` → Consumidores: Motor Score, Apuracao Mensal, Notificacoes
  - `absenteismo.ocorrencia_atualizada` → Consumidores: Apuracao Mensal
  - `absenteismo.importacao_concluida` → Consumidores: Recalculo de KPIs, Notificacao de resumo
  - `absenteismo.threshold_atingido` → Consumidores: Alertas, Planos de Acao

### 24.6 APIs
- Substituir chamadas PostgREST diretas por API REST padronizada do SIGA.
- Endpoints sugeridos:
  - `GET /api/v1/rh/ocorrencias` (listagem com paginacao, filtros)
  - `POST /api/v1/rh/ocorrencias` (criacao)
  - `PUT /api/v1/rh/ocorrencias/{id}` (atualizacao)
  - `DELETE /api/v1/rh/ocorrencias/{id}` (exclusao logica)
  - `POST /api/v1/rh/ocorrencias/importar` (importacao em lote)
  - `POST /api/v1/rh/ocorrencias/sync-convenia` (sincronizacao)
  - `GET /api/v1/rh/absenteismo/dashboard` (KPIs)
  - `GET /api/v1/rh/absenteismo/relatorio` (relatorio analitico)

### 24.7 LGPD (Dados Medicos)

**CRITICO**: Dados de saude sao dados sensiveis sob a LGPD (Art. 5, II e Art. 11).

| Dado | Classificacao LGPD | Tratamento Necessario |
|------|-------------------|-----------------------|
| CID (codigo diagnostico) | Dado sensivel de saude | Acesso restrito a RH/Admin; criptografia em repouso; log de acesso |
| Nome do medico / CRM | Dado pessoal | Acesso restrito |
| Dias de atestado | Dado pessoal | Acesso restrito para individuais |
| Observacoes de atestado | Potencialmente sensivel | Acesso restrito |
| Motivo de afastamento | Dado sensivel | Acesso restrito |

Requisitos LGPD:
1. **Base legal**: Cumprimento de obrigacao legal (Art. 11, II, a) — legislacao trabalhista
2. **Consentimento**: Nao necessario quando base legal e obrigacao legal
3. **Minimizacao**: Coletar apenas dados estritamente necessarios
4. **Retencao**: Definir prazo de retencao (sugestao: 20 anos para dados trabalhistas/previdenciarios)
5. **Anonimizacao**: Relatórios agregados nao devem permitir identificacao individual
6. **Log de acesso**: Registrar quem acessou dados de saude individuais
7. **Direito ao esquecimento**: Nao aplicavel (obrigacao legal)
8. **Encarregado (DPO)**: Notificar quando dados de saude sao acessados fora do RH

### 24.8 Rastreabilidade
- Cada ocorrencia deve ter ID sequencial legivel (ex: `OC-2026-001234`) alem do UUID.
- Historico completo de transicoes de status.
- Vinculo com processo formal (processo_id → rh_processo).
- Vinculo com evidencia digital (documento_id → storage).

---

## 25. Criterios de Aceite

### Registro de Ocorrencias

**CA-01**: O sistema deve permitir registrar uma nova ocorrencia informando funcionario (obrigatorio), tipo (obrigatorio), data inicio (obrigatorio), data fim (opcional), dias (auto-calculado ou manual), competencia (auto-derivada), observacoes (opcional).

**CA-02**: Ao informar data inicio e data fim, o campo "dias" deve ser automaticamente calculado como `(data_fim - data_inicio) + 1` (inclusivo).

**CA-03**: Ao informar a data inicio, o campo "competencia" deve ser automaticamente preenchido com YYYY-MM.

**CA-04**: O sistema deve validar campos obrigatorios (funcionario, tipo, data inicio) e exibir mensagem de erro se algum estiver vazio.

**CA-05**: Apos salvar com sucesso, o modal deve fechar, a tabela deve ser recarregada e os KPIs recalculados.

**CA-06**: O sistema deve permitir editar uma ocorrencia existente clicando no icone de edicao, preenchendo o modal com os dados atuais.

### Importacao em Lote

**CA-07**: O sistema deve aceitar drag-and-drop de arquivos PDF ou colagem de nomes de arquivo (um por linha) na area de importacao.

**CA-08**: O parser deve extrair nome do funcionario e datas do nome do arquivo nos formatos: `NOME DDMMAAAA.pdf` e `NOME DDMMAAAA a DDMMAAAA.pdf`.

**CA-09**: O sistema deve realizar matching fuzzy do nome extraido com a base de colaboradores, exibindo o nivel de confianca (exato, contem, parcial, nome_sobrenome, primeiro_nome).

**CA-10**: Para registros sem match automatico, o sistema deve exibir um dropdown para selecao manual do colaborador.

**CA-11**: O botao "Salvar no banco" deve inserir apenas registros validos (com match) e exibir contagem de salvos/erros.

**CA-12**: Apos importacao bem-sucedida, o sistema deve registrar evento de auditoria com quantidade importada.

### Sincronizacao Convenia

**CA-13**: Se o token Convenia nao estiver configurado, o sistema deve exibir confirmacao e redirecionar para a pagina de configuracao.

**CA-14**: A sincronizacao deve paginar a API Convenia (ate 30 paginas, 100/pagina) e exibir progresso.

**CA-15**: O sistema deve deduplicar registros verificando (colaborador_id + data_inicio + tipo) antes de inserir.

**CA-16**: Apos sincronizacao, o sistema deve exibir resumo: inseridos, ja existentes, sem funcionario, erros.

### KPIs e Dashboard

**CA-17**: O KPI "% Absenteismo" deve ser calculado como `(totalDiasMes / (ativos * 22)) * 100` e exibido com 1 casa decimal.

**CA-18**: O KPI "Setor Critico" deve exibir o nome do setor com maior soma de dias perdidos no mes.

**CA-19**: Os KPIs devem ser recalculados automaticamente apos qualquer operacao de CRUD ou sincronizacao.

### Filtros e Ordenacao

**CA-20**: O filtro por nome deve ser case-insensitive e buscar por substring.

**CA-21**: O filtro por tipo deve filtrar por valor exato do campo tipo.

**CA-22**: O filtro por competencia deve filtrar registros cuja competencia comeca com o valor selecionado (YYYY-MM).

**CA-23**: As colunas Funcionario, Tipo, Data Inicio e Dias devem ser ordenáveis com click no cabeçalho, alternando entre ASC e DESC.

### Resumo Mensal

**CA-24**: O resumo mensal deve listar TODOS os colaboradores ativos, com flag "PERDE" para quem excedeu os limites de assiduidade.

**CA-25**: A regra de perda de assiduidade deve ser: `dias_atestado > 3 OR qtd_advertencias > 0 OR dias_suspensao > 0`.

**CA-26**: O KPI "Perdem Assiduidade" deve exibir o percentual de colaboradores que perdem em relacao ao total de ativos.

### Relatorio de Atestados

**CA-27**: O relatorio deve exibir dados filtrados por ano e opcionalmente por mes.

**CA-28**: A secao "Por Medico" deve classificar como "Investigar" medicos com 5+ atestados ou media >= 10 dias.

**CA-29**: O relatorio deve exibir alertas de padrao suspeito para CRMs com 3+ atestados em 2 anos.

**CA-30**: Para perfil gestor, as secoes de CID, medico, funcionarios individuais e alertas devem ser ocultadas.

**CA-31**: O footer LGPD deve ser exibido em todas as visoes do relatorio.

### Seguranca

**CA-32**: Apenas perfis administrador e rh devem ter acesso as paginas absenteismo.html e ocorrencias.html.

**CA-33**: Perfil gestor no relatorio de atestados deve ver apenas dados agregados, sem nomes de funcionarios em dados de saude.

**CA-34**: DELETE em rh_ocorrencias deve ser restrito ao perfil administrador (RLS policy).

---

## 26. Casos de Teste

### Fluxo Feliz

**CT-01**: Criar ocorrencia tipo "atestado" com todos os campos preenchidos.
- Pre-condicao: Usuario logado como RH, pelo menos 1 funcionario ativo.
- Passos: Abrir absenteismo.html → Click "+ Nova Ocorrencia" → Selecionar funcionario → Selecionar tipo "Atestado" → Informar data inicio → Informar data fim → Verificar dias auto-calculado → Verificar competencia auto-preenchida → Click "Salvar".
- Resultado esperado: Modal fecha, registro aparece na tabela, KPIs atualizados.

**CT-02**: Editar ocorrencia existente alterando o tipo.
- Pre-condicao: Pelo menos 1 ocorrencia existente.
- Passos: Click no icone de edicao → Alterar tipo de "atestado" para "falta_justificada" → Click "Salvar".
- Resultado esperado: Registro atualizado na tabela com novo tipo.

**CT-03**: Importar atestados via drag-and-drop de PDFs.
- Pre-condicao: Arquivos com nomes no formato `NOME DDMMAAAA.pdf`.
- Passos: Abrir ocorrencias.html → Tab "Atestados" → Arrastar 5 PDFs → Click "Importar" → Verificar preview com matches → Click "Salvar no banco".
- Resultado esperado: 5 registros criados (ou menos se duplicatas), toast de sucesso.

**CT-04**: Importar atestados via colagem de nomes.
- Pre-condicao: Lista de nomes de arquivo.
- Passos: Tab "Atestados" → Colar nomes no textarea → Click "Importar" → Verificar preview → Selecionar manualmente colaboradores sem match → Click "Salvar no banco".
- Resultado esperado: Registros criados para todos os matches (automaticos e manuais).

**CT-05**: Gerar resumo mensal.
- Pre-condicao: Ocorrencias registradas para a competencia selecionada.
- Passos: Tab "Resumo Mensal" → Selecionar competencia → Click "Gerar Resumo".
- Resultado esperado: KPIs exibidos, tabela com todos os colaboradores, flags "PERDE"/"OK" corretos.

**CT-06**: Sincronizar com Convenia.
- Pre-condicao: Token Convenia configurado, afastamentos na API.
- Passos: Click "Sincronizar Convenia" → Aguardar processamento.
- Resultado esperado: Registros inseridos, duplicatas ignoradas, status exibido.

**CT-07**: Visualizar relatorio de atestados com filtro de mes.
- Pre-condicao: Dados de atestados no ano corrente.
- Passos: Abrir relatorio_atestados.html → Selecionar ano → Selecionar mes → Click "Aplicar".
- Resultado esperado: Todas as secoes carregam com dados filtrados.

### Fluxos Alternativos

**CT-08**: Criar ocorrencia sem data fim.
- Passos: Preencher apenas funcionario, tipo e data inicio → Salvar.
- Resultado esperado: Registro criado com dias=1, data_fim=NULL.

**CT-09**: Importar arquivo com nome nao reconhecido.
- Passos: Colar nome sem data (ex: "documento.pdf") → Importar.
- Resultado esperado: Registro aparece na preview com erro "Formato nao reconhecido", opacidade reduzida, nao e salvo.

**CT-10**: Sincronizar Convenia sem token.
- Passos: Limpar localStorage → Click "Sincronizar Convenia".
- Resultado esperado: Dialog de confirmacao → Redirecionamento para convenia_integracao.html.

**CT-11**: Sincronizar Convenia com funcionarios sem match.
- Passos: API retorna afastamentos de funcionarios nao cadastrados.
- Resultado esperado: `semFunc++`, registro nao criado, contagem exibida no status.

### Fluxos de Erro

**CT-12**: Tentar salvar ocorrencia sem campos obrigatorios.
- Passos: Abrir modal → Deixar campos vazios → Click "Salvar".
- Resultado esperado: Alert "Preencha os campos obrigatorios: Funcionario, Tipo e Data Inicio."

**CT-13**: Erro de rede ao salvar ocorrencia.
- Passos: Desconectar rede → Tentar salvar.
- Resultado esperado: Alert "Erro ao salvar ocorrencia.", botao reabilitado.

**CT-14**: Erro de API ao salvar (ex: constraint violation).
- Passos: Tentar inserir com tipo invalido na constraint CHECK.
- Resultado esperado: Alert "Erro ao salvar: Erro 400".

**CT-15**: Sessao expirada durante operacao.
- Passos: Token JWT expirado → Qualquer operacao de API.
- Resultado esperado: Redirecionamento para login.html.

### Permissoes

**CT-16**: Gestor tenta acessar absenteismo.html.
- Passos: Logar como gestor → Navegar para absenteismo.html.
- Resultado esperado: Pagina nao aparece no menu (shell.js filtra por perfis). Se acessar direto pela URL, RHAuth.init restringe acesso.

**CT-17**: Gestor acessa relatorio_atestados.html.
- Passos: Logar como gestor → Abrir relatorio_atestados.html.
- Resultado esperado: Secoes de CID, medico, funcionarios individuais e alertas ficam ocultas. Banner "Visao do Gestor" aparece.

**CT-18**: Visualizador tenta acessar ocorrencias.html.
- Passos: Logar como visualizador → Tentar navegar para ocorrencias.html.
- Resultado esperado: Pagina nao listada no menu. Acesso direto pela URL e bloqueado.

### Concorrencia

**CT-19**: Dois usuarios editam a mesma ocorrencia simultaneamente.
- Passos: Usuario A abre modal da ocorrencia X → Usuario B abre modal da mesma ocorrencia X → Ambos alteram e salvam.
- Resultado esperado: Ambos os PATCHs sao executados (ultimo ganha - no legado nao ha controle de concorrencia). O `atualizado_em` registra o timestamp do ultimo.

**CT-20**: Importacao em lote enquanto outro usuario sincroniza Convenia.
- Passos: Usuario A importa atestados por arquivo → Simultaneamente, Usuario B sincroniza Convenia.
- Resultado esperado: Ambas operacoes inserem registros. A deduplicacao Convenia (check por colaborador_id + data_inicio + tipo) evita duplicatas entre os dois canais.

---

## 27. Casos Extremos

**CE-01**: Funcionario com 0 ocorrencias em todos os meses.
- Comportamento esperado: Aparece na tabela do resumo mensal com todos os valores zerados e flag "OK".
- KPI de absenteismo: 0%.

**CE-02**: Atestado que cruza virada de mes (ex: 28/01 a 03/02).
- Comportamento esperado: No legado, a competencia e derivada da data inicio (janeiro). Os dias incluem ambos os meses. Nao ha rateio de dias entre competencias.
- Impacto: O resumo mensal de janeiro contabiliza todos os 7 dias, fevereiro contabiliza 0 (se a competencia do registro e janeiro).

**CE-03**: Afastamento INSS > 365 dias.
- Comportamento esperado: vw_absenteismo_real calcula `dias_empresa = 14`, `dias_inss = dias - 14`. O custo da empresa e limitado a 14 dias.
- Classificacao: `longo_inss` na faixa, `AFASTAMENTO PROLONGADO` na recorrencia.

**CE-04**: Funcionario desligado com atestados pendentes.
- Comportamento esperado: O registro de ocorrencia permanece (FK com ON DELETE CASCADE em rh_colaboradores, mas nao em rh_funcionarios). O funcionario nao aparece nos KPIs (filtro `status = 'ativo'`).

**CE-05**: Importacao de 1.000+ atestados de uma vez.
- Comportamento esperado: O loop sequencial (await API.post para cada) pode levar varios minutos. Nao ha barra de progresso ou batching no legado.

**CE-06**: Dois atestados sobrepostos para o mesmo funcionario (ex: 01/01-05/01 e 03/01-07/01).
- Comportamento esperado: No legado, ambos sao aceitos sem validacao de sobreposicao. Os dias sao somados (5 + 5 = 10), embora os dias reais perdidos sejam 7.

**CE-07**: Competencia futura (ex: 2027-01 registrado em 2026-12).
- Comportamento esperado: Aceito sem validacao. O campo competencia e texto livre sem restricao temporal.

**CE-08**: Nome de arquivo PDF com caracteres especiais ou acentos.
- Comportamento esperado: A funcao `normalize()` remove acentos e caracteres especiais antes do matching. Ex: "JOSE da SILVA" → "JOSE DA SILVA".

**CE-09**: Todos os funcionarios de um setor com atestado no mesmo dia (surto/epidemia).
- Comportamento esperado: Setor aparece como "Setor Critico" no KPI. Motor de decisao gera alerta "setor_critico" se total_dias > 50.

**CE-10**: Funcionario com licenca maternidade de 120 dias.
- Comportamento esperado: Motor de decisao exclui do calculo de recorrencia (motivo_exclusao: "Licenca maternidade"). Nao gera alerta.

**CE-11**: Nenhum funcionario ativo na base.
- Comportamento esperado: `ativos = 0` → divisao por zero no calculo de absenteismo. No legado: `ativos > 0 ? formula : 0`, resultado 0%.

**CE-12**: Token Convenia invalido ou expirado.
- Comportamento esperado: API retorna HTTP 401/403. Sistema exibe "Erro: HTTP 401 (verifique o token)" no status.

---

## 28. Melhorias Futuras

**MF-01**: **Predicao de absenteismo com Machine Learning**
- Usar historico de atestados, sazonalidade (inverno = mais respiratorios), perfil demografico para prever picos de absenteismo e permitir planejamento de contingencia.

**MF-02**: **Integracao com prontuario medico ocupacional digital**
- Receber atestados diretamente do sistema do medico do trabalho, sem necessidade de digitacao/importacao manual.

**MF-03**: **Gamificacao de assiduidade**
- Ranking de assiduidade por setor com metas progressivas. Recompensas para setores com menor absenteismo.

**MF-04**: **Alerta preventivo por padrao climatico**
- Integrar com dados meteorologicos para alertar RH sobre previsao de aumento de atestados em periodos de frio intenso, chuvas, etc.

**MF-05**: **Portal do colaborador para auto-registro**
- Permitir que o proprio funcionario registre seu atestado com foto do documento, aguardando validacao do RH.

**MF-06**: **Integracao com plano de saude**
- Receber automaticamente informacoes de utilizacao do plano para correlacao com absenteismo.

**MF-07**: **Dashboard comparativo com benchmarks do setor**
- Comparar taxa de absenteismo da Classic com benchmarks da industria de couros/calçados (ABNT, FIESP).

**MF-08**: **Analise de causa-raiz com IA**
- Usar NLP para analisar observacoes e textos de OCR, identificando padroes de causas (ergonomia, estresse, ambiente).

**MF-09**: **Calculo de Bradford Factor**
- Implementar o indice Bradford (S x S x D, onde S = spells e D = duration) para identificar absenteismo de curta duracao recorrente (mais custoso que afastamentos longos).

**MF-10**: **Relatorio para eSocial automatizado**
- Gerar automaticamente os XMLs dos eventos S-2230 (afastamento temporario) e S-2231 (cessacao de afastamento) a partir dos registros de ocorrencias.

**MF-11**: **Mapa de calor por setor e turno**
- Visualizar absenteismo em mapa de calor cruzando setor x turno x dia da semana para identificar padroes operacionais.

**MF-12**: **Integracao com relogio de ponto biometrico em tempo real**
- Detectar ausencias no momento da ocorrencia (falta de batida) e gerar alerta imediato para o gestor.

**MF-13**: **Workflow de retorno ao trabalho (Return-to-Work)**
- Apos afastamento prolongado (> 15 dias), disparar checklist automatico:
  - ASO de retorno
  - Reuniao com gestor
  - Verificacao de posto de trabalho
  - Acompanhamento nos primeiros 30 dias

**MF-14**: **Relatorio de custo total de absenteismo**
- Incluir custos indiretos: horas extras de cobertura, queda de produtividade do time, custos administrativos de processamento.

**MF-15**: **API publica para integracao com ERPs externos**
- Endpoint REST padronizado para que sistemas de folha de pagamento consultem dados de absenteismo diretamente.

---

*Documento gerado em 2026-07-27 como especificacao funcional para rebuild da feature F-14 (Absenteismo) no SIGA ERP.*
*Fonte: Classic RH (app.classiccouros.com.br) -- repositorio classic-ia.*
*Autor: Analista de Negocios Senior (IA) -- Sessao Claude Code.*
