# F-10 -- Gestao de Colaboradores

## Especificacao Funcional Completa para Reconstrucao no SIGA ERP

---

## 1. Identificacao

| Atributo       | Valor                                                                 |
|----------------|-----------------------------------------------------------------------|
| **Codigo**     | F-10                                                                  |
| **Nome**       | Gestao de Colaboradores                                               |
| **Modulo**     | RH (Recursos Humanos)                                                 |
| **Submodulo**  | Pessoas                                                               |
| **Versao legada** | Classic RH v2 -- colaboradores.html + ficha.html + ficha_360.html  |
| **Objetivo**   | Manter o cadastro completo de todos os colaboradores da empresa (ativos, afastados, em ferias e desligados), oferecendo consulta rapida, edicao de dados cadastrais, importacao em lote via eSocial (CSV), exportacao para planilha, e acesso a ficha individual com historico 360 graus (score, SST, ponto, beneficios, custos, timeline). |

---

## 2. Problema que Resolve

### Contexto operacional
A Classic Importacao e Exportacao de Couros EIRELI possui 629 registros de colaboradores (133 ativos, ~10 afastados, ~483 desligados) distribuidos em duas unidades: Classic (Jardim Alegre-PR) e APAC (cooperativa). Antes deste modulo, os dados de colaboradores estavam dispersos em planilhas Excel, no Convenia (plataforma terceirizada de RH) e em controles manuais de papel.

### Dores resolvidas
1. **Centralizacao cadastral**: Unifica todas as informacoes de pessoal em fonte unica de verdade, eliminando divergencias entre planilhas, Convenia e controles de papel.
2. **Conformidade legal**: Garante campos obrigatorios para eSocial (CPF, CBO, matricula, data admissao, categoria), rastreabilidade de Jovem Aprendiz (Art. 429 CLT -- 5% a 15%) e identificacao de Cargo de Confianca (Art. 62 II CLT).
3. **Visibilidade gerencial**: Oferece KPIs em tempo real (total ativos, APAC, admitidos no mes, tempo medio, quota Jovem Aprendiz).
4. **Agilidade operacional**: Importacao em lote de CSV eSocial, busca por nome, filtro por status/cidade/unidade, exportacao CSV com BOM UTF-8 para Excel.
5. **Ficha completa**: Acesso instantaneo a ficha 360 de cada colaborador com score Motor v2, atestados, ponto, SST, beneficios, documentos, custos e timeline historica.
6. **Eliminacao de exclusao fisica**: O sistema impede DELETE fisico de funcionarios (trigger `fn_block_delete_funcionario`), garantindo que todo historico trabalhista e previdenciario permaneca intacto.

---

## 3. Processo Operacional Completo

### 3.1 Fluxo de Consulta (Listagem)
```
Entrada: Usuario acessa /rh/colaboradores.html
   |
   v
Autenticacao: Shell.init() verifica sessao JWT (rh_sess_v2)
   |-- Se nao autenticado: redireciona para login.html
   |-- Se autenticado: carrega perfil (admin/rh/gestor/visualizador)
   |
   v
Carga inicial:
   1. API.get('rh_deslocamento_cidades?ativo=eq.true&order=cidade.asc&select=cidade')
   2. API.get('rh_colaboradores?status=in.(ativo,experiencia)&order=nome.asc')
   3. API.get('rh_setores?ativo=eq.true&order=nome')
   |
   v
Renderizacao:
   1. Calcular KPIs (total ativos, APAC, admitidos mes, tempo medio, Jovem Aprendiz)
   2. Popular filtros (cidades, unidades)
   3. Renderizar tabela com colunas: Nome, CPF (mascarado), Setor, Cargo, Data Admissao, Status, JA, Unidade
   |
   v
Interacao:
   - Busca por nome (filtro client-side em tempo real)
   - Filtro por status (Ativos | Ativos+Afastados | Todos)
   - Filtro por cidade (dropdown dinamico)
   - Filtro por unidade (Classic | APAC)
   - Ordenacao por qualquer coluna (click no header)
   |
   v
Resultado: Lista filtrada com contagem "X colaborador(es) encontrado(s)"
```

### 3.2 Fluxo de Cadastro (Novo Colaborador)
```
Entrada: Clicar botao "+ Novo Colaborador"
   |-- Verificacao: botao so visivel se API.canAccess('editar_funcionario')
   |
   v
Modal "Novo Colaborador" abre com formulario limpo
   |-- Valores padrao: Estado=PR, Turno=Comercial, Status=Ativo, Unidade=Classic, Categoria=101-CLT
   |
   v
Preenchimento do formulario (18 campos)
   |
   v
Validacao client-side:
   [?] Nome preenchido?
      |-- Nao: toast "Preencha o nome do colaborador"
   [?] CPF com 11 digitos?
      |-- Nao: toast "CPF invalido (11 digitos)"
   [?] Data admissao preenchida?
      |-- Nao: toast "Preencha a data de admissao"
   |
   v
Normalizacao:
   - Nome convertido para UPPERCASE
   - CPF limpo (somente digitos)
   - Estado convertido para UPPERCASE
   - Setor: resolve ID para nome via allSetores
   - Timestamp atualizado_em = new Date().toISOString()
   |
   v
Persistencia: API.post('rh_colaboradores', payload)
   |-- PostgREST escreve na view rh_colaboradores (que mapeia para rh_funcionarios)
   [?] Sucesso (res.ok)?
      |-- Sim: toast "Colaborador cadastrado", logEvent('colab_create'), fecha modal, recarrega lista
      |-- Nao, erro duplicate/unique: toast "CPF ja cadastrado no sistema"
      |-- Nao, outro erro: toast "Erro ao salvar: {erro}"
```

### 3.3 Fluxo de Edicao
```
Entrada: Clicar em linha da tabela
   |
   v
Modal "Editar Colaborador" abre preenchido com dados atuais
   |
   v
Mesma validacao e normalizacao do cadastro
   |
   v
Persistencia: API.patch('rh_colaboradores?id=eq.{id}', payload)
   [?] Sucesso?
      |-- Sim: toast "Colaborador atualizado", logEvent('colab_update'), fecha modal, recarrega
      |-- Nao: toast com erro
```

### 3.4 Fluxo de Importacao eSocial (CSV)
```
Entrada: Clicar botao "Importar eSocial"
   |
   v
Modal com textarea para colar CSV separado por ponto-e-virgula
   Formato: nome;cpf;matricula;categoria;data_admissao;cbo;estabelecimento
   |
   v
Validacao:
   [?] CSV nao vazio?
   [?] Pelo menos 2 linhas (cabecalho + 1 dado)?
   [?] Cabecalho contem: nome, cpf, data_admissao (obrigatorios)?
   |
   v
Parse linha a linha:
   - nome -> UPPERCASE
   - cpf -> limpar digitos, validar 11 caracteres
   - estabelecimento -> se contem "APAC" -> unidade "apac", senao "classic"
   - Valores padrao: status=ativo, estado=PR, turno=comercial, categoria=101
   |
   v
Insercao sequencial (row by row):
   Para cada linha valida: API.post('rh_colaboradores', row)
   [?] Sucesso: incrementar contador inseridos
   [?] Erro duplicate/unique: registrar "CPF duplicado"
   [?] Outro erro: registrar mensagem
   |
   v
Resultado exibido no modal:
   - Sucesso total: "Importacao concluida: X colaborador(es) inserido(s)."
   - Parcial: "Inseridos: X | Erros: Y" + lista de erros
   |
   v
logEvent('colab_import', { total, inserted, errors })
Recarregar lista
```

### 3.5 Fluxo de Exportacao CSV
```
Entrada: Clicar botao "Exportar CSV"
   |
   v
Pegar dados filtrados atuais (getFiltered())
   [?] Lista vazia? -> toast "Nenhum dado para exportar"
   |
   v
Gerar CSV com BOM UTF-8:
   Colunas: nome, cpf, matricula, cargo, cbo, salario_base, data_admissao, cidade, estado, turno, status, unidade, categoria
   Separador: ponto-e-virgula (;)
   |
   v
Download automatico: colaboradores_YYYY-MM-DD.csv
toast "CSV exportado"
```

### 3.6 Fluxo de Ficha Individual (ficha.html)
```
Entrada: Busca global no topbar -> click no resultado -> ficha.html?id={uuid}
   |
   v
RPC: API.rpc('ficha_completa', { p_funcionario_id: id })
   |-- Retorna JSONB com: dados cadastrais, score motor v2, atestados, ponto, EPI, treinamentos, ASOs, beneficios, documentos, pendencias, ferias
   |
   v
Renderizacao:
   1. Hero card: avatar (iniciais), nome, cargo, setor, badges (status, cargo confianca, JA, turno, GHE, transporte)
   2. Score card: score final + 6 barras (presenca, producao, treinamento, estabilidade, EPI, qualidade)
   3. KPIs: atestados, ponto, EPI, treinamentos
   4. Tabs: Resumo, Atestados (restrito RH), Ponto, SST, Beneficios, Documentos, Pendencias
```

### 3.7 Fluxo de Ficha 360 (ficha_360.html)
```
Entrada: Menu lateral "Ficha 360" ou link direto ficha_360.html?id={uuid}
   |-- Se sem ID: exibe seletor de colaborador com busca
   |
   v
RPCs paralelas:
   1. API.rpc('ficha_360_colaborador', { p_funcionario_id: id })
   2. API.get('arm_ocupacoes?funcionario_id=eq.{id}&data_devolucao=is.null&select=...')
   |
   v
Renderizacao completa com 8 tabs:
   0. Dados Gerais (pessoais + contratuais + transporte + armario)
   1. RH (ocorrencias + atestados + ferias)
   2. SST (ASOs + treinamentos + EPIs + acidentes)
   3. Ponto (registros 30d + ultimos 15 dias + totais mensais)
   4. Beneficios (tipo, status, valor, data inicio)
   5. Documentos (total + por categoria + recentes)
   6. Custos (salario + transporte + beneficios + SST 12m + historico)
   7. Historico (timeline + hist. cargo + hist. salario + hist. setor + pendencias)
```

---

## 4. Usuarios

| Perfil | Consultar lista | Cadastrar | Editar | Importar eSocial | Exportar CSV | Ver ficha | Ver atestados | Ver custos | Ver salario |
|--------|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **administrador** | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim |
| **rh** | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim | Sim |
| **gestor_confianca** | Sim (subordinados) | Nao | Nao | Nao | Sim | Sim (subordinados) | Nao | Nao | Sim (subordinados) |
| **gestor** | Sim (subordinados) | Nao | Nao | Nao | Sim | Sim (subordinados) | Nao | Nao | Nao |
| **visualizador** | Sim | Nao | Nao | Nao | Sim | Sim | Nao | Nao | Nao |

**Observacoes sobre permissoes no legado:**
- Botoes "+ Novo Colaborador" e "Importar eSocial" sao ocultos via `display:none` quando `API.canAccess('editar_funcionario')` retorna false.
- A permissao `editar_funcionario` esta restrita a perfis `administrador` e `rh` (definido em `api.js` PERMISSIONS).
- Na ficha (ficha.html), a aba "Atestados" e restrita a perfis `admin` e `rh` (verificacao `isRH`). Demais perfis veem mensagem "Dados restritos a RH".
- Na ficha 360 (ficha_360.html), dados sensiveis (CPF completo, PIS, CTPS, salario, ocorrencias detalhadas, custos) sao restritos por perfil via a RPC `ficha_360_colaborador` no servidor.

---

## 5. Fluxograma

```
[INICIO] --> [Autenticacao JWT]
   |
   [?] Sessao valida?
   |-- Nao --> [Redirecionar login.html] --> [FIM]
   |-- Sim --> [Carregar perfil usuario]
         |
         v
   [Carregar dados: cidades + colaboradores + setores]
         |
         v
   [Calcular KPIs] --> [Renderizar tabela]
         |
         v
   [AGUARDAR ACAO DO USUARIO]
         |
         |-- [Buscar por nome] --> [Filtro client-side] --> [Re-renderizar tabela]
         |
         |-- [Mudar filtro status] --> [Recarregar dados do servidor] --> [Re-renderizar]
         |
         |-- [Mudar filtro cidade/unidade] --> [Filtro client-side] --> [Re-renderizar]
         |
         |-- [Ordenar coluna] --> [Sort client-side] --> [Re-renderizar]
         |
         |-- [Click linha da tabela] --> [Abrir modal edicao]
         |      |-- [Preencher campos] --> [Salvar]
         |      |      [?] Validacao OK?
         |      |      |-- Nao --> [Toast erro] --> [Permanecer no modal]
         |      |      |-- Sim --> [API.patch] --> [?] Sucesso?
         |      |             |-- Sim --> [Toast OK] --> [Fechar modal] --> [Recarregar dados]
         |      |             |-- Nao --> [Toast erro] --> [Permanecer no modal]
         |      |-- [Cancelar] --> [Fechar modal]
         |
         |-- [+ Novo Colaborador] --> [?] Tem permissao editar_funcionario?
         |      |-- Nao --> [Botao oculto, acao impossivel]
         |      |-- Sim --> [Abrir modal novo]
         |             |-- [Preencher campos] --> [Salvar]
         |             |      [?] Validacao OK?
         |             |      |-- Nao --> [Toast erro]
         |             |      |-- Sim --> [API.post] --> [?] Sucesso?
         |             |             |-- Sim --> [Toast OK] --> [Fechar modal] --> [Recarregar]
         |             |             |-- Nao, CPF duplicado --> [Toast "CPF ja cadastrado"]
         |             |             |-- Nao, outro --> [Toast erro generico]
         |             |-- [Cancelar] --> [Fechar modal]
         |
         |-- [Importar eSocial] --> [?] Tem permissao?
         |      |-- Sim --> [Abrir modal import]
         |             |-- [Colar CSV] --> [Importar]
         |             |      [?] CSV valido?
         |             |      |-- Nao --> [Toast erro formato]
         |             |      |-- Sim --> [Parse + insercao sequencial] --> [Exibir resultado]
         |             |-- [Cancelar] --> [Fechar modal]
         |
         |-- [Exportar CSV] --> [Gerar CSV dos dados filtrados] --> [Download automatico]
         |
         v
   [AGUARDAR PROXIMA ACAO]
```

---

## 6. Regras de Negocio

### RN-01: Obrigatoriedade de Campos no Cadastro
- **Nome completo**: obrigatorio, convertido automaticamente para UPPERCASE.
- **CPF**: obrigatorio, exatamente 11 digitos (apos remocao de mascara).
- **Data de admissao**: obrigatoria.
- Os demais campos sao opcionais no formulario do frontend, porem na tabela `rh_funcionarios` do banco, `empresa_id`, `filial_id`, `setor_id` e `cargo_id` sao NOT NULL. A view `rh_colaboradores` abstrai isso.

### RN-02: Unicidade de CPF
- CPF deve ser unico em todo o sistema (`rh_funcionarios.cpf VARCHAR(11) UNIQUE NOT NULL`).
- Na tentativa de cadastro/importacao com CPF duplicado, o sistema exibe toast "CPF ja cadastrado no sistema".

### RN-03: Unicidade de Matricula
- Matricula deve ser unica (`rh_funcionarios.matricula TEXT UNIQUE`), porem e opcional.
- Erro de duplicidade de matricula e tratado como erro generico.

### RN-04: Mascaramento de CPF na Listagem
- Na tabela, CPF e exibido mascarado: `***.***.XXX-XX` (apenas ultimos 5 digitos visiveis).
- Na edicao, CPF e exibido com mascara completa `000.000.000-00`.

### RN-05: Formatacao de CPF em Tempo Real
- Campo CPF aplica mascara `000.000.000-00` a cada digitacao, limitando a 14 caracteres (11 digitos + 3 separadores).

### RN-06: Calculo de KPI -- Total Ativos
- Conta colaboradores onde `status = 'ativo'` no dataset carregado.

### RN-07: Calculo de KPI -- Total APAC
- Conta colaboradores onde `status = 'ativo' AND unidade = 'apac'`.

### RN-08: Calculo de KPI -- Admitidos Este Mes
- Conta colaboradores cuja `data_admissao` comeca com o formato `YYYY-MM` do mes corrente.
- Conta independente do status (inclui desligados que foram admitidos no mes).

### RN-09: Calculo de KPI -- Tempo Medio de Empresa
- Calcula media de dias entre `data_admissao` e hoje para todos os colaboradores ativos com data de admissao preenchida.
- Converte para anos: `(totalDias / qtdAtivos / 365.25).toFixed(1)` e exibe como "X.Xa".
- Se nao houver ativos com data, exibe "--".

### RN-10: Calculo de KPI -- Jovem Aprendiz (Art. 429 CLT)
- Conta ativos onde `jovem_aprendiz = true`.
- Calcula limites legais: minimo = `ceil(totalAtivos * 0.05)`, maximo = `floor(totalAtivos * 0.15)`.
- Exibe: "{qtd JA} de {min}-{max}".
- Label: "Jovens Aprendiz (CLT Art. 429: 5% a 15% = {min} a {max})".

### RN-11: Filtro por Status
- Tres opcoes pre-definidas:
  - "Ativos" = `status IN (ativo, experiencia)` -- padrao
  - "Ativos + Afastados" = `status IN (ativo, experiencia, afastado, ferias)`
  - "Todos (incl. Desligados)" = `status IN (ativo, experiencia, afastado, ferias, desligado)`
- Mudanca de filtro status faz nova chamada ao servidor (recarrega dados).

### RN-12: Filtros Client-Side
- Busca por nome: filtro `includes` case-insensitive no campo `nome`.
- Filtro por cidade: match exato no campo `cidade`.
- Filtro por unidade: match exato no campo `unidade` (classic|apac).
- Todos os filtros sao aplicados simultaneamente (AND logico).

### RN-13: Ordenacao de Tabela
- Click no header da coluna ativa ordenacao ascendente.
- Click repetido na mesma coluna inverte para descendente.
- Click em outra coluna reseta para ascendente.
- Colunas ordenaveis: nome, cpf, setor, cargo, data_admissao, status, unidade.
- Ordenacao case-insensitive para strings.

### RN-14: Status Permitidos
- Valores validos: `ativo`, `afastado`, `ferias`, `desligado`, `experiencia`.
- CHECK constraint na tabela: `status IN ('ativo','afastado','ferias','desligado','experiencia')`.

### RN-15: Unidades
- Valores validos: `classic`, `apac`.
- Na view `rh_colaboradores`, derivado de `rh_filiais.nome_fantasia`: se contem "APAC" (case-insensitive) -> `apac`, senao -> `classic`.

### RN-16: Categorias
- Na view `rh_colaboradores`, mapeado de campos da tabela:
  - `modalidade_contrato = 'Experiencia'` -> `103`
  - `tipo_vinculo = 'CLT'` -> `101`
  - `tipo_vinculo = 'APAC'` -> `102`
  - Outros -> `999`

### RN-17: Tipo de Vinculo
- Valores permitidos via CHECK: `CLT`, `APAC`, `PJ`, `Terceiro`.
- Default: `CLT`.

### RN-18: Modalidade de Contrato
- Somente aplicavel quando `tipo_vinculo = 'CLT'`.
- Valores: `Indeterminado`, `Experiencia`, `Temporario`, `Intermitente`, `Safra`, `Aprendiz`, `Estagio`.
- Constraint: se `tipo_vinculo != 'CLT'`, modalidade deve ser NULL.

### RN-19: Bloqueio de Exclusao Fisica
- Trigger `trg_block_delete_func` (BEFORE DELETE) impede qualquer DELETE em `rh_funcionarios`.
- Registra a tentativa em `rh_audit_log` com acao `DELETE_BLOCKED`.
- Desligamento e feito exclusivamente via UPDATE do campo `status` para `desligado`.

### RN-20: Coerencia de Datas
- Constraint `chk_datas_coerentes`: `data_desligamento` deve ser NULL ou >= `data_admissao`.

### RN-21: Permissao de Edicao
- Somente perfis `administrador` e `rh` podem cadastrar ou editar.
- Aplicado em tres niveis:
  1. Frontend: botoes ocultos via `API.canAccess('editar_funcionario')`
  2. RLS: policies `func_insert` e `func_update` restringem a `administrador` e `rh`
  3. API: PostgREST aplica RLS automaticamente

### RN-22: Importacao eSocial -- Cabecalho Obrigatorio
- O CSV deve conter cabecalho com pelo menos: `nome`, `cpf`, `data_admissao`.
- Colunas opcionais: `matricula`, `categoria`, `cbo`, `estabelecimento`.
- Separador: ponto-e-virgula (`;`).

### RN-23: Importacao eSocial -- Mapeamento de Estabelecimento
- Se a coluna `estabelecimento` contem "APAC" (case-insensitive), a unidade e `apac`.
- Caso contrario, a unidade e `classic`.

### RN-24: Importacao eSocial -- Validacao por Linha
- Linhas com nome vazio, CPF com menos de 11 digitos ou data_admissao vazia sao silenciosamente ignoradas (skip).

### RN-25: Exportacao CSV -- Formato
- Separador: ponto-e-virgula (`;`).
- BOM UTF-8 (`﻿`) no inicio para compatibilidade com Excel.
- Ponto-e-virgulas dentro de valores sao substituidos por virgulas.
- Nome do arquivo: `colaboradores_YYYY-MM-DD.csv`.
- Exporta somente os dados atualmente filtrados/visiveis.

### RN-26: Evento de Log
- Eventos registrados via `API.logEvent()`:
  - `colab_create`: { nome, cpf } -- ao criar colaborador
  - `colab_update`: { nome, cpf } -- ao editar colaborador
  - `colab_import`: { total, inserted, errors } -- ao importar lote

### RN-27: Setor -- Resolucao ID/Nome
- No formulario, setor e selecionado via dropdown que carrega de `rh_setores?ativo=eq.true&order=nome`.
- O payload enviado inclui tanto `setor_id` (UUID) quanto `setor` (nome textual).

### RN-28: Estado Padrao
- Valor default do campo Estado: "PR" (Parana).

### RN-29: Busca Global (Shell/Topbar)
- O topbar possui busca global que pesquisa em `vw_funcionario_base` (status in ativo, afastado, ferias, experiencia).
- Resultados linkam para `ficha.html?id={uuid}`.
- Exibe ate 8 resultados.
- Debounce de 250ms.
- Minimo 2 caracteres para iniciar busca.
- Cache client-side (primeira busca carrega todos, filtros subsequentes sao locais).

---

## 7. Campos

### 7.1 Campos do Formulario (Modal Cadastro/Edicao -- colaboradores.html)

| # | Campo | ID HTML | Tipo HTML | Tipo SQL (rh_funcionarios) | Obrigatorio Frontend | Obrigatorio BD | Valor Padrao | Mascara/Formato | Validacao | Observacoes |
|---|-------|---------|-----------|---------------------------|:---:|:---:|-------------|-----------------|-----------|-------------|
| 1 | Nome completo | f-nome | text | TEXT NOT NULL (nome_completo) | Sim | Sim | -- | -- | Nao vazio; convertido para UPPERCASE | Classe CSS `full` (largura total) |
| 2 | CPF | f-cpf | text | VARCHAR(11) UNIQUE NOT NULL | Sim | Sim | -- | 000.000.000-00 | Exatamente 11 digitos; mascara em tempo real; unicidade | maxlength=14; formatado via cpfFormat() |
| 3 | Matricula | f-matricula | text | TEXT UNIQUE | Nao | Nao | -- | -- | Unicidade (quando preenchido) | Pode ser nulo |
| 4 | Cargo | f-cargo | text | -- (resolvido via cargo_id -> rh_cargos.nome) | Nao | Sim* | -- | -- | Texto livre no legado | *No banco, cargo_id e NOT NULL |
| 5 | Setor | f-setor | select | UUID NOT NULL (setor_id -> rh_setores) | Nao | Sim* | "" (vazio) | -- | Carregado de rh_setores ativas | *No banco, setor_id e NOT NULL |
| 6 | GHE | f-ghe | text | TEXT (ghe) | Nao | Nao | -- | -- | Texto livre | Grupo Homogeneo de Exposicao (SST) |
| 7 | CBO | f-cbo | text | VARCHAR(10) (via rh_cargos.cbo) | Nao | Nao | -- | -- | Texto livre | Classificacao Brasileira de Ocupacoes |
| 8 | Salario Base (R$) | f-salario | number | NUMERIC(12,2) | Nao | Nao | -- | -- | step=0.01, min=0 | Exibido com 2 casas decimais |
| 9 | Data Admissao | f-data-admissao | date | DATE NOT NULL | Sim | Sim | -- | YYYY-MM-DD (input date nativo) | Nao vazio | Constraint: >= data_desligamento |
| 10 | Cidade | f-cidade | select | JSONB (endereco->>'cidade') | Nao | Nao | "" | -- | Carregado de rh_deslocamento_cidades + "Jardim Alegre" garantido + "Outro" | Na view rh_colaboradores, lido de endereco JSONB |
| 11 | Estado | f-estado | text | JSONB (endereco->>'uf') | Nao | Nao | "PR" | -- | maxlength=2; UPPERCASE | Default Parana |
| 12 | Turno | f-turno | select | VARCHAR(30) DEFAULT 'comercial' | Nao | Nao | "comercial" | -- | Opcoes fixas: Comercial, 1o turno, 2o turno | -- |
| 13 | Status | f-status | select | VARCHAR(20) NOT NULL DEFAULT 'ativo' | Nao | Sim | "ativo" | -- | Opcoes: Ativo, Afastado, Ferias, Desligado | CHECK constraint no banco tambem inclui 'experiencia' |
| 14 | Unidade | f-unidade | select | Derivado de filial_id (na view) | Nao | Nao* | "classic" | -- | Opcoes: Classic, APAC | *No banco, filial_id e NOT NULL |
| 15 | Categoria | f-categoria | select | Derivado (na view) | Nao | Nao | "101" | -- | Opcoes: 101-CLT, 103-Experiencia | Na view, mapeado de tipo_vinculo + modalidade_contrato |
| 16 | Cargo de Confianca | f-cargo-confianca | checkbox | BOOLEAN (cargo_confianca) | Nao | Nao | false | -- | CLT Art. 62, II | Colaborador isento de controle de ponto |
| 17 | Jovem Aprendiz | f-jovem-aprendiz | checkbox | BOOLEAN (jovem_aprendiz) | Nao | Nao | false | -- | Lei 10.097/2000 | Afeta KPI Art. 429 |

### 7.2 Colunas da Tabela (Listagem -- colaboradores.html)

| # | Coluna | Campo fonte | Ordenavel | Formato | Observacoes |
|---|--------|-------------|:---------:|---------|-------------|
| 1 | Nome | nome | Sim | Texto | -- |
| 2 | CPF | cpf | Sim | ***.***.XXX-XX | Mascarado (cpfMask) |
| 3 | Setor | setor | Sim | Texto | -- ou "--" se null |
| 4 | Cargo | cargo | Sim | Texto | -- ou "--" se null |
| 5 | Data Admissao | data_admissao | Sim | DD/MM/YYYY | Via API.fmtData() |
| 6 | Status | status | Sim | Badge colorido | CSS class `status-{status}` |
| 7 | JA | jovem_aprendiz | Nao | Badge "JA" azul | So exibido se true |
| 8 | Unidade | unidade | Sim | Badge UPPERCASE | CSS class `unidade-{unidade}` |

### 7.3 Campos Completos da Tabela rh_funcionarios (banco de dados)

| # | Coluna | Tipo | NOT NULL | Default | FK / Constraint | Observacoes |
|---|--------|------|:--------:|---------|-----------------|-------------|
| 1 | id | UUID | Sim | gen_random_uuid() | PK | -- |
| 2 | nome_completo | TEXT | Sim | -- | -- | Convertido para UPPERCASE pelo frontend |
| 3 | cpf | VARCHAR(11) | Sim | -- | UNIQUE | Somente digitos, sem mascara |
| 4 | data_nascimento | DATE | Nao | -- | -- | Exibido na ficha 360 |
| 5 | sexo | VARCHAR(20) | Nao | -- | -- | -- |
| 6 | estado_civil | VARCHAR(30) | Nao | -- | -- | -- |
| 7 | telefone | TEXT | Nao | -- | -- | -- |
| 8 | email_pessoal | TEXT | Nao | -- | -- | Usado para vincular com rh_usuarios |
| 9 | endereco | JSONB | Nao | -- | -- | {logradouro, numero, complemento, bairro, cidade, uf, cep} |
| 10 | matricula | TEXT | Nao | -- | UNIQUE | -- |
| 11 | pis_pasep | VARCHAR(11) | Nao | -- | -- | Exibido na ficha 360 (restrito admin/rh) |
| 12 | ctps_numero | TEXT | Nao | -- | -- | Exibido na ficha 360 (restrito admin/rh) |
| 13 | ctps_serie | TEXT | Nao | -- | -- | Exibido na ficha 360 (restrito admin/rh) |
| 14 | empresa_id | UUID | Sim | -- | FK rh_empresas(id) | -- |
| 15 | filial_id | UUID | Sim | -- | FK rh_filiais(id) | Determina unidade (classic/apac) |
| 16 | setor_id | UUID | Sim | -- | FK rh_setores(id) | -- |
| 17 | cargo_id | UUID | Sim | -- | FK rh_cargos(id) | CBO herdado de rh_cargos |
| 18 | centro_custo_id | UUID | Nao | -- | FK rh_centros_custo(id) | -- |
| 19 | gestor_id | UUID | Nao | -- | FK rh_funcionarios(id) | Self-reference (hierarquia) |
| 20 | tipo_vinculo | VARCHAR(20) | Sim | 'CLT' | CHECK IN (CLT, APAC, PJ, Terceiro) | -- |
| 21 | modalidade_contrato | VARCHAR(30) | Nao | -- | CHECK + constraint vinculo_modalidade | So aplicavel para CLT |
| 22 | salario_base | NUMERIC(12,2) | Nao | -- | -- | Valor bruto mensal |
| 23 | data_admissao | DATE | Sim | -- | -- | -- |
| 24 | data_desligamento | DATE | Nao | -- | chk_datas_coerentes (>= data_admissao) | -- |
| 25 | turno | VARCHAR(30) | Nao | 'comercial' | -- | -- |
| 26 | carga_horaria_semanal | NUMERIC(4,1) | Nao | 44.0 | -- | Horas semanais |
| 27 | status | VARCHAR(20) | Sim | 'ativo' | CHECK IN (ativo, afastado, ferias, desligado, experiencia) | Nunca deletar, usar status |
| 28 | grau_risco_sst | VARCHAR(10) | Sim | 'padrao' | CHECK IN (alto, padrao, baixo, isento) | -- |
| 29 | elegivel_beneficios | BOOLEAN | Sim | TRUE | -- | Regra de negocio |
| 30 | elegivel_sst | BOOLEAN | Sim | TRUE | -- | Regra de negocio |
| 31 | observacoes | TEXT | Nao | -- | -- | -- |
| 32 | created_at | TIMESTAMPTZ | Sim | NOW() | -- | -- |
| 33 | updated_at | TIMESTAMPTZ | Sim | NOW() | -- | Mantido por trigger fn_set_updated_at |
| 34 | created_by | UUID | Nao | -- | FK auth.users(id) | -- |
| 35* | cargo_confianca | BOOLEAN | Nao | -- | -- | *Coluna em producao, nao presente no script FUNDACAO inicial |
| 36* | jovem_aprendiz | BOOLEAN | Nao | -- | -- | *Coluna em producao, nao presente no script FUNDACAO inicial |
| 37* | ghe | TEXT | Nao | -- | -- | *Grupo Homogeneo de Exposicao |
| 38* | transporte_onibus | BOOLEAN/TEXT | Nao | -- | -- | *Flag transporte onibus |
| 39* | transporte_rota | TEXT | Nao | -- | -- | *Rota do transporte |
| 40* | transporte_custo | NUMERIC | Nao | -- | -- | *Custo mensal transporte |
| 41* | tipo_afastamento | VARCHAR | Nao | -- | -- | *Tipo do afastamento quando status=afastado |
| 42* | afastamento_inicio | DATE | Nao | -- | -- | *Data inicio do afastamento |
| 43* | afastamento_previsao_retorno | DATE | Nao | -- | -- | *Previsao de retorno |
| 44* | afastamento_origem | TEXT | Nao | -- | -- | *Origem do afastamento |

> *Nota: As colunas 35-44 existem em producao e sao referenciadas pelas RPCs (ficha_completa, ficha_360_colaborador, motor_v2), porem nao constam no script FUNDACAO_BANCO_v2.sql original. Foram adicionadas via ALTER TABLE em producao.

### 7.4 Campos da View rh_colaboradores

| # | Coluna | Origem | Tipo | Observacoes |
|---|--------|--------|------|-------------|
| 1 | id | rh_funcionarios.id | UUID | PK |
| 2 | nome | rh_funcionarios.nome_completo | TEXT | Alias |
| 3 | cpf | rh_funcionarios.cpf | VARCHAR(11) | -- |
| 4 | matricula | rh_funcionarios.matricula | TEXT | -- |
| 5 | cargo | rh_cargos.nome | TEXT | JOIN via cargo_id |
| 6 | cbo | rh_cargos.cbo | VARCHAR(10) | JOIN via cargo_id |
| 7 | salario_base | rh_funcionarios.salario_base | NUMERIC(12,2) | -- |
| 8 | data_admissao | rh_funcionarios.data_admissao | DATE | -- |
| 9 | data_desligamento | rh_funcionarios.data_desligamento | DATE | -- |
| 10 | data_nascimento | rh_funcionarios.data_nascimento | DATE | -- |
| 11 | cidade | COALESCE(endereco->>'cidade', '') | TEXT | Extraido de JSONB |
| 12 | estado | COALESCE(endereco->>'uf', 'PR') | TEXT | Default PR |
| 13 | setor | rh_setores.nome | TEXT | JOIN via setor_id |
| 14 | setor_id | rh_funcionarios.setor_id | UUID | -- |
| 15 | categoria | CASE derivado | TEXT | 101/102/103/999 |
| 16 | estabelecimento | rh_filiais.nome_fantasia | TEXT | -- |
| 17 | turno | rh_funcionarios.turno | VARCHAR(30) | -- |
| 18 | status | rh_funcionarios.status | VARCHAR(20) | -- |
| 19 | unidade | CASE derivado de filial | TEXT | classic ou apac |
| 20 | criado_em | rh_funcionarios.created_at | TIMESTAMPTZ | -- |
| 21 | atualizado_em | rh_funcionarios.updated_at | TIMESTAMPTZ | -- |

---

## 8. Tabelas

### 8.1 Tabelas Diretamente Utilizadas

| Tabela | Papel | Operacoes | Relacionamento |
|--------|-------|-----------|----------------|
| **rh_funcionarios** | Tabela canonica de funcionarios | INSERT, UPDATE (via view) | Central -- todas as demais referenciam |
| **rh_colaboradores** (VIEW) | Compatibilidade frontend | SELECT, INSERT*, PATCH* | Sobre rh_funcionarios + JOINs |
| **vw_funcionario_base** (VIEW) | Busca global e listagens enxutas | SELECT | Sobre rh_funcionarios + JOINs |
| **rh_setores** | Setores/departamentos | SELECT (para dropdown) | FK: filial_id -> rh_filiais |
| **rh_cargos** | Cargos com CBO | SELECT (via view) | FK: nivel_hierarquico_id -> rh_niveis_hierarquicos |
| **rh_filiais** | Filiais/estabelecimentos | SELECT (via view) | FK: empresa_id -> rh_empresas |
| **rh_empresas** | Empresas do grupo | SELECT (via view) | -- |
| **rh_deslocamento_cidades** | Cidades para beneficio deslocamento | SELECT (para dropdown) | Independente |
| **rh_hist_cargo** | Historico de mudancas de cargo | INSERT (via trigger) | FK: funcionario_id, cargo_id |
| **rh_hist_salario** | Historico de reajustes salariais | INSERT (via trigger) | FK: funcionario_id |
| **rh_hist_setor** | Historico de transferencias de setor | INSERT (via trigger) | FK: funcionario_id, setor_id |
| **rh_hist_gestor** | Historico de mudancas de gestor | INSERT (via trigger) | FK: funcionario_id, gestor_id |
| **rh_audit_log** | Log de auditoria | INSERT (via trigger) | -- |
| **rh_usuarios** | Usuarios do sistema com RBAC | SELECT (via RLS/auth) | FK: auth_uid -> auth.users |

### 8.2 Tabelas Utilizadas pela Ficha (ficha.html + ficha_360.html)

| Tabela | Uso na Ficha |
|--------|-------------|
| **rh_ocorrencias** | Atestados, advertencias, suspensoes |
| **rh_ponto** | Registros diarios de ponto |
| **rh_ponto_totais** | Totais mensais de ponto |
| **rh_ferias** | Periodos de ferias |
| **rh_beneficio** | Beneficios do colaborador |
| **rh_documento_digital** | Documentos digitalizados |
| **rh_acompanhamento** | Acompanhamentos por gestores |
| **rh_custo_colaborador** | Historico de custos por competencia |
| **sst_aso** | Atestados de Saude Ocupacional |
| **sst_treinamento** | Treinamentos e NRs |
| **sst_tipo_treinamento** | Catalogo de tipos de treinamento |
| **sst_epi_entrega** | Entregas de EPI |
| **sst_catalogo_epi** | Catalogo de EPIs (descricao, CA) |
| **sst_acidente** | Registros de acidentes de trabalho |
| **sst_fatura_historico_colaborador** | Custos SST por colaborador |
| **motor_score** | Scores do Motor v2 |
| **agente_pendencia** / **orch_alerta** | Pendencias e alertas |
| **arm_ocupacoes** / **arm_armarios** / **arm_blocos** | Armarios (ficha 360) |

### 8.3 Diagrama de Relacionamentos (Nucleo)

```
rh_empresas (1) ─────── (N) rh_filiais (1) ─────── (N) rh_setores
                                    |
                                    v
                              rh_funcionarios
                               /    |    \     \
                              /     |     \     \
                             v      v      v     v
                    rh_cargos   rh_centros_custo  rh_funcionarios (gestor_id self-ref)
                        |
                        v
                 rh_niveis_hierarquicos

rh_funcionarios (1) ─── (N) rh_hist_cargo
rh_funcionarios (1) ─── (N) rh_hist_salario
rh_funcionarios (1) ─── (N) rh_hist_setor
rh_funcionarios (1) ─── (N) rh_hist_gestor
rh_funcionarios (1) ─── (N) rh_ocorrencias
rh_funcionarios (1) ─── (N) rh_ponto
rh_funcionarios (1) ─── (N) rh_ferias
rh_funcionarios (1) ─── (N) rh_beneficio
rh_funcionarios (1) ─── (N) sst_aso
rh_funcionarios (1) ─── (N) sst_treinamento
rh_funcionarios (1) ─── (N) sst_epi_entrega
rh_funcionarios (1) ─── (N) sst_acidente
```

---

## 9. Procedures/RPCs

### 9.1 RPCs Diretamente Utilizadas pelas Telas de Colaboradores

| RPC | Chamada por | Parametros | Retorno | Permissao |
|-----|-------------|------------|---------|-----------|
| **ficha_completa** | ficha.html | `p_funcionario_id UUID` | JSONB com dados cadastrais + score + atestados + ponto + EPI + treinamentos + ASOs + beneficios + documentos + pendencias + ferias | admin, rh, gestor |
| **ficha_360_colaborador** | ficha_360.html | `p_funcionario_id UUID` | JSONB completo com 15 blocos (B1-B15): cadastrais, score, ASOs, treinamentos, EPI, ocorrencias, ponto, ferias, beneficios, documentos, custos, historico (cargo/salario/setor), acidentes, pendencias, timeline | admin, rh (dados completos) / gestor_confianca (parcial) |

### 9.2 RPCs que Consomem Dados de Colaboradores (Indiretamente)

| RPC | Relacao com Colaboradores |
|-----|--------------------------|
| **motor_v2_calcular_scores** | Calcula score para cada funcionario ativo |
| **motor_v2_dashboard_executivo** | Exibe scores de funcionarios |
| **motor_decisoes** | Analisa absenteismo por funcionario |
| **sst_dashboard_completo** | Conta funcionarios sem ASO, sem EPI |
| **ferias_dashboard** | Lista ferias vencidas por funcionario |
| **gestor_dashboard** | Exibe equipe do setor do gestor |
| **gestor_confianca_dashboard** | Exibe equipe detalhada com salarios |
| **relatorio_atestados_dashboard** | Top funcionarios com atestados |
| **rh_cpf_map** | Mapeamento CPF para ingestores |
| **calcular_ponto_totais** | Totaliza ponto por funcionario |

---

## 10. Functions SQL

### 10.1 Functions de Infraestrutura

| Funcao | Tipo | Descricao |
|--------|------|-----------|
| **fn_set_updated_at()** | TRIGGER | Atualiza `updated_at = NOW()` automaticamente em todo UPDATE |
| **fn_audit_trigger()** | TRIGGER | Registra INSERT/UPDATE em `rh_audit_log` com dados antigos, novos e campos alterados |
| **fn_block_delete_funcionario()** | TRIGGER | Impede DELETE em rh_funcionarios; registra tentativa em audit_log |
| **fn_hist_cargo()** | TRIGGER | Registra mudanca de cargo em rh_hist_cargo |
| **fn_hist_salario()** | TRIGGER | Registra mudanca de salario em rh_hist_salario (com calculo percentual) |
| **fn_hist_setor()** | TRIGGER | Registra mudanca de setor em rh_hist_setor |
| **fn_hist_gestor()** | TRIGGER | Registra mudanca de gestor em rh_hist_gestor |
| **fn_hist_inicial()** | TRIGGER (AFTER INSERT) | Cria primeiros registros de historico ao inserir funcionario |

### 10.2 Functions de RBAC

| Funcao | Retorno | Descricao |
|--------|---------|-----------|
| **rh_perfil_atual()** | TEXT | Retorna perfil do usuario autenticado consultando rh_usuarios via auth.uid() |
| **rh_funcionario_atual_id()** | UUID | Retorna ID do funcionario vinculado ao usuario autenticado (match por email) |
| **rh_setor_do_gestor()** | UUID | Retorna setor_id do funcionario que e gestor |

### 10.3 Functions de Formatacao (Frontend -- api.js)

| Funcao | Descricao |
|--------|-----------|
| **API.fmtData(d)** | Formata data ISO para DD/MM/YYYY |
| **API.fmtMoeda(v)** | Formata valor como "R$ X.XXX,XX" |
| **API.fmtCPF(cpf)** | Formata CPF como 000.000.000-00 |
| **API.fmtTempo(dias)** | Formata dias em "Xd", "Xm", "Xa Xm" |
| **API.esc(s)** | Escapa HTML (XSS prevention) |

---

## 11. Triggers

### Triggers em rh_funcionarios

| Trigger | Evento | Timing | Funcao | Descricao |
|---------|--------|--------|--------|-----------|
| **trg_updated_func** | UPDATE | BEFORE | fn_set_updated_at() | Atualiza updated_at automaticamente |
| **trg_audit_func** | INSERT, UPDATE | AFTER | fn_audit_trigger() | Registra todas alteracoes em rh_audit_log |
| **trg_block_delete_func** | DELETE | BEFORE | fn_block_delete_funcionario() | Impede exclusao fisica; registra tentativa |
| **trg_func_hist_cargo** | UPDATE | AFTER | fn_hist_cargo() | Registra mudanca de cargo_id |
| **trg_func_hist_salario** | UPDATE | AFTER | fn_hist_salario() | Registra mudanca de salario_base (com % reajuste) |
| **trg_func_hist_setor** | UPDATE | AFTER | fn_hist_setor() | Registra mudanca de setor_id |
| **trg_func_hist_gestor** | UPDATE | AFTER | fn_hist_gestor() | Registra mudanca de gestor_id |
| **trg_func_hist_inicial** | INSERT | AFTER | fn_hist_inicial() | Cria registros iniciais de historico (cargo, salario, setor, gestor) |

**Total: 8 triggers ativos em rh_funcionarios.**

---

## 12. APIs (Endpoints PostgREST)

### 12.1 Chamadas em colaboradores.html

| # | Metodo | Endpoint | Parametros | Contexto |
|---|--------|----------|------------|----------|
| 1 | GET | `rh_deslocamento_cidades?ativo=eq.true&order=cidade.asc&select=cidade` | -- | Carregar cidades para dropdown |
| 2 | GET | `rh_colaboradores?status=in.({statusFilter})&order=nome.asc` | statusFilter = "ativo,experiencia" (default) | Listar colaboradores |
| 3 | GET | `rh_setores?ativo=eq.true&order=nome` | -- | Carregar setores para dropdown |
| 4 | POST | `rh_colaboradores` | payload completo (18 campos) | Criar novo colaborador |
| 5 | PATCH | `rh_colaboradores?id=eq.{id}` | payload completo (18 campos) | Editar colaborador existente |

### 12.2 Chamadas em ficha.html

| # | Metodo | Endpoint | Parametros | Contexto |
|---|--------|----------|------------|----------|
| 1 | RPC POST | `rpc/ficha_completa` | `{ p_funcionario_id: uuid }` | Carregar ficha completa |

### 12.3 Chamadas em ficha_360.html

| # | Metodo | Endpoint | Parametros | Contexto |
|---|--------|----------|------------|----------|
| 1 | RPC POST | `rpc/ficha_360_colaborador` | `{ p_funcionario_id: uuid }` | Carregar ficha 360 |
| 2 | GET | `arm_ocupacoes?funcionario_id=eq.{id}&data_devolucao=is.null&select=id,armario_id,data_atribuicao,chave_entregue,arm_armarios(codigo,tipo,status,arm_blocos(nome,tipo))` | -- | Carregar armario ocupado |
| 3 | GET | `vw_funcionario_base?status=in.(ativo,afastado,ferias,experiencia)&select=id,nome,cargo,setor,status&order=nome&limit=500` | -- | Seletor de colaborador (quando sem ID na URL) |

### 12.4 Chamadas em shell.js (Busca Global)

| # | Metodo | Endpoint | Parametros | Contexto |
|---|--------|----------|------------|----------|
| 1 | GET | `vw_funcionario_base?status=in.(ativo,afastado,ferias,experiencia)&select=id,nome,cargo,setor,status&order=nome&limit=500` | -- | Cache de busca global no topbar |

---

## 13. Integracoes

### 13.1 Convenia (REST API via n8n)
- **Status**: Funcional 85%
- **Relacao**: Os dados de colaboradores podem ser sincronizados bidireccionalmente com o Convenia.
- **Fluxo**: n8n workflows fazem heartbeat a cada 65min, extraindo dados de colaboradores do Convenia e sincronizando com rh_funcionarios.
- **Dados sincronizados**: nome, CPF, matricula, cargo, setor, data_admissao, status.
- **Gaps**: ferias e afastamentos existem no Convenia mas o ETL nao sincroniza (pendencia documentada).

### 13.2 eSocial (Importacao Manual)
- **Status**: Funcional via importacao CSV manual.
- **Fluxo**: Usuario exporta CSV do eSocial, cola no modal de importacao, sistema insere linha a linha.
- **Campos mapeados**: nome, cpf, matricula, categoria, data_admissao, cbo, estabelecimento.

### 13.3 Secullum (File Upload XLSX)
- **Status**: Funcional 80%.
- **Relacao indireta**: Ponto importado via Secullum e vinculado a colaboradores por funcionario_id (match por CPF/matricula).

### 13.4 BuscaEPI (REST API via n8n)
- **Status**: Parcial 60%.
- **Relacao indireta**: Entregas de EPI sao vinculadas a colaboradores por funcionario_id.

### 13.5 ATAK (REST API via n8n)
- **Status**: Funcional.
- **Relacao indireta**: Dados de producao sao rateados por colaborador para calculo de score.

---

## 14. Dashboards que Este Modulo Alimenta

| Dashboard | Pagina | Dados de Colaboradores Utilizados |
|-----------|--------|----------------------------------|
| Dashboard RH | dashboard_rh.html | Total ativos, por setor, por status, turnover |
| Dashboard Executivo | dashboard_gerencial.html | KPIs de headcount, custo pessoal |
| Motor v2 Score | motor_v2.html | Score individual de cada colaborador ativo |
| Painel SST | sst_dashboard.html | Contagem de funcionarios sem ASO, sem EPI, sem treinamento |
| Dashboard Custos | dashboard_custos.html | Salarios, beneficios, transporte por colaborador |
| Painel Gestao | painel_gestao.html | Equipe do gestor com detalhamento individual |
| Dashboard Gestor | gestor.html | Equipe resumida do setor |
| Motor Decisoes | motor_decisoes.html | Recorrencia de atestados por colaborador |
| Relatorio Atestados | relatorio_atestados.html | Top funcionarios com mais atestados |

---

## 15. Relatorios

| Relatorio | Descricao | Formato | Gerado por |
|-----------|-----------|---------|------------|
| **Exportacao CSV de Colaboradores** | Lista filtrada com 13 colunas | CSV (`;`, BOM UTF-8) | Botao "Exportar CSV" em colaboradores.html |
| **Ficha do Colaborador** | Ficha completa impressa via browser | Impressao nativa (window.print) | Botao "Imprimir" na ficha.html |
| **Relatorio de Atestados** | Analise por CID, medico, setor | HTML (tela) | relatorio_atestados.html |

---

## 16. Documentos

| Tipo | Formato | Geracao | Observacoes |
|------|---------|---------|-------------|
| Exportacao de colaboradores | CSV | Automatica (client-side) | BOM UTF-8, separador `;` |
| Impressao de ficha | HTML/PDF (via browser print) | Via `window.print()` | Usa CSS de impressao do ds.css |
| Documentos digitalizados | Diversos (listados na ficha) | Consultados na aba "Documentos" da ficha | Armazenados em rh_documento_digital |

---

## 17. Evidencias

| Tipo | Presente no Legado | Detalhes |
|------|--------------------|----------|
| Fotos | Nao | Nao ha upload de foto de colaborador |
| Arquivos | Parcial | rh_documento_digital armazena referencia a documentos por categoria |
| Assinaturas | Nao | Nenhum processo de assinatura digital |
| QR Code | Nao | Nao implementado |
| Avatar | Parcial | Gerado por iniciais do nome (sem foto real) |

---

## 18. Permissoes

### 18.1 Permissoes no Frontend (api.js PERMISSIONS)

```javascript
const PERMISSIONS = {
  dados_sensiveis: ['administrador', 'rh'],
  salario: ['administrador', 'rh', 'gestor_confianca'],
  score_individual: ['administrador', 'rh', 'gestor_confianca'],
  atestado_individual: ['administrador', 'rh'],
  cid: ['administrador', 'rh'],
  editar_funcionario: ['administrador', 'rh'],
  registrar_acompanhamento: ['administrador', 'rh', 'gestor_confianca'],
  ver_integracao: ['administrador', 'rh'],
  ver_plano_acao: ['administrador', 'rh', 'gestor_confianca'],
  ver_motor_decisao: ['administrador', 'rh'],
  ver_auditoria: ['administrador', 'rh'],
};
```

### 18.2 RLS no Banco (rh_funcionarios)

| Operacao | Policy | Condicao |
|----------|--------|----------|
| SELECT | func_select | admin/rh/visualizador: todos. gestor: WHERE setor_id = setor_do_gestor OR gestor_id = func_atual |
| INSERT | func_insert | admin, rh |
| UPDATE | func_update | admin, rh |
| DELETE | func_delete | USING (FALSE) -- ninguem pode deletar |

### 18.3 RLS na View (rh_colaboradores)

- Grant SELECT para `authenticated`.
- Escrita passa pela tabela base (rh_funcionarios) e suas policies.

### 18.4 Permissoes na Sidebar (shell.js)

- Rota `colaboradores`: perfis `['all']` -- todos os perfis autenticados podem acessar.
- Rota `ficha_360`: perfis `['administrador', 'rh', 'gestor_confianca']`.

---

## 19. Auditoria

### 19.1 Registros de Auditoria no Banco

| Evento | Tabela Destino | Campos Registrados |
|--------|---------------|-------------------|
| INSERT em rh_funcionarios | rh_audit_log | tabela, registro_id, acao='INSERT', dados_novos (JSONB), usuario_id |
| UPDATE em rh_funcionarios | rh_audit_log | tabela, registro_id, acao='UPDATE', dados_antigos, dados_novos, campos_alterados (array), usuario_id |
| DELETE tentado em rh_funcionarios | rh_audit_log | tabela, registro_id, acao='DELETE_BLOCKED', dados_antigos, usuario_id |
| Mudanca de cargo | rh_hist_cargo | funcionario_id, cargo_id, vigencia_inicio, vigencia_fim |
| Mudanca de salario | rh_hist_salario | funcionario_id, salario_anterior, salario_novo, percentual_reajuste, vigencia_inicio |
| Mudanca de setor | rh_hist_setor | funcionario_id, setor_id, vigencia_inicio, vigencia_fim |
| Mudanca de gestor | rh_hist_gestor | funcionario_id, gestor_id, vigencia_inicio, vigencia_fim |

### 19.2 Registros de Auditoria no Frontend

| Evento | Tipo | Payload |
|--------|------|---------|
| Colaborador criado | colab_create | { nome, cpf } |
| Colaborador editado | colab_update | { nome, cpf } |
| Importacao em lote | colab_import | { total, inserted, errors } |
| Erro de API | API_ERROR | { endpoint, params, error } |
| Erro de UI | UI_ERROR | { page, error } |

---

## 20. Eventos

### 20.1 Eventos Gerados

| Evento | Origem | Consumidores |
|--------|--------|-------------|
| INSERT em rh_funcionarios | Cadastro/importacao | Triggers: hist_inicial, audit_trigger |
| UPDATE em rh_funcionarios | Edicao | Triggers: hist_cargo, hist_salario, hist_setor, hist_gestor, updated_at, audit_trigger |
| colab_create (log frontend) | colaboradores.html | Array _events em api.js (observabilidade) |
| colab_update (log frontend) | colaboradores.html | Array _events em api.js |
| colab_import (log frontend) | colaboradores.html | Array _events em api.js |

### 20.2 Eventos Consumidos

| Evento | Origem | Efeito em Colaboradores |
|--------|--------|------------------------|
| Sync Convenia | n8n workflow | Atualiza dados em rh_funcionarios |
| Import Secullum | importar_secullum.html | Vincula ponto por funcionario_id |
| Calculo Motor v2 | motor_v2.html | Le todos os funcionarios ativos para scoring |
| BuscaEPI sync | n8n workflow | Vincula entregas de EPI por funcionario_id |

---

## 21. Dependencias (Pre-Requisitos)

### 21.1 Para o Cadastro de Colaboradores Funcionar

| Dependencia | Tabela | Motivo |
|-------------|--------|--------|
| **Empresa cadastrada** | rh_empresas | FK obrigatoria empresa_id |
| **Filial cadastrada** | rh_filiais | FK obrigatoria filial_id |
| **Setor cadastrado e ativo** | rh_setores | FK obrigatoria setor_id; dropdown carrega setores ativos |
| **Cargo cadastrado** | rh_cargos | FK obrigatoria cargo_id |
| **Usuario autenticado** | rh_usuarios + auth.users | Sessao JWT valida |
| **Perfil com permissao** | rh_usuarios.perfil | admin ou rh para cadastro/edicao |

### 21.2 Para a Ficha Funcionar

| Dependencia | Motivo |
|-------------|--------|
| Funcionario existente (UUID valido) | Parametro da RPC |
| Motor v2 executado pelo menos uma vez | Dados de score |
| Ponto importado | Dados da aba Ponto |
| ASOs registrados | Dados da aba SST |
| Treinamentos registrados | Dados da aba SST |
| EPIs entregues registrados | Dados da aba SST |

---

## 22. Funcionalidades Relacionadas (Modulos Dependentes)

| Modulo | Dependencia | Tipo |
|--------|-------------|------|
| **Ocorrencias** (ocorrencias.html) | Referencia colaborador_id em rh_ocorrencias | FK |
| **Absenteismo** (absenteismo.html) | Analisa atestados por funcionario | Consulta |
| **Apuracao/Experiencia** (apuracao.html) | Verifica data_admissao para contratos de experiencia | Consulta |
| **Desligamentos/Turnover** (desligamentos_turnover.html) | Analisa status desligado e data_desligamento | Consulta |
| **ASOs** (asos.html) | Vincula ASOs por funcionario_id | FK |
| **Treinamentos** (treinamentos.html) | Vincula treinamentos por funcionario_id | FK |
| **EPIs** (epis.html) | Vincula entregas por funcionario_id | FK |
| **Acidentes** (acidentes.html) | Vincula acidentes por funcionario_id | FK |
| **Ponto** (importar_secullum.html) | Vincula registros por funcionario_id | FK |
| **Beneficios** (beneficios.html) | Vincula beneficios por funcionario_id | FK |
| **Ferias** (ferias.html) | Vincula ferias por funcionario_id | FK |
| **Motor v2 Score** (motor_v2.html) | Calcula score de cada funcionario ativo | Consulta |
| **Motor Decisoes** (motor_decisoes.html) | Analisa recorrencia por funcionario | Consulta |
| **Alertas RH** (alertas_rh.html) | Gera pendencias por funcionario | FK |
| **Armarios** (armarios.html) | Atribui armario por funcionario_id | FK |
| **Dashboard SST** (sst_dashboard.html) | Conta funcionarios sem ASO/EPI/treinamento | Consulta |
| **Dashboard Custos** (dashboard_custos.html) | Soma custos por funcionario | Consulta |
| **Organograma** (organograma.html) | Hierarquia via gestor_id | FK |
| **Recrutamento** (recrutamento_selecao.html) | Converte candidato em funcionario | Fluxo |
| **Restricoes Medicas** (restricoes_medicas.html) | Vincula restricoes por funcionario_id | FK |
| **DDS** (dds.html) | Registra participacao por funcionario | FK |
| **Ordem de Servico** (ordem_servico.html) | Ciencia de OS por funcionario_id | FK |

---

## 23. Melhorias para o SIGA

### M-01: Validacao de CPF com Algoritmo
- O legado valida apenas se tem 11 digitos. O SIGA deve validar CPF pelo algoritmo dos digitos verificadores (modulo 11).

### M-02: Cadastro em Etapas (Wizard)
- Substituir o modal unico por wizard multi-step:
  1. Dados Pessoais (nome, CPF, nascimento, sexo, estado civil, telefone, email, endereco)
  2. Dados Contratuais (empresa, filial, setor, cargo, tipo vinculo, modalidade, admissao, turno, carga horaria)
  3. Dados SST (GHE, grau risco, elegibilidade)
  4. Dados Complementares (matricula, PIS/PASEP, CTPS, observacoes)
  5. Revisao e confirmacao

### M-03: Importacao Inteligente (Upsert)
- O legado insere linha a linha com falha silenciosa para duplicados. O SIGA deve oferecer:
  - Deteccao de registros existentes (por CPF) com opcao de atualizar ou pular
  - Preview antes de importar (dry-run)
  - Importacao via upload de arquivo (nao apenas paste de CSV)
  - Suporte a XLSX alem de CSV
  - Mapeamento dinamico de colunas
  - Log detalhado de cada linha processada

### M-04: Foto do Colaborador
- Adicionar upload de foto 3x4 com armazenamento em object storage (S3/Supabase Storage).
- Exibir foto real no avatar da ficha ao inves de iniciais.

### M-05: Busca Avancada
- Adicionar busca por CPF, matricula, cargo (alem de nome).
- Filtros por data de admissao (periodo), faixa salarial, cargo, GHE.
- Busca full-text com ranking de relevancia.

### M-06: Paginacao Server-Side
- O legado carrega TODOS os colaboradores de uma vez (ate 629 registros). O SIGA deve implementar paginacao server-side com offset/limit para escalabilidade.

### M-07: Campos Pessoais Faltantes no Formulario
- O formulario legado nao inclui: data_nascimento, sexo, estado_civil, telefone, email, endereco completo, PIS/PASEP, CTPS. Esses dados existem no banco mas so sao editaveis por meios indiretos (integracao Convenia, SQL direto). O SIGA deve incluir todos no formulario.

### M-08: Vinculo Organizacional Completo
- O formulario legado usa setor como dropdown mas cargo como texto livre. O SIGA deve usar:
  - Empresa (select)
  - Filial (select, filtrado por empresa)
  - Setor (select, filtrado por filial)
  - Cargo (select, com CBO automatico)
  - Centro de custo (select)
  - Gestor direto (select de funcionarios)
  - Nivel hierarquico (derivado do cargo)

### M-09: Workflow de Admissao
- Integrar com modulo de Recrutamento: ao aprovar candidato, pre-preencher formulario de colaborador.
- Checklist de admissao: ASO admissional, integração SST, entrega de EPI, entrega de uniforme, treinamentos obrigatorios.

### M-10: Desligamento Estruturado
- O legado apenas altera status para "desligado". O SIGA deve ter workflow de desligamento:
  - Motivo do desligamento (iniciativa empresa/funcionario, justa causa, etc.)
  - Data do desligamento
  - Checklist de desligamento (devolucao de EPI, armario, cracha, entrevista de desligamento)
  - Calculo de verbas rescisorias (integracao com folha)
  - ASO demissional obrigatorio

### M-11: Historico com Motivo Obrigatorio
- Os triggers de historico aceitam motivo NULL. O SIGA deve exigir motivo para mudancas de cargo, setor e salario.

### M-12: Notificacoes Automaticas
- Ao cadastrar colaborador: notificar SST para agendar ASO admissional, notificar TI para criar acessos.
- Ao alterar status para afastado: notificar gestor e RH.
- Ao completar 45/90 dias (experiencia): notificar RH automaticamente.

---

## 24. Adequacao ao SIGA

### 24.1 Multiempresa
- **Legado**: A view rh_colaboradores derivava unidade (classic/apac) de filial_id, mas o formulario tinha select fixo "Classic/APAC".
- **SIGA**: Usar hierarquia completa Empresa -> Filial -> Setor. O usuario so ve colaboradores das filiais que tem acesso (rh_usuarios.filiais_acesso). Selects em cascata no formulario.

### 24.2 RBAC
- **Legado**: 4 perfis fixos (administrador, rh, gestor, visualizador) + gestor_confianca, definidos por CHECK constraint.
- **SIGA**: Manter perfis por funcao (nao por tela). Implementar permissoes granulares: `colaborador.criar`, `colaborador.editar`, `colaborador.ver_salario`, `colaborador.ver_cpf`, `colaborador.importar`, `colaborador.exportar`, `colaborador.ver_atestado`, etc. Permitir perfis custom.

### 24.3 Workflow
- **Legado**: Nenhum workflow formal. Cadastro e edicao sao imediatos (sem aprovacao).
- **SIGA**: Implementar workflow configuravel:
  - Cadastro de colaborador pode exigir aprovacao de gestor antes de efetivar
  - Alteracao de salario pode exigir aprovacao de diretoria
  - Desligamento pode exigir aprovacao de RH + diretoria
  - Status de cada etapa visivel no historico

### 24.4 Auditoria
- **Legado**: Trigger generico que registra INSERT/UPDATE em rh_audit_log + triggers especificos de historico (cargo, salario, setor, gestor). Frontend registra eventos em array em memoria (nao persistido).
- **SIGA**: Manter auditoria no banco (completa). Adicionar:
  - Auditoria de consulta (quem viu dados sensiveis)
  - Dashboard de auditoria (quem mudou o que, quando)
  - Exportacao de trilha de auditoria
  - Retencao configuravel (LGPD)

### 24.5 Event Bus
- **Legado**: Sem event bus. Triggers operam sincrona e localmente.
- **SIGA**: Implementar event bus assincrono para:
  - `colaborador.criado` -> SST agenda ASO admissional, TI cria acessos
  - `colaborador.editado.setor` -> Recalcula score, atualiza lotacao
  - `colaborador.editado.status` -> Notificacoes, checklists
  - `colaborador.editado.salario` -> Atualiza custos, notifica financeiro

### 24.6 APIs
- **Legado**: PostgREST (REST automatico do Supabase) + RPCs customizadas.
- **SIGA**: API REST versionada (v1/colaboradores) com:
  - GET /colaboradores (listagem paginada com filtros)
  - GET /colaboradores/:id (detalhe)
  - POST /colaboradores (criar)
  - PUT /colaboradores/:id (atualizar)
  - GET /colaboradores/:id/ficha-360 (ficha completa)
  - GET /colaboradores/:id/historico (timeline)
  - POST /colaboradores/importar (importacao em lote)
  - GET /colaboradores/exportar (exportacao)
  - Documentacao OpenAPI/Swagger

### 24.7 LGPD
- **Legado**: CPF mascarado na listagem (`***.***.XXX-XX`). Dados sensiveis (atestados, CID, salario) restritos por perfil. Nenhum mecanismo formal de consentimento ou portabilidade.
- **SIGA**: Implementar:
  - Consentimento de tratamento de dados no cadastro
  - Direito de acesso: colaborador pode solicitar extrato de seus dados
  - Direito de retificacao: solicitacao formal para correcao
  - Direito de eliminacao: anonimizacao apos prazo legal (5 anos trabalhista, 20 anos previdenciario)
  - Log de acesso a dados sensiveis
  - Classificacao de dados por sensibilidade (PII, dados de saude, dados financeiros)
  - Criptografia de CPF, PIS/PASEP no banco (at rest)

### 24.8 Rastreabilidade
- **Legado**: created_by (UUID do auth.users) em todas as tabelas + rh_audit_log.
- **SIGA**: Manter e expandir:
  - IP de origem em cada operacao
  - User-Agent
  - Sessao (session_id)
  - Motivo da alteracao (obrigatorio para campos criticos)
  - Hash de integridade do registro de auditoria (imutabilidade)

---

## 25. Criterios de Aceite

### CA-01: Listagem
- [ ] Ao acessar a tela, exibir KPIs: total ativos, total APAC, admitidos no mes, tempo medio empresa, quota Jovem Aprendiz.
- [ ] Tabela exibe colunas: Nome, CPF (mascarado), Setor, Cargo, Data Admissao, Status (badge), JA (badge), Unidade (badge).
- [ ] Filtro padrao carrega somente status ativo e experiencia.
- [ ] Busca por nome filtra em tempo real (client-side).
- [ ] Filtro por status recarrega dados do servidor.
- [ ] Filtro por cidade e unidade filtram client-side.
- [ ] Ordenacao por qualquer coluna clicavel (asc/desc).
- [ ] Contagem atualizada: "X colaborador(es) encontrado(s)".
- [ ] Estado vazio: "Nenhum colaborador encontrado" com icone.

### CA-02: Cadastro
- [ ] Botao "+ Novo Colaborador" visivel apenas para perfis admin e rh.
- [ ] Modal abre com campos em branco e valores padrao corretos.
- [ ] Validacao: nome obrigatorio, CPF 11 digitos, data admissao obrigatoria.
- [ ] CPF formatado com mascara em tempo real.
- [ ] Nome convertido para UPPERCASE ao salvar.
- [ ] Ao salvar com sucesso: toast positivo, modal fecha, lista recarrega.
- [ ] CPF duplicado: toast "CPF ja cadastrado no sistema".
- [ ] Evento colab_create registrado.

### CA-03: Edicao
- [ ] Click na linha da tabela abre modal preenchido com dados atuais.
- [ ] Titulo do modal muda para "Editar Colaborador".
- [ ] Mesma validacao do cadastro.
- [ ] Ao salvar com sucesso: toast positivo, modal fecha, lista recarrega.
- [ ] Evento colab_update registrado.
- [ ] Triggers de historico disparam para mudancas em cargo, salario, setor, gestor.

### CA-04: Importacao eSocial
- [ ] Botao "Importar eSocial" visivel apenas para perfis admin e rh.
- [ ] Modal aceita CSV com separador ponto-e-virgula.
- [ ] Cabecalho obrigatorio: nome, cpf, data_admissao.
- [ ] Linhas invalidas sao ignoradas silenciosamente.
- [ ] Estabelecimento APAC mapeado para unidade apac.
- [ ] Resultado exibido: inseridos e erros.
- [ ] Duplicados de CPF identificados como "CPF duplicado".
- [ ] Evento colab_import registrado.

### CA-05: Exportacao CSV
- [ ] Botao "Exportar CSV" disponivel para todos os perfis.
- [ ] Exporta somente dados filtrados atuais.
- [ ] CSV com 13 colunas, separador `;`, BOM UTF-8.
- [ ] Nome do arquivo: `colaboradores_YYYY-MM-DD.csv`.
- [ ] Lista vazia: toast "Nenhum dado para exportar".

### CA-06: Ficha Individual
- [ ] Acessivel via busca global no topbar (link para ficha.html?id=).
- [ ] Hero card exibe: avatar, nome, cargo, setor, badges de status.
- [ ] Score card exibe score final + 6 barras de dimensoes.
- [ ] KPIs: atestados, ponto, EPI, treinamentos.
- [ ] 7 tabs funcionais: Resumo, Atestados, Ponto, SST, Beneficios, Documentos, Pendencias.
- [ ] Aba Atestados restrita a perfis admin/rh.
- [ ] Botao "Imprimir" funcional.
- [ ] Erro de carregamento exibe estado de erro com link de volta.

### CA-07: Ficha 360
- [ ] Seletor de colaborador quando acessado sem ID.
- [ ] 8 tabs funcionais: Dados Gerais, RH, SST, Ponto, Beneficios, Documentos, Custos, Historico.
- [ ] Dados sensiveis (CPF, PIS, CTPS, salario, custos, ocorrencias) restritos por perfil no servidor.
- [ ] Score expandivel com detalhamento de 6 dimensoes.
- [ ] Timeline completa com eventos de admissao, mudancas de cargo/setor/salario, ASOs, treinamentos, ferias, acidentes, ocorrencias, desligamento.
- [ ] Armario vinculado exibido nos Dados Gerais.
- [ ] Alertas visuais para: colaborador em experiencia, colaborador afastado.

### CA-08: Seguranca
- [ ] DELETE fisico bloqueado por trigger.
- [ ] RLS aplicada em todas as operacoes.
- [ ] Permissoes verificadas em 3 niveis (frontend, RLS, API).
- [ ] CPF mascarado na listagem.
- [ ] Auditoria registrada para todas as operacoes.
- [ ] Sessao expirada redireciona para login (status 401).
- [ ] Acesso negado tratado (status 403).

---

## 26. Casos de Teste

### CT-01: Fluxo Feliz -- Cadastro de Colaborador
1. Login como perfil `rh`.
2. Acessar /rh/colaboradores.html.
3. Clicar "+ Novo Colaborador".
4. Preencher: Nome="JOAO SILVA", CPF="12345678901", Data Admissao="2026-07-27".
5. Clicar "Salvar".
6. **Esperado**: Toast "Colaborador cadastrado". Modal fecha. "JOAO SILVA" aparece na lista. KPI "Total Ativos" incrementa em 1.

### CT-02: Fluxo Feliz -- Edicao de Colaborador
1. Clicar na linha de "JOAO SILVA".
2. Alterar Setor para "Producao".
3. Clicar "Salvar".
4. **Esperado**: Toast "Colaborador atualizado". Setor na tabela muda para "Producao".

### CT-03: Erro -- CPF Duplicado
1. Cadastrar colaborador com CPF="12345678901" (ja existente).
2. **Esperado**: Toast "CPF ja cadastrado no sistema". Modal permanece aberto.

### CT-04: Erro -- Nome Vazio
1. Abrir modal de novo colaborador. Deixar nome vazio. Clicar "Salvar".
2. **Esperado**: Toast "Preencha o nome do colaborador".

### CT-05: Erro -- CPF Invalido
1. Preencher CPF com "123" (menos de 11 digitos). Clicar "Salvar".
2. **Esperado**: Toast "CPF invalido (11 digitos)".

### CT-06: Erro -- Data Admissao Vazia
1. Preencher nome e CPF. Deixar data admissao vazia. Clicar "Salvar".
2. **Esperado**: Toast "Preencha a data de admissao".

### CT-07: Permissao -- Perfil Visualizador
1. Login como `visualizador`.
2. Acessar /rh/colaboradores.html.
3. **Esperado**: Lista carrega normalmente. Botoes "+ Novo Colaborador" e "Importar eSocial" estao OCULTOS. "Exportar CSV" visivel.

### CT-08: Importacao eSocial -- Sucesso Total
1. Login como `rh`. Clicar "Importar eSocial".
2. Colar CSV valido com 3 linhas (novos CPFs).
3. Clicar "Importar".
4. **Esperado**: "Importacao concluida: 3 colaborador(es) inserido(s)."

### CT-09: Importacao eSocial -- Parcial
1. Colar CSV com 5 linhas, sendo 2 com CPFs ja existentes.
2. **Esperado**: "Inseridos: 3 | Erros: 2" com detalhe "(CPF duplicado)".

### CT-10: Importacao eSocial -- Cabecalho Invalido
1. Colar CSV sem coluna "cpf" no cabecalho.
2. **Esperado**: Toast "Cabecalho obrigatorio: nome, cpf, data_admissao".

### CT-11: Exportacao CSV
1. Filtrar por unidade "APAC".
2. Clicar "Exportar CSV".
3. **Esperado**: Download de arquivo `colaboradores_2026-07-27.csv` contendo somente colaboradores APAC. Arquivo abre corretamente no Excel (acentos preservados).

### CT-12: Exportacao CSV -- Lista Vazia
1. Filtrar por cidade inexistente (resultando em 0 registros).
2. Clicar "Exportar CSV".
3. **Esperado**: Toast "Nenhum dado para exportar". Nenhum download.

### CT-13: Filtro por Status
1. Selecionar "Todos (incl. Desligados)".
2. **Esperado**: Lista recarrega do servidor incluindo desligados. Contagem aumenta.

### CT-14: Busca por Nome
1. Digitar "SILVA" no campo de busca.
2. **Esperado**: Apenas colaboradores com "SILVA" no nome aparecem. Contagem atualizada.

### CT-15: Ordenacao
1. Clicar no header "Data Admissao".
2. **Esperado**: Tabela ordena por data ascendente. Seta aponta para cima.
3. Clicar novamente.
4. **Esperado**: Ordena descendente. Seta aponta para baixo.

### CT-16: Ficha -- Score Exibido
1. Acessar ficha de colaborador com score calculado.
2. **Esperado**: Score numerico exibido com cor (verde >= 80, azul >= 60, amarelo >= 40, vermelho < 40). 6 barras de dimensoes com cores proporcionais.

### CT-17: Ficha -- Atestados Restritos
1. Login como `gestor`. Acessar ficha de subordinado.
2. **Esperado**: Aba "Atestados" exibe mensagem "Dados restritos a RH".

### CT-18: Ficha 360 -- Custos Restritos
1. Login como `gestor`. Acessar ficha_360 de subordinado.
2. **Esperado**: Aba "Custos" exibe "Voce nao tem permissao para visualizar custos".

### CT-19: Ficha 360 -- Timeline Completa
1. Acessar ficha_360 de colaborador com historico longo.
2. **Esperado**: Timeline mostra eventos de admissao, mudancas de cargo/setor/salario, ASOs, treinamentos, ferias, acidentes, ocorrencias em ordem cronologica decrescente.

### CT-20: Concorrencia -- Edicao Simultanea
1. Dois usuarios abrem a ficha do mesmo colaborador.
2. Usuario A altera cargo e salva. Usuario B altera setor e salva.
3. **Esperado**: Ambas as alteracoes sao persistidas (ultimo write vence). Nao ha merge conflict (PATCH atualiza campos individuais). Historico registra ambas as mudancas.

### CT-21: Delete Bloqueado
1. Tentar DELETE direto via API (curl/Postman) em rh_funcionarios.
2. **Esperado**: Erro "DELETE nao permitido em rh_funcionarios. Use UPDATE status = 'desligado'". Tentativa registrada em rh_audit_log.

### CT-22: Sessao Expirada
1. Deixar sessao expirar (JWT invalido). Tentar acao.
2. **Esperado**: Status 401. Redirecionamento para login.html.

### CT-23: Ficha 360 -- Sem ID
1. Acessar ficha_360.html sem parametro id na URL.
2. **Esperado**: Exibe seletor de colaborador com busca. Ao selecionar, redireciona para ficha_360.html?id={uuid}.

### CT-24: KPI Jovem Aprendiz
1. Sistema com 133 ativos, 3 jovens aprendizes.
2. **Esperado**: KPI exibe "3 de 7-19" (ceil(133*0.05)=7, floor(133*0.15)=19).

### CT-25: Importacao -- Mapeamento APAC
1. CSV com estabelecimento="FAZENDA APAC".
2. **Esperado**: Colaborador criado com unidade="apac".

---

## 27. Casos Extremos

### CE-01: Colaborador sem Setor
- O formulario permite setor vazio (value=""), mas rh_funcionarios.setor_id e NOT NULL.
- **Comportamento legado**: O payload envia `setor_id: null` e `setor: null`, o que deveria causar erro 400 no PostgREST. Na pratica, a view rh_colaboradores pode ter INSTEAD OF trigger que trata isso.
- **SIGA**: Tornar setor obrigatorio no formulario ou garantir valor padrao.

### CE-02: Colaborador sem Cargo
- O campo cargo e texto livre no formulario legado. O payload envia `cargo: "texto"`, mas rh_funcionarios.cargo_id e NOT NULL.
- **Comportamento legado**: Mesmo problema do setor. A view abstrai, mas a tabela base requer FK.
- **SIGA**: Usar select de cargos cadastrados (nao texto livre).

### CE-03: CPF com Zeros a Esquerda
- CPF "00012345678" e valido. O `cpfFormat` e `cpfClean` lidam corretamente com zeros.
- Armazenamento: VARCHAR(11), `padStart(11, '0')` na exibicao.

### CE-04: Nome Muito Longo
- Sem limite de caracteres no campo nome (tipo TEXT no banco). Nomes com 100+ caracteres podem quebrar layout da tabela.
- **SIGA**: Definir limite maximo (ex: 200 caracteres) e truncar na exibicao em tabela.

### CE-05: Importacao de Milhares de Linhas
- A importacao e sequencial (1 POST por linha). Com 1000 linhas, pode levar minutos e travar a interface.
- **SIGA**: Usar bulk insert (batch de 100), progress bar, processamento assincrono.

### CE-06: Colaborador Desligado e Re-Admitido
- O sistema nao impede re-admissao (mesmo CPF). Porem, CPF e UNIQUE, entao nao pode existir 2 registros.
- **Comportamento legado**: Para re-admissao, seria necessario editar o registro existente (mudar status de desligado para ativo, atualizar data_admissao).
- **SIGA**: Criar mecanismo explicito de re-admissao que preserva historico anterior.

### CE-07: Salario Zero ou Negativo
- Campo numerico com `min=0` e `step=0.01`. Permite 0.00 (estagiarios?).
- Banco: `NUMERIC(12,2)` sem CHECK > 0.
- **SIGA**: Validar salario > 0 para CLT; aceitar 0 para estagiarios e aprendizes conforme tipo vinculo.

### CE-08: Data de Admissao no Futuro
- O input type=date nao impede datas futuras. O banco nao tem CHECK para isso.
- **SIGA**: Validar data_admissao <= data atual (ou permitir ate D+30 para admissoes programadas).

### CE-09: Mudanca de Status para "Desligado" sem Data de Desligamento
- O formulario permite mudar status para "desligado" sem preencher data de desligamento.
- Constraint `chk_datas_coerentes` nao impede desligamento sem data (so valida que se preenchida, >= admissao).
- **SIGA**: Exigir data_desligamento quando status = desligado; exigir motivo.

### CE-10: 629 Registros Carregados de Uma Vez
- A listagem carrega todos os 629 registros em uma unica chamada GET. Performance aceitavel atualmente, mas nao escala.
- **SIGA**: Paginacao server-side obrigatoria. Lazy loading para scroll infinito ou paginacao tradicional.

### CE-11: Perda de Conexao Durante Importacao
- Se a conexao cair no meio da importacao (ex: na linha 50 de 100), as primeiras 50 ja foram inseridas e nao ha rollback.
- **SIGA**: Importacao transacional (tudo ou nada) ou com checkpoint para retomada.

---

## 28. Melhorias Futuras

### MF-01: Portal do Colaborador (Self-Service)
- Permitir que o proprio colaborador acesse seus dados, solicite ferias, consulte contracheque, atualize endereco/telefone.

### MF-02: Integracao com eSocial Automatica
- Envio automatico de eventos S-2200 (admissao), S-2206 (alteracao contratual), S-2299 (desligamento) para o eSocial.

### MF-03: Assinatura Digital
- Assinatura eletronica (ICP-Brasil ou DocuSign) para contratos de trabalho, termos de entrega de EPI, ordens de servico.

### MF-04: QR Code no Cracha
- Gerar QR Code com UUID do colaborador para identificacao rapida em entrega de EPI, registro de treinamento, controle de acesso.

### MF-05: Dashboard de Diversidade
- Indicadores de diversidade por genero, idade, PCD, raca (quando aplicavel), para compliance e ESG.

### MF-06: Gestao de Documentos Completa
- Upload de documentos pessoais (RG, titulo, certificados) com OCR e validacao automatica de vencimento.

### MF-07: Integracao com Folha de Pagamento
- Sincronizacao bidirecional com sistema de folha para atualizar salarios, beneficios e descontos.

### MF-08: API Publica para Integracao
- API REST documentada (OpenAPI) para integracao com ERPs terceiros, BI, e-learning, etc.

### MF-09: Historico de Acesso (LGPD)
- Registrar quem acessou dados sensiveis de qual colaborador, quando e por quanto tempo.

### MF-10: Machine Learning para Predicao de Turnover
- Modelo preditivo baseado em score, atestados, tempo de empresa, historico salarial para identificar risco de desligamento.

### MF-11: Biometria
- Integracao com sistemas biometricos para controle de acesso e registro de ponto sem dependencia do Secullum.

### MF-12: Chat/Comunicacao Interna
- Canal direto entre RH e colaborador dentro do sistema para notificacoes, solicitacoes e acompanhamentos.

### MF-13: Multi-Idioma
- Suporte a espanhol e ingles para filiais internacionais (se houver expansao).

### MF-14: App Mobile
- Aplicativo mobile para gestores consultarem ficha de subordinados, aprovarem solicitacoes e receberem alertas.

### MF-15: Integracao com Medicina Ocupacional
- Integracao com clinicas de medicina do trabalho para agendamento e recebimento automatico de ASOs.

---

*Documento gerado em 27/07/2026. Baseado na analise completa do codigo-fonte legado (colaboradores.html, ficha.html, ficha_360.html, api.js, shell.js) e dos scripts de banco de dados (FUNDACAO_BANCO_v2.sql, VIEWS_FRONTEND_COMPATIBILIDADE.sql, FUNCOES_APLICACAO.sql).*
