# F-17 -- Gestao de EPIs

## Especificacao Funcional Completa

**Modulo:** SST -- Seguranca e Saude do Trabalho
**Sistema:** Classic RH & SST
**Empresa:** Classic Importacao e Exportacao de Couros EIRELI (CNPJ 08.849.964/0001-10)
**Versao:** 1.0
**Data:** 28/07/2026
**Autor:** Equipe Classic IA

---

## 1. Identificacao

| Campo                 | Valor                                                    |
|-----------------------|----------------------------------------------------------|
| Codigo                | F-17                                                     |
| Nome                  | Gestao de EPIs                                           |
| Modulo                | SST (Seguranca e Saude do Trabalho)                      |
| Submodulo             | Equipamentos de Protecao Individual                      |
| Norma regulamentadora | NR-6 (Equipamento de Protecao Individual)                |
| Paginas do sistema    | `rh/epis.html`, `rh/entrega_epi.html`, `rh/estoque_epi.html`, `rh/ordem_servico.html` |
| Banco de dados        | Supabase RH (`muiqmtnfvyffborgiwdw.supabase.co`)        |
| Empresa               | Classic Importacao e Exportacao de Couros EIRELI          |
| CNPJ                  | 08.849.964/0001-10                                       |
| Sede                  | Jardim Alegre - PR                                       |
| Funcionarios ativos   | 133 (10 afastados)                                       |
| Segmento              | Industria de couros (curtimento, acabamento, exportacao)  |
| Risco ocupacional     | Alto (agentes quimicos, ruido, maquinas de corte, calor) |

---

## 2. Problema

### 2.1 Contexto regulatorio

A NR-6 (Portaria MTE 3.214/78, ultima atualizacao Portaria SEPRT 11.437/2020) obriga o empregador a:

- Adquirir o EPI adequado ao risco de cada atividade;
- Exigir o uso do EPI;
- Fornecer ao trabalhador somente EPI aprovado pelo orgao nacional competente (CA -- Certificado de Aprovacao);
- Orientar e treinar o trabalhador sobre o uso adequado, guarda e conservacao;
- Registrar o fornecimento do EPI ao trabalhador mediante recibo (ficha de entrega);
- Responsabilizar-se pela higienizacao e manutencao periodica;
- Comunicar ao MTE qualquer irregularidade observada;
- Substituir imediatamente EPI danificado ou extraviado.

### 2.2 Problemas identificados antes da implantacao

1. **Controle manual em planilhas**: entregas de EPI eram registradas em planilhas Excel sem rastreabilidade, sem vinculo com o CA e sem controle de validade.
2. **Ausencia de ficha de entrega digital**: documentos comprobatorios de entrega dependiam de fichas em papel sujeitas a extravio.
3. **Estoque sem visibilidade**: nao havia controle centralizado de quantidades minimas, gerando rupturas de estoque.
4. **Ordens de Servico desvinculadas**: OS de SST eram documentos Word avulsos sem versionamento nem registro de ciencia.
5. **Integracao parcial com BuscaEPI**: dados de entregas no sistema BuscaEPI nao eram sincronizados com o ERP.
6. **Dificuldade em auditorias**: fiscalizacao trabalhista e auditorias internas exigiam compilacao manual de evidencias.
7. **Motor Score sem dados**: o calculo de score EPI no Motor v2 ficava prejudicado pela falta de registros digitais.

### 2.3 Impacto

- Multas de R$ 2.396,35 a R$ 6.708,08 por infracoes a NR-6 (valores 2026);
- Risco de interditor em caso de acidente sem EPI comprovado;
- Comprometimento do eSocial (evento S-2240 -- condicoes ambientais do trabalho);
- Score Motor v2 distorcido (eixo EPI com cobertura 0% para ~60% dos funcionarios).

---

## 3. Processo operacional

### 3.1 Catalogo de EPIs

1. Tecnico SST cadastra o item no catalogo (`sst_catalogo_epi`) com descricao, numero do CA, validade do CA, fabricante e categoria.
2. Sistema valida se CA esta dentro da validade.
3. Item fica disponivel para entregas e para vinculacao com cargos.

### 3.2 Matriz cargo x EPI

1. Tecnico SST vincula cada cargo da empresa aos EPIs obrigatorios (`sst_cargo_epi`).
2. Define quantidade padrao e periodicidade de substituicao em meses.
3. Essa matriz alimenta alertas automaticos de pendencia.

### 3.3 Registro de entrega

1. Responsavel (SST ou gestor) acessa `entrega_epi.html`.
2. Busca o funcionario pelo nome (autocomplete com filtro de ativos).
3. Sistema exibe informacoes do funcionario (nome, setor, cargo, ultimo EPI recebido).
4. Busca o EPI no catalogo (autocomplete por descricao ou CA).
5. Sistema exibe CA e validade do CA selecionado, com alerta visual se vencido.
6. Preenche quantidade (padrao: 1), motivo (primeira_entrega, substituicao, desgaste, perda, outro) e observacoes opcionais.
7. Clica em "Registrar Entrega".
8. Sistema grava registro em `sst_epi_entrega` e cria movimentacao de saida em `sst_estoque_movimentacao`.
9. Apos sucesso, exibe opcao "Imprimir Ficha de Entrega".

### 3.4 Controle de estoque

1. Responsavel acessa `estoque_epi.html`.
2. Visualiza posicao atual de estoque com quantidade atual, minima e status (OK / Baixo / Zerado).
3. Registra entradas (compras, reposicoes) e saidas (descartes, defeitos).
4. Sistema atualiza `sst_estoque_epi` e registra movimentacao.
5. KPIs de estoque alertam itens abaixo do minimo.

### 3.5 Ordens de Servico

1. Tecnico SST cria OS (`ordem_servico.html`) definindo: titulo, setor, cargo, atividade principal, riscos, medidas preventivas, EPIs obrigatorios, treinamentos, procedimentos de emergencia, regras de seguranca, responsavel tecnico.
2. OS passa pelo fluxo: Rascunho -> Em Revisao -> Aprovada -> Vigente.
3. Com status "Vigente", permite registrar ciencia de funcionarios (assinatura digital, fisica ou presencial).
4. OS pode ser substituida ou arquivada. Revisoes sao versionadas em `sst_os_revisao`.

### 3.6 Dashboard consolidado

1. Pagina `epis.html` consolida todos os dados em 6 abas: Catalogo, Entregas Recentes, Por Funcionario (com drill-down), Por Setor, Pendentes Assinatura e Ordens de Servico.
2. KPIs do topo: Catalogo Ativo, Total Entregas, Entregas 12m, Func. Ativos c/ EPI, Pend. Assinatura.

---

## 4. Usuarios

| Perfil          | Permissoes EPI                                                                 |
|-----------------|-------------------------------------------------------------------------------|
| administrador   | Acesso total: catalogo, entregas, estoque, OS, exclusao                       |
| rh              | Acesso total: catalogo, entregas, estoque, OS                                 |
| gestor          | Visualizacao de todos dados; registro de entregas para seus funcionarios       |
| visualizador    | Somente leitura em todas as abas                                              |
| funcionario     | Sem acesso direto (recebe EPI e assina ciencia de OS)                         |

### 4.1 Perfis e paginas

- `epis.html`: administrador, rh, gestor, visualizador (Shell.init controla via auth)
- `entrega_epi.html`: administrador, rh, gestor, visualizador (perfisPermitidos no RHAuth.init)
- `estoque_epi.html`: administrador, rh, gestor, visualizador (perfisPermitidos no RHAuth.init)
- `ordem_servico.html`: administrador, rh, gestor, visualizador (perfisPermitidos no RHAuth.init)

---

## 5. Fluxograma

### 5.1 Fluxo de entrega de EPI

```
[Inicio]
   |
   v
[Selecionar Funcionario] --> (autocomplete min 2 chars)
   |
   v
[Exibir info funcionario: nome, setor, cargo, ultimo EPI]
   |
   v
[Selecionar EPI do catalogo] --> (autocomplete por descricao/CA)
   |
   v
[CA vencido?] --Sim--> [Alerta visual "Vencido"] --> [Prosseguir?]
   |                                                      |Nao--> [Voltar]
   |Nao                                                   |Sim
   v                                                      v
[Preencher: Qtd, Motivo, Obs]
   |
   v
[Validar: funcId? epiId? qtd>0?] --Falha--> [Toast erro]
   |OK
   v
[POST sst_epi_entrega] --> [POST sst_estoque_movimentacao (saida)]
   |
   v
[Toast sucesso] --> [Exibir btn "Imprimir Ficha"]
   |
   v
[Limpar formulario] --> [Atualizar lista "Entregas Recentes"]
   |
   v
[Fim]
```

### 5.2 Fluxo de Ordem de Servico

```
[Criar OS (Rascunho)]
   |
   v
[Enviar para Revisao] --> status = em_revisao
   |
   v
[Aprovar] --> status = aprovada
   |
   v
[Tornar Vigente] --> status = vigente
   |                     |
   |                     v
   |              [Registrar Ciencia de funcionarios]
   |                     |
   |                     v
   |              [sst_os_ciencia: func, meio, obs]
   |
   v
[Substituir / Arquivar] --> status = substituida / arquivada
```

### 5.3 Fluxo de estoque

```
[Entrada]                           [Saida]
   |                                   |
   v                                   v
[Selecionar EPI]                 [Selecionar EPI]
   |                                   |
   v                                   v
[Qtd + Motivo + Resp]           [Qtd + Motivo + Resp]
   |                                   |
   v                                   v
[POST movimentacao]              [Validar estoque >= qtd]
   |                                   |Falha--> Toast "insuficiente"
   v                                   |OK
[PATCH estoque += qtd]           [POST movimentacao]
   |                                   |
   v                                   v
[Toast sucesso]                  [PATCH estoque -= qtd]
   |                                   |
   v                                   v
[Reload]                         [Toast sucesso] --> [Reload]
```

---

## 6. Regras de negocio

### Catalogo

| ID     | Regra                                                                                                  |
|--------|--------------------------------------------------------------------------------------------------------|
| RN-01  | Todo EPI cadastrado deve possuir descricao obrigatoria.                                               |
| RN-02  | O numero do CA (Certificado de Aprovacao) e opcional mas fortemente recomendado.                       |
| RN-03  | A validade do CA e verificada no frontend: se `ca_validade < hoje`, exibir badge "Vencido" em vermelho.|
| RN-04  | EPIs inativos (`ativo = false`) nao aparecem na selecao de entrega, mas permanecem no catalogo.        |
| RN-05  | Categoria e texto livre populado via dados existentes (sem enum fixo no banco).                        |

### Entrega

| ID     | Regra                                                                                                  |
|--------|--------------------------------------------------------------------------------------------------------|
| RN-06  | Selecao de funcionario e obrigatoria. Autocomplete ativa com minimo 2 caracteres.                     |
| RN-07  | Selecao de EPI e obrigatoria. Somente EPIs ativos sao exibidos na busca.                             |
| RN-08  | Quantidade minima de entrega e 1 (CHECK quantidade > 0 no banco).                                    |
| RN-09  | Motivo de entrega e obrigatorio, com valores: primeira_entrega, substituicao, desgaste, perda, outro. |
| RN-10  | Na tabela SQL, motivo aceita tambem `mudanca_funcao` (CHECK constraint inclui 5 valores).             |
| RN-11  | Data de entrega e preenchida automaticamente com a data atual (ISO).                                  |
| RN-12  | Ao registrar entrega, sistema automaticamente cria movimentacao de saida no estoque (`tipo=saida`, `motivo=entrega_funcionario`). |
| RN-13  | Observacoes sao opcionais (gravadas como null se vazio).                                              |
| RN-14  | Apos registro, lista de "Entregas Recentes" e recarregada (ultimas 30).                              |

### Estoque

| ID     | Regra                                                                                                  |
|--------|--------------------------------------------------------------------------------------------------------|
| RN-15  | Quantidade minima padrao por item e 5 unidades (DEFAULT no banco).                                    |
| RN-16  | Status do estoque: OK (qtd >= min), Baixo (0 < qtd < min), Zerado (qtd = 0).                        |
| RN-17  | Saida so e permitida se estoque atual >= quantidade solicitada.                                       |
| RN-18  | Tipos de movimentacao: entrada, saida, ajuste, devolucao.                                             |
| RN-19  | Motivos de saida (select): entrega, descarte, defeito.                                                |
| RN-20  | Se item nao existe em `sst_estoque_epi` ao registrar entrada, sistema cria registro com qtd_minima=0. |
| RN-21  | Localizacao do estoque e campo texto livre (ex: "Almoxarifado SST", "Galpao 2").                     |

### Ordem de Servico

| ID     | Regra                                                                                                  |
|--------|--------------------------------------------------------------------------------------------------------|
| RN-22  | Titulo da OS e obrigatorio.                                                                           |
| RN-23  | Status segue fluxo linear: rascunho -> em_revisao -> aprovada -> vigente -> substituida/arquivada.    |
| RN-24  | Somente OS com status "vigente" permite registro de ciencia.                                          |
| RN-25  | Riscos, medidas preventivas, EPIs obrigatorios e treinamentos sao armazenados como JSONB (arrays).   |
| RN-26  | Ciencia registra: funcionario, meio de assinatura (digital, fisico, presencial) e observacao.         |
| RN-27  | Versao da OS e inteiro auto-incrementado. Padrao = 1. Revisoes criam entrada em `sst_os_revisao`.    |
| RN-28  | Codigo da OS e texto livre (sem auto-geracao no banco atual).                                         |

### Dashboard consolidado (epis.html)

| ID     | Regra                                                                                                  |
|--------|--------------------------------------------------------------------------------------------------------|
| RN-29  | KPIs sao calculados preferencialmente via RPC `epi_kpis()` no servidor. Se RPC falhar, fallback client-side. |
| RN-30  | Aba "Entregas Recentes" carrega as ultimas 200 entregas (limit no GET).                               |
| RN-31  | Aba "Por Funcionario" usa dados agregados via RPC `epi_entregas_por_funcionario()`.                   |
| RN-32  | Drill-down de funcionario carrega entregas individuais sob demanda via API (sem limit).                |
| RN-33  | Aba "Por Setor" usa dados agregados via RPC `epi_entregas_por_setor()`.                               |
| RN-34  | Clicar em card de setor redireciona para aba "Por Funcionario" filtrada pelo setor.                   |
| RN-35  | Aba "Pendentes Assinatura" usa RPC `epi_pendentes_assinatura()` -- entregas sem assinatura.           |
| RN-36  | Aba "Ordens de Servico" usa RPC `epi_ordens_servico()` para listar OS com agregacao de ciencias.     |
| RN-37  | Filtro de filial (FilialContext) e aplicado quando disponivel, tanto em queries diretas quanto em RPCs.|

### Motor Score v2

| ID     | Regra                                                                                                  |
|--------|--------------------------------------------------------------------------------------------------------|
| RN-38  | Score EPI = peso 10% no score final do Motor v2.                                                      |
| RN-39  | Score EPI = 100 se funcionario tem >= 3 entregas nos ultimos 12 meses; proporcional se < 3.           |
| RN-40  | Cobertura EPI = 100% se ultima entrega < 180 dias; 50% se > 180 dias; 0% se nenhuma entrega.         |
| RN-41  | Fator "Sem registro de EPI" adicionado ao array de fatores quando cobertura = 0%.                     |

---

## 7. Campos

### 7.1 Tabela `sst_catalogo_epi`

| Campo       | Tipo         | Obrig. | Default             | Descricao                          |
|-------------|-------------|--------|---------------------|------------------------------------|
| id          | UUID        | Sim    | gen_random_uuid()   | PK                                 |
| descricao   | TEXT        | Sim    | --                  | Nome/descricao do EPI              |
| ca_numero   | TEXT        | Nao    | NULL                | Numero do CA (Certificado Aprovacao)|
| ca_validade | DATE        | Nao    | NULL                | Data de validade do CA             |
| fabricante  | TEXT        | Nao    | NULL                | Nome do fabricante                 |
| categoria   | TEXT        | Nao    | NULL                | Categoria (ex: luva, oculos, bota) |
| grupo_risco | TEXT        | Nao    | NULL                | Grupo de risco associado           |
| ativo       | BOOLEAN     | Sim    | TRUE                | Item ativo no catalogo             |
| created_at  | TIMESTAMPTZ | Sim    | NOW()               | Data de criacao                    |
| updated_at  | TIMESTAMPTZ | Sim    | NOW()               | Data de ultima atualizacao         |
| created_by  | UUID        | Nao    | NULL                | FK auth.users -- criador           |

### 7.2 Tabela `sst_epi_entrega`

| Campo              | Tipo         | Obrig. | Default             | Descricao                          |
|--------------------|-------------|--------|---------------------|------------------------------------|
| id                 | UUID        | Sim    | gen_random_uuid()   | PK                                 |
| funcionario_id     | UUID        | Sim    | --                  | FK rh_funcionarios                 |
| catalogo_epi_id    | UUID        | Sim    | --                  | FK sst_catalogo_epi                |
| data_entrega       | DATE        | Sim    | --                  | Data da entrega                    |
| data_devolucao     | DATE        | Nao    | NULL                | Data de devolucao (se aplicavel)   |
| quantidade         | INTEGER     | Sim    | 1                   | Quantidade entregue (CHECK > 0)    |
| motivo_entrega     | VARCHAR(30) | Nao    | 'primeira_entrega'  | Motivo (CHECK enum 5 valores)      |
| assinatura_digital | BOOLEAN     | Nao    | FALSE               | Se houve assinatura digital        |
| observacoes        | TEXT        | Nao    | NULL                | Observacoes gerais                 |
| created_at         | TIMESTAMPTZ | Sim    | NOW()               | Data de criacao do registro        |
| updated_at         | TIMESTAMPTZ | Sim    | NOW()               | Data de ultima atualizacao         |
| created_by         | UUID        | Nao    | NULL                | FK auth.users -- criador           |

**CHECK constraint motivo_entrega:**
`IN ('primeira_entrega','substituicao','perda','desgaste','mudanca_funcao')`

**Nota:** O frontend inclui 'outro' na lista de motivos, mas a constraint do banco nao inclui. Divergencia a resolver (ver CE-03).

### 7.3 Tabela `sst_cargo_epi`

| Campo              | Tipo         | Obrig. | Default             | Descricao                          |
|--------------------|-------------|--------|---------------------|------------------------------------|
| id                 | UUID        | Sim    | gen_random_uuid()   | PK                                 |
| cargo_id           | UUID        | Sim    | --                  | FK rh_cargos                       |
| catalogo_epi_id    | UUID        | Sim    | --                  | FK sst_catalogo_epi                |
| obrigatorio        | BOOLEAN     | Sim    | TRUE                | Se EPI e obrigatorio para o cargo  |
| quantidade_padrao  | INTEGER     | Sim    | 1                   | Qtd padrao de entrega              |
| periodicidade_meses| INTEGER     | Nao    | NULL                | Meses entre trocas                 |
| observacao         | TEXT        | Nao    | NULL                | Observacoes                        |
| created_at         | TIMESTAMPTZ | Sim    | NOW()               | Data de criacao                    |
| created_by         | UUID        | Nao    | NULL                | FK auth.users                      |

**UNIQUE constraint:** `(cargo_id, catalogo_epi_id)`

### 7.4 Tabela `sst_estoque_epi`

| Campo            | Tipo         | Obrig. | Default             | Descricao                          |
|------------------|-------------|--------|---------------------|------------------------------------|
| id               | UUID        | Sim    | gen_random_uuid()   | PK                                 |
| catalogo_epi_id  | UUID        | Sim    | --                  | FK sst_catalogo_epi                |
| quantidade_atual | INTEGER     | Sim    | 0                   | Quantidade atual em estoque        |
| quantidade_minima| INTEGER     | Nao    | 5                   | Quantidade minima (alerta)         |
| localizacao      | TEXT        | Nao    | NULL                | Local fisico do estoque            |
| ultima_entrada   | DATE        | Nao    | NULL                | Data da ultima entrada             |
| ultima_saida     | DATE        | Nao    | NULL                | Data da ultima saida               |
| created_at       | TIMESTAMPTZ | Nao    | NOW()               | Data de criacao                    |
| updated_at       | TIMESTAMPTZ | Nao    | NOW()               | Data de ultima atualizacao         |

### 7.5 Tabela `sst_estoque_movimentacao`

| Campo           | Tipo         | Obrig. | Default             | Descricao                          |
|-----------------|-------------|--------|---------------------|------------------------------------|
| id              | UUID        | Sim    | gen_random_uuid()   | PK                                 |
| catalogo_epi_id | UUID        | Sim    | --                  | FK sst_catalogo_epi                |
| tipo            | TEXT        | Sim    | --                  | Tipo: entrada, saida, ajuste, devolucao |
| quantidade      | INTEGER     | Sim    | --                  | Quantidade movimentada             |
| motivo          | TEXT        | Nao    | NULL                | Motivo da movimentacao             |
| entrega_id      | UUID        | Nao    | NULL                | FK sst_epi_entrega (se vinculada)  |
| responsavel     | TEXT        | Nao    | NULL                | Nome do responsavel                |
| created_at      | TIMESTAMPTZ | Nao    | NOW()               | Data/hora da movimentacao          |

### 7.6 Tabela `sst_ordem_servico`

| Campo                       | Tipo         | Obrig. | Default          | Descricao                          |
|-----------------------------|-------------|--------|------------------|------------------------------------|
| id                          | UUID        | Sim    | gen_random_uuid()| PK                                 |
| codigo                      | TEXT        | Nao    | NULL             | Codigo da OS (ex: OS-SST-001)      |
| titulo                      | TEXT        | Sim    | --               | Titulo descritivo da OS            |
| setor_id                    | UUID        | Nao    | NULL             | FK rh_setores                      |
| cargo_id                    | UUID        | Nao    | NULL             | FK rh_cargos                       |
| atividade_principal         | TEXT        | Nao    | NULL             | Descricao da atividade             |
| riscos                      | JSONB       | Nao    | '[]'             | Array de riscos identificados      |
| medidas_preventivas         | JSONB       | Nao    | '[]'             | Array de medidas preventivas       |
| epis_obrigatorios           | JSONB       | Nao    | '[]'             | Array de EPIs obrigatorios         |
| treinamentos_obrigatorios   | JSONB       | Nao    | '[]'             | Array de treinamentos obrigatorios |
| procedimentos_emergencia    | TEXT        | Nao    | NULL             | Procedimentos de emergencia        |
| regras_seguranca            | TEXT        | Nao    | NULL             | Regras gerais de seguranca         |
| versao                      | INTEGER     | Sim    | 1                | Versao do documento                |
| status                      | TEXT        | Sim    | 'rascunho'       | Status da OS (ver RN-23)           |
| data_emissao                | DATE        | Nao    | NULL             | Data de emissao                    |
| data_revisao                | DATE        | Nao    | NULL             | Data da ultima revisao             |
| data_vigencia_inicio        | DATE        | Nao    | NULL             | Inicio da vigencia                 |
| data_vigencia_fim           | DATE        | Nao    | NULL             | Fim da vigencia                    |
| responsavel_tecnico         | TEXT        | Nao    | NULL             | Nome do resp. tecnico              |
| responsavel_tecnico_registro| TEXT        | Nao    | NULL             | Registro profissional (CREA, CRM)  |
| aprovado_por                | TEXT        | Nao    | NULL             | Nome de quem aprovou               |
| created_at                  | TIMESTAMPTZ | Nao    | NOW()            | Data de criacao                    |
| updated_at                  | TIMESTAMPTZ | Nao    | NOW()            | Data de ultima atualizacao         |
| created_by                  | UUID        | Nao    | auth.uid()       | FK auth.users                      |

### 7.7 Tabela `sst_os_ciencia`

| Campo           | Tipo         | Obrig. | Default          | Descricao                          |
|-----------------|-------------|--------|------------------|------------------------------------|
| id              | UUID        | Sim    | gen_random_uuid()| PK                                 |
| os_id           | UUID        | Sim    | --               | FK sst_ordem_servico               |
| funcionario_id  | UUID        | Sim    | --               | FK rh_funcionarios                 |
| data_ciencia    | DATE        | Sim    | CURRENT_DATE     | Data da ciencia                    |
| meio_assinatura | TEXT        | Nao    | NULL             | digital, fisico, presencial        |
| observacao      | TEXT        | Nao    | NULL             | Observacao complementar            |
| created_at      | TIMESTAMPTZ | Nao    | NOW()            | Data de criacao do registro        |

### 7.8 Tabela `sst_os_revisao`

| Campo           | Tipo         | Obrig. | Default          | Descricao                          |
|-----------------|-------------|--------|------------------|------------------------------------|
| id              | UUID        | Sim    | gen_random_uuid()| PK                                 |
| os_id           | UUID        | Sim    | --               | FK sst_ordem_servico               |
| versao_anterior | INTEGER     | Sim    | --               | Versao antes da revisao            |
| versao_nova     | INTEGER     | Sim    | --               | Nova versao                        |
| motivo_revisao  | TEXT        | Sim    | --               | Motivo da revisao                  |
| alteracoes      | TEXT        | Nao    | NULL             | Descricao das alteracoes           |
| revisado_por    | TEXT        | Nao    | NULL             | Nome do revisor                    |
| created_at      | TIMESTAMPTZ | Nao    | NOW()            | Data da revisao                    |

### 7.9 Campos de filtro no frontend (nao persistidos)

| Pagina           | Campo              | Tipo    | Descricao                              |
|------------------|--------------------|---------|----------------------------------------|
| epis.html        | fCatBusca          | text    | Busca por descricao ou CA no catalogo  |
| epis.html        | fCatCategoria      | select  | Filtro por categoria                   |
| epis.html        | fCatAtivo          | select  | Filtro: Todos/Ativos/Inativos          |
| epis.html        | fEntBusca          | text    | Busca por EPI ou funcionario           |
| epis.html        | fEntDe             | date    | Data inicio (filtro entregas)          |
| epis.html        | fEntAte            | date    | Data fim (filtro entregas)             |
| epis.html        | fFuncBusca         | text    | Busca por nome do funcionario          |
| epis.html        | fFuncSetor         | select  | Filtro por setor                       |
| epis.html        | fFuncStatus        | select  | Apenas Ativos / Todos                 |
| entrega_epi.html | searchFunc         | text    | Autocomplete funcionario               |
| entrega_epi.html | searchEpi          | text    | Autocomplete EPI                       |
| entrega_epi.html | entregaQtd         | number  | Quantidade (min=1, default=1)          |
| entrega_epi.html | entregaMotivo      | select  | Motivo da entrega (5 opcoes)           |
| entrega_epi.html | entregaObs         | textarea| Observacoes                            |
| estoque_epi.html | searchEstoque      | text    | Busca por descricao no estoque         |
| estoque_epi.html | filterTipoMov      | select  | Filtro tipo movimentacao               |
| estoque_epi.html | entrada-epi        | select  | Selecao EPI para entrada               |
| estoque_epi.html | entrada-qtd        | number  | Quantidade entrada                     |
| estoque_epi.html | entrada-motivo     | text    | Motivo da entrada                      |
| estoque_epi.html | entrada-resp       | text    | Responsavel pela entrada               |
| estoque_epi.html | saida-epi          | select  | Selecao EPI para saida                 |
| estoque_epi.html | saida-motivo       | select  | Motivo saida (entrega/descarte/defeito)|
| estoque_epi.html | saida-qtd          | number  | Quantidade saida                       |
| estoque_epi.html | saida-resp         | text    | Responsavel pela saida                 |
| ordem_servico.html| searchOS           | text    | Busca por titulo/codigo/setor          |
| ordem_servico.html| filterStatus       | select  | Filtro status OS (6 opcoes)            |
| ordem_servico.html| os-titulo          | text    | Titulo da OS (obrigatorio)             |
| ordem_servico.html| os-setor           | select  | Setor da OS                            |
| ordem_servico.html| os-cargo           | select  | Cargo da OS                            |
| ordem_servico.html| os-atividade       | textarea| Atividade principal                    |
| ordem_servico.html| os-riscos          | textarea| Riscos (um por linha)                  |
| ordem_servico.html| os-medidas         | textarea| Medidas preventivas (um por linha)     |
| ordem_servico.html| os-epis            | textarea| EPIs obrigatorios (um por linha)       |
| ordem_servico.html| os-treinamentos    | textarea| Treinamentos (um por linha)            |
| ordem_servico.html| os-emergencia      | textarea| Procedimentos emergencia               |
| ordem_servico.html| os-regras          | textarea| Regras de seguranca                    |
| ordem_servico.html| os-responsavel     | text    | Responsavel tecnico                    |
| ordem_servico.html| os-registro        | text    | Registro profissional                  |
| ordem_servico.html| ciencia-func       | select  | Funcionario para ciencia               |
| ordem_servico.html| ciencia-meio       | select  | Meio assinatura (digital/fisico/presencial)|
| ordem_servico.html| ciencia-obs        | textarea| Observacao da ciencia                  |

---

## 8. Tabelas

| Tabela                     | Registros est. | Modulo | Descricao                              |
|----------------------------|---------------|--------|-----------------------------------------|
| sst_catalogo_epi           | ~100-200      | SST    | Catalogo de EPIs com CA                 |
| sst_epi_entrega            | ~41.000+      | SST    | Registro de cada entrega individual     |
| sst_cargo_epi              | ~200-400      | SST    | Matriz cargo x EPI obrigatorio          |
| sst_estoque_epi            | ~50-100       | SST    | Posicao de estoque por item             |
| sst_estoque_movimentacao   | ~5.000+       | SST    | Historico de entradas e saidas          |
| sst_ordem_servico          | ~20-50        | SST    | Ordens de Servico de SST               |
| sst_os_ciencia             | ~500-2000     | SST    | Registro de ciencia de OS               |
| sst_os_revisao             | ~10-30        | SST    | Historico de revisoes de OS             |
| rh_funcionarios            | 629           | RH     | Funcionarios (referenciada por entregas)|
| rh_setores                 | ~15-20        | RH     | Setores da empresa                      |
| rh_cargos                  | ~40-60        | RH     | Cargos da empresa                       |
| rh_colaboradores (view)    | 629           | RH     | View consolidada de funcionarios        |
| stg_buscaepi_entregas      | ~41.787       | ETL    | Staging de entregas vindas do BuscaEPI  |

---

## 9. RPCs (Remote Procedure Calls)

### 9.1 RPCs referenciadas no frontend (epis.html)

| RPC                              | Parametros         | Retorno                                        | Status      |
|----------------------------------|--------------------|------------------------------------------------|-------------|
| epi_kpis                         | p_filial_id (opt)  | {catalogo_ativo, total_entregas, entregas_12m, func_com_epi, pendentes_assinatura} | A implementar |
| epi_entregas_por_funcionario     | p_filial_id (opt)  | [{funcionario_id, total, ultima_entrega}]      | A implementar |
| epi_entregas_por_setor           | p_filial_id (opt)  | [{setor_id, setor_nome, total_entregas, total_funcionarios}] | A implementar |
| epi_pendentes_assinatura         | (nenhum)           | [{funcionario_nome, setor_nome, epi_descricao, ca, data_entrega, url_termo}] | A implementar |
| epi_ordens_servico               | (nenhum)           | [{codigo, titulo, setor_nome, cargo_nome, status, data_emissao, total_ciencias}] | A implementar |

### 9.2 RPCs existentes que consomem dados de EPI

| RPC                              | Uso de dados EPI                                            |
|----------------------------------|-------------------------------------------------------------|
| sst_dashboard_kpis               | Agrega catalogo_ativo, total_entregas, entregas_12m, func_com_epi, func_sem_epi |
| motor_score_executar             | Score EPI (peso 10%): conta entregas 12m e recencia 180d    |
| motor_decisao_diagnostico        | Diagnostica "sem EPI" como risco SST com impacto financeiro |
| ficha_completa_funcionario       | Agrega total_entregas, ultima_entrega, entregas_12m, lista recentes |
| painel_setor                     | Agrega com_epi por setor no bloco SST                       |

---

## 10. Functions (banco de dados)

| Funcao                           | Tipo      | Descricao                                              |
|----------------------------------|-----------|---------------------------------------------------------|
| sst_dashboard_kpis()             | RETURNS jsonb | KPIs consolidados de SST (inclui secao 'epis')      |
| motor_score_executar()           | RETURNS void  | Calcula score EPI para todos ativos                  |
| motor_decisao_diagnostico()      | RETURNS jsonb | Diagnostica riscos SST incluindo falta de EPI        |
| ficha_completa_funcionario()     | RETURNS jsonb | Ficha completa incluindo secao EPI                   |
| painel_setor()                   | RETURNS jsonb | Painel de setor com metricas SST/EPI                 |

**Nota:** As 5 RPCs da secao 9.1 referenciadas pelo frontend `epis.html` nao possuem definicao SQL nos scripts de migracao atuais. Sao chamadas pelo frontend e precisam ser implementadas.

---

## 11. Triggers

| Trigger                          | Tabela              | Evento           | Descricao                              |
|----------------------------------|---------------------|------------------|----------------------------------------|
| (A implementar) trg_estoque_mov  | sst_estoque_movimentacao | AFTER INSERT | Atualizar sst_estoque_epi.quantidade_atual automaticamente |
| (A implementar) trg_updated_at   | sst_catalogo_epi    | BEFORE UPDATE    | Atualizar campo updated_at             |
| (A implementar) trg_updated_at   | sst_epi_entrega     | BEFORE UPDATE    | Atualizar campo updated_at             |
| (A implementar) trg_updated_at   | sst_estoque_epi     | BEFORE UPDATE    | Atualizar campo updated_at             |
| (A implementar) trg_updated_at   | sst_ordem_servico   | BEFORE UPDATE    | Atualizar campo updated_at             |
| (A implementar) trg_os_versao    | sst_ordem_servico   | BEFORE UPDATE    | Auto-incrementar versao em revisoes    |

**Nota:** Atualmente a logica de atualizacao de estoque e feita no frontend (PATCH manual apos POST de movimentacao). Idealmente deveria ser um trigger de banco para garantir consistencia.

---

## 12. APIs (chamadas do frontend)

### 12.1 Pagina `epis.html`

| # | Metodo | Endpoint                                                              | Descricao                          |
|---|--------|-----------------------------------------------------------------------|-------------------------------------|
| 1 | GET    | `sst_catalogo_epi?select=id,descricao,ca_numero,ca_validade,categoria,fabricante,ativo&order=descricao` | Catalogo completo |
| 2 | GET    | `sst_epi_entrega?select=id,funcionario_id,catalogo_epi_id,data_entrega,quantidade,motivo_entrega&order=data_entrega.desc&limit=200` | Ultimas 200 entregas |
| 3 | GET    | `rh_funcionarios?select=id,nome_completo,setor_id,status&status=in.(ativo,afastado,ferias)&limit=2000` | Funcionarios ativos |
| 4 | GET    | `rh_setores?select=id,nome`                                          | Setores                            |
| 5 | RPC    | `epi_kpis` (POST com filtro filial)                                   | KPIs consolidados                  |
| 6 | GET    | `rpc/epi_entregas_por_funcionario`                                    | Agregacao por funcionario          |
| 7 | GET    | `rpc/epi_entregas_por_setor`                                          | Agregacao por setor                |
| 8 | GET    | `rpc/epi_pendentes_assinatura`                                        | Entregas pendentes de assinatura   |
| 9 | GET    | `rpc/epi_ordens_servico`                                              | OS com total de ciencias           |
|10 | GET    | `sst_epi_entrega?funcionario_id=eq.{id}&order=data_entrega.desc`      | Drill-down: entregas do funcionario|

### 12.2 Pagina `entrega_epi.html`

| # | Metodo | Endpoint                                                              | Descricao                          |
|---|--------|-----------------------------------------------------------------------|-------------------------------------|
| 1 | GET    | `rh_colaboradores?status=eq.ativo&select=id,nome,setor,cargo&order=nome` | Lista funcionarios ativos       |
| 2 | GET    | `sst_catalogo_epi?ativo=eq.true&select=id,descricao,ca_numero,ca_validade&order=descricao` | EPIs ativos |
| 3 | GET    | `sst_epi_entrega?order=data_entrega.desc&limit=30`                    | Ultimas 30 entregas (recentes)     |
| 4 | GET    | `sst_epi_entrega?funcionario_id=eq.{id}&order=data_entrega.desc&limit=1` | Ultimo EPI do funcionario       |
| 5 | POST   | `sst_epi_entrega`                                                     | Registrar entrega                  |
| 6 | POST   | `sst_estoque_movimentacao`                                            | Registrar saida de estoque         |

### 12.3 Pagina `estoque_epi.html`

| # | Metodo | Endpoint                                                              | Descricao                          |
|---|--------|-----------------------------------------------------------------------|-------------------------------------|
| 1 | GET    | `sst_catalogo_epi?ativo=eq.true&order=descricao`                     | Catalogo ativo                     |
| 2 | GET    | `sst_estoque_epi?select=*,sst_catalogo_epi(descricao,ca_numero)&order=quantidade_atual` | Estoque com join |
| 3 | GET    | `sst_estoque_movimentacao?select=*,sst_catalogo_epi(descricao)&order=created_at.desc&limit=50` | Movimentacoes |
| 4 | POST   | `sst_estoque_movimentacao`                                            | Registrar movimentacao             |
| 5 | PATCH  | `sst_estoque_epi?id=eq.{id}`                                         | Atualizar quantidade atual         |
| 6 | POST   | `sst_estoque_epi`                                                     | Criar item de estoque (se novo)    |

### 12.4 Pagina `ordem_servico.html`

| # | Metodo | Endpoint                                                              | Descricao                          |
|---|--------|-----------------------------------------------------------------------|-------------------------------------|
| 1 | GET    | `sst_ordem_servico?order=created_at.desc`                             | Lista todas as OS                  |
| 2 | GET    | `rh_setores?ativo=eq.true&order=nome`                                | Setores ativos                     |
| 3 | GET    | `rh_cargos?order=nome`                                               | Lista de cargos                    |
| 4 | GET    | `rh_colaboradores?status=eq.ativo&select=id,nome,setor,cargo&order=nome` | Funcionarios para ciencia       |
| 5 | POST   | `sst_ordem_servico`                                                   | Criar nova OS                      |
| 6 | PATCH  | `sst_ordem_servico?id=eq.{id}`                                       | Atualizar status da OS             |
| 7 | GET    | `sst_os_ciencia?os_id=eq.{id}&order=created_at.desc`                 | Ciencias de uma OS                 |
| 8 | POST   | `sst_os_ciencia`                                                      | Registrar ciencia                  |

---

## 13. Integracoes

### 13.1 BuscaEPI (REST API via n8n)

| Atributo          | Valor                                              |
|-------------------|-----------------------------------------------------|
| Sistema           | BuscaEPI                                             |
| Tipo              | REST API                                             |
| Orquestracao      | n8n (workflows automatizados)                        |
| Status            | Parcial 60%                                          |
| Heartbeat         | Verificar n8n (ultimo registro ~5 dias atras)        |
| Tabela staging    | stg_buscaepi_entregas (~41.787 registros conhecidos) |
| Direcao           | BuscaEPI -> Classic RH (importacao)                  |
| Dados importados  | Entregas de EPI historicas                           |
| Pendencias        | Heartbeat inativo; sincronizacao intermitente        |

### 13.2 eSocial (evento S-2240)

| Atributo          | Valor                                              |
|-------------------|-----------------------------------------------------|
| Evento            | S-2240 -- Condicoes Ambientais do Trabalho           |
| Dados EPI         | EPIs obrigatorios por funcao/setor                   |
| Tabela            | esocial_s2240, esocial_eventos                       |
| Status            | Monitor eSocial implementado (`monitor-esocial.html`)|
| Impacto           | Falta de registro de EPI gera inconsistencia no S-2240|

### 13.3 Motor Score v2

| Atributo          | Valor                                              |
|-------------------|-----------------------------------------------------|
| Funcao            | motor_score_executar()                               |
| Peso EPI          | 10% do score final                                   |
| Fonte de dados    | sst_epi_entrega (ultimos 12 meses e 180 dias)       |
| Saida             | motor_score.score_epi, motor_score.cobertura_epi     |

### 13.4 Motor de Decisao

| Atributo          | Valor                                              |
|-------------------|-----------------------------------------------------|
| Funcao            | motor_decisao_diagnostico()                          |
| Tipo alerta       | sst_epi (prioridade 2, risco alto)                   |
| Condicao          | Carga BuscaEPI >= 80% E funcionarios sem EPI > 5     |
| Impacto financeiro| Estimativa: N funcionarios x R$ 3.000                |

---

## 14. Dashboards

### 14.1 Dashboard EPIs (`epis.html`)

#### KPIs (barra superior, 5 cards clicaveis)

| KPI                | Fonte                        | Acao ao clicar          |
|--------------------|------------------------------|-------------------------|
| Catalogo Ativo     | epi_kpis().catalogo_ativo    | Vai para aba Catalogo   |
| Total Entregas     | epi_kpis().total_entregas    | Vai para aba Entregas   |
| Entregas 12m       | epi_kpis().entregas_12m      | Vai para aba Entregas   |
| Func. Ativos c/ EPI| epi_kpis().func_com_epi      | Vai para aba Funcionario|
| Pend. Assinatura   | epi_kpis().pendentes_assinatura | Vai para aba Pendentes|

#### 6 Abas

| Aba                | Conteudo                                                |
|--------------------|---------------------------------------------------------|
| Catalogo           | Tabela: Descricao, CA, Validade CA, Fabricante, Categoria, Status. Filtros: busca, categoria, status. |
| Entregas Recentes  | Tabela: Data, EPI, CA, Funcionario, Setor, Qtd, Motivo. Filtros: busca, data de/ate. Limite: 200. |
| Por Funcionario    | Tabela: Funcionario, Setor, Total EPIs, Ultima Entrega. Filtros: busca, setor, status. Drill-down com entregas. |
| Por Setor          | Grid de cards: Setor, Total EPIs, Funcionarios. Clique navega para aba Funcionario filtrada. |
| Pend. Assinatura   | Tabela: Funcionario, Setor, EPI, CA, Data Entrega, Acao (link assinar). |
| Ordens de Servico  | Tabela: Codigo, Titulo, Setor, Cargo, Status, Emissao, Ciencias. |

### 14.2 Estoque EPI (`estoque_epi.html`)

#### KPIs (4 cards)

| KPI                | Fonte                              |
|--------------------|------------------------------------|
| Itens em Estoque   | COUNT(sst_estoque_epi)             |
| Abaixo do Minimo   | COUNT(qtd_atual < qtd_minima)      |
| Ultima Entrada     | MAX(created_at) WHERE tipo=entrada |
| Ultima Saida       | MAX(created_at) WHERE tipo=saida   |

### 14.3 Ordens de Servico (`ordem_servico.html`)

#### KPIs (4 cards)

| KPI               | Fonte                                 |
|-------------------|---------------------------------------|
| Total OS          | COUNT(sst_ordem_servico)              |
| Vigentes          | COUNT WHERE status=vigente            |
| Rascunho          | COUNT WHERE status=rascunho           |
| Func. sem Ciencia | (a calcular por OS vigente)           |

---

## 15. Relatorios

### 15.1 Relatorio de entregas por funcionario

- **Fonte:** Drill-down na aba "Por Funcionario" de `epis.html`
- **Campos:** Data, EPI, CA, Quantidade, Motivo
- **Filtros:** Nome do funcionario, setor, status
- **Formato:** Tela (tabela expandivel)

### 15.2 Relatorio de entregas por setor

- **Fonte:** Aba "Por Setor" de `epis.html`
- **Campos:** Setor, Total EPIs, Total Funcionarios
- **Formato:** Cards com metricas agregadas

### 15.3 Relatorio de estoque

- **Fonte:** Aba "Estoque Atual" de `estoque_epi.html`
- **Campos:** EPI, CA, Quantidade, Minimo, Status, Localizacao
- **Formato:** Tabela com destaque visual para itens em alerta

### 15.4 Relatorio de movimentacoes

- **Fonte:** Aba "Movimentacoes" de `estoque_epi.html`
- **Campos:** Data, EPI, Tipo, Quantidade, Motivo, Responsavel
- **Filtro:** Tipo de movimentacao
- **Formato:** Tabela (ultimas 50)

### 15.5 Ficha de Entrega (impressao)

- **Fonte:** Botao "Imprimir Ficha de Entrega" em `entrega_epi.html`
- **Formato:** Impressao via `window.print()` (CSS @media print)
- **Uso:** Documento comprobatorio para arquivo fisico

---

## 16. Documentos

### 16.1 Ficha de Entrega de EPI (NR-6)

- **Obrigatoriedade:** NR-6 exige registro de fornecimento mediante recibo
- **Campos da ficha:** Nome funcionario, cargo, setor, EPI entregue, CA, quantidade, data, motivo, assinatura
- **Geracao:** Botao "Imprimir Ficha" apos registro de entrega
- **Arquivo:** Deve ser mantido enquanto durar a relacao empregaticia + 20 anos (prescricao trabalhista)

### 16.2 Ordem de Servico (NR-1, art. 1.7)

- **Obrigatoriedade:** NR-1 exige que o empregador elabore OS sobre seguranca e saude
- **Campos:** Titulo, setor, cargo, atividade, riscos, medidas, EPIs, treinamentos, procedimentos emergencia, regras, responsavel
- **Ciencia:** Funcionario deve tomar ciencia dos riscos e EPIs, com registro
- **Versionamento:** Cada revisao gera nova versao com historico

---

## 17. Evidencias

### 17.1 Evidencias de conformidade NR-6

| Evidencia                          | Tabela                    | Campo/metrica                     |
|------------------------------------|---------------------------|-----------------------------------|
| Registro de entrega de EPI         | sst_epi_entrega           | data_entrega, funcionario_id      |
| CA valido no momento da entrega    | sst_catalogo_epi          | ca_validade >= data_entrega       |
| Assinatura do funcionario          | sst_epi_entrega           | assinatura_digital                |
| Termo pendente de assinatura       | RPC epi_pendentes_assinatura | url_termo                      |
| EPI obrigatorio por cargo          | sst_cargo_epi             | obrigatorio = true                |
| Funcionarios sem EPI               | Motor Score               | cobertura_epi = 0                 |
| Estoque adequado                   | sst_estoque_epi           | quantidade_atual >= quantidade_minima |
| OS com ciencia registrada          | sst_os_ciencia            | data_ciencia, meio_assinatura     |

### 17.2 Evidencias para eSocial

| Evento  | Dado EPI necessario                                    |
|---------|--------------------------------------------------------|
| S-2240  | EPIs fornecidos por funcao/ambiente (sst_cargo_epi)    |
| S-2210  | Uso de EPI no momento do acidente (sst_epi_entrega)    |

---

## 18. Permissoes

### 18.1 Permissoes por perfil (RHAuth)

| Acao                            | administrador | rh  | gestor | visualizador |
|---------------------------------|:---:|:---:|:---:|:---:|
| Visualizar catalogo             | Sim | Sim | Sim | Sim |
| Cadastrar/editar catalogo       | Sim | Sim | Nao | Nao |
| Registrar entrega               | Sim | Sim | Sim | Nao |
| Visualizar entregas             | Sim | Sim | Sim | Sim |
| Registrar movimentacao estoque  | Sim | Sim | Nao | Nao |
| Visualizar estoque              | Sim | Sim | Sim | Sim |
| Criar OS                        | Sim | Sim | Nao | Nao |
| Alterar status OS               | Sim | Sim | Nao | Nao |
| Registrar ciencia               | Sim | Sim | Sim | Nao |
| Visualizar OS                   | Sim | Sim | Sim | Sim |
| Imprimir ficha de entrega       | Sim | Sim | Sim | Nao |

### 18.2 RLS (Row Level Security)

**Status atual:** Nenhuma tabela de EPI possui RLS configurada nos scripts de migracao atuais. As tabelas `sst_catalogo_epi`, `sst_epi_entrega`, `sst_estoque_epi`, `sst_estoque_movimentacao`, `sst_cargo_epi`, `sst_ordem_servico`, `sst_os_ciencia` e `sst_os_revisao` dependem exclusivamente da autenticacao via JWT (Supabase Auth) e controle de perfil no frontend (RHAuth).

**Recomendacao:** Implementar RLS para garantir que dados sensiveis de EPI (especialmente `sst_epi_entrega`) sejam protegidos a nivel de banco. Perfis `visualizador` e `gestor` devem ter acesso somente a dados de seus setores.

---

## 19. Auditoria

### 19.1 Campos de auditoria por tabela

| Tabela                     | created_at | updated_at | created_by |
|----------------------------|:---:|:---:|:---:|
| sst_catalogo_epi           | Sim | Sim | Sim |
| sst_epi_entrega            | Sim | Sim | Sim |
| sst_cargo_epi              | Sim | Nao | Sim |
| sst_estoque_epi            | Sim | Sim | Nao |
| sst_estoque_movimentacao   | Sim | Nao | Nao |
| sst_ordem_servico          | Sim | Sim | Sim |
| sst_os_ciencia             | Sim | Nao | Nao |
| sst_os_revisao             | Sim | Nao | Nao |

### 19.2 Lacunas de auditoria

- `sst_estoque_epi` nao registra `created_by` -- impossivel rastrear quem criou item de estoque.
- `sst_estoque_movimentacao` nao registra `created_by` -- depende do campo texto `responsavel`.
- `sst_os_ciencia` nao registra quem registrou a ciencia (apenas quem tomou ciencia via `funcionario_id`).
- Nao ha tabela de log de auditoria (audit trail) para alteracoes em registros existentes.

---

## 20. Eventos

### 20.1 Eventos do frontend

| Evento                        | Pagina              | Descricao                              |
|-------------------------------|---------------------|----------------------------------------|
| Tab switch                    | epis.html           | Navegacao entre 6 abas via click       |
| KPI click                     | epis.html           | Clique no KPI navega para aba correspondente |
| Drill-down toggle             | epis.html           | Expandir/colapsar entregas de funcionario |
| Drill setor                   | epis.html           | Clicar em card de setor filtra aba Funcionario |
| Search autocomplete (func)    | entrega_epi.html    | Input com min 2 chars dispara busca    |
| Search autocomplete (epi)     | entrega_epi.html    | Input com min 2 chars dispara busca    |
| Dropdown select               | entrega_epi.html    | Selecao em dropdown customizado        |
| Dropdown close (outside click)| entrega_epi.html    | Fechar dropdown ao clicar fora         |
| Registrar entrega             | entrega_epi.html    | Submit do formulario de entrega        |
| Print                         | entrega_epi.html    | Impressao de ficha via window.print()  |
| Registrar entrada             | estoque_epi.html    | Submit de entrada no estoque           |
| Registrar saida               | estoque_epi.html    | Submit de saida do estoque             |
| Nova OS                       | ordem_servico.html  | Abertura de modal de criacao           |
| Salvar OS                     | ordem_servico.html  | Submit de criacao de OS                |
| Abrir detalhe OS              | ordem_servico.html  | Click em linha ou botao "Ver"          |
| Mudar status OS               | ordem_servico.html  | Botoes de transicao de status          |
| Registrar ciencia             | ordem_servico.html  | Submit de registro de ciencia          |
| Modal open/close              | ordem_servico.html  | 3 modais: Nova OS, Detalhe, Ciencia    |
| Filter input                  | todas               | Filtros de busca em tempo real (oninput)|
| Filter select change          | todas               | Filtros de select em tempo real        |

---

## 21. Dependencias

### 21.1 Dependencias de dados

| Dependencia                  | Tabela/View         | Direcao              |
|------------------------------|---------------------|----------------------|
| rh_funcionarios              | sst_epi_entrega.funcionario_id | FK obrigatoria |
| rh_funcionarios              | sst_os_ciencia.funcionario_id  | FK obrigatoria |
| rh_setores                   | sst_ordem_servico.setor_id     | FK opcional    |
| rh_cargos                    | sst_ordem_servico.cargo_id     | FK opcional    |
| rh_cargos                    | sst_cargo_epi.cargo_id         | FK obrigatoria |
| sst_catalogo_epi             | sst_epi_entrega.catalogo_epi_id| FK obrigatoria |
| sst_catalogo_epi             | sst_cargo_epi.catalogo_epi_id  | FK obrigatoria |
| sst_catalogo_epi             | sst_estoque_epi.catalogo_epi_id| FK logica      |
| sst_catalogo_epi             | sst_estoque_movimentacao.catalogo_epi_id | FK logica |
| sst_ordem_servico            | sst_os_ciencia.os_id           | FK obrigatoria |
| sst_ordem_servico            | sst_os_revisao.os_id           | FK obrigatoria |
| auth.users                   | sst_catalogo_epi.created_by    | FK opcional    |
| auth.users                   | sst_ordem_servico.created_by   | FK via auth.uid() |
| rh_colaboradores (view)      | entrega_epi.html, ordem_servico.html | Busca de ativos |

### 21.2 Dependencias de modulo

| Modulo          | Dependencia                                             |
|-----------------|---------------------------------------------------------|
| Shell.js        | epis.html usa Shell.init() para sidebar e topbar        |
| RHAuth          | entrega_epi, estoque_epi, ordem_servico usam RHAuth.init() |
| API.js          | Todas as paginas usam API.get, API.post, API.patch, API.rpc |
| FilialContext   | epis.html verifica e aplica filtro de filial            |
| ds.css          | Design system (unico CSS para todas as paginas)         |
| ui_components.js| Componentes reutilizaveis (badges, toast, etc.)         |

---

## 22. Funcionalidades relacionadas

| Funcionalidade           | Pagina                      | Relacao com F-17                           |
|--------------------------|-----------------------------|--------------------------------------------|
| F-07 Dashboard SST       | sst_dashboard.html          | KPIs de EPI alimentam dashboard SST        |
| F-10 ASOs                | asos.html                   | ASO pode indicar necessidade de EPI especifico |
| F-11 Treinamentos        | treinamentos.html           | Treinamento NR-6 vinculado a entrega de EPI|
| F-12 Acidentes           | acidentes.html              | Investigacao de acidente verifica uso de EPI|
| F-08 Motor Score v2      | motor_v2.html               | Score EPI (10% do score final)             |
| F-09 Motor Decisao       | motor_decisoes.html         | Diagnostico "sem EPI" gera alerta          |
| F-03 Colaboradores       | colaboradores.html          | Cadastro base de funcionarios              |
| F-22 eSocial             | monitor-esocial.html        | Evento S-2240 referencia EPIs fornecidos   |
| F-15 Restricoes Medicas  | restricoes_medicas.html     | Restricao pode exigir EPI adicional        |
| F-14 CIPA                | cipa.html                   | Inspecoes CIPA verificam uso de EPI        |
| F-16 Ficha Funcionario   | (RPC ficha_completa)        | Secao EPI na ficha do funcionario          |
| F-20 Integracoes         | integracoes.html            | Status da integracao BuscaEPI              |

---

## 23. Melhorias SIGA

> Melhorias identificadas para alinhamento com boas praticas de sistemas integrados de gestao.

| ID      | Melhoria                                                                                |
|---------|----------------------------------------------------------------------------------------|
| SIGA-01 | Implementar auto-geracao de codigo sequencial para OS (ex: OS-SST-2026-001)            |
| SIGA-02 | Adicionar campo `lote` e `nota_fiscal` na entrada de estoque para rastreabilidade      |
| SIGA-03 | Criar workflow de aprovacao de entrega para EPIs de alto custo (> R$ 100)               |
| SIGA-04 | Implementar alerta automatico quando CA estiver a 30 dias de vencer                    |
| SIGA-05 | Vincular entrega de EPI ao treinamento NR-6 -- nao permitir entrega sem treinamento    |
| SIGA-06 | Gerar PPP (Perfil Profissiografico Previdenciario) a partir dos dados de EPI e OS      |
| SIGA-07 | Dashboard de custo de EPI por setor/funcionario (integrado com financeiro)              |
| SIGA-08 | Notificacao push para gestor quando funcionario de seu setor nao tem EPI nos ultimos 6m|

---

## 24. Adequacao SIGA

> Avaliacao da aderencia atual ao modelo SIGA (Sistema Integrado de Gestao Administrativa).

| Criterio                            | Status           | Observacao                                  |
|--------------------------------------|-----------------|---------------------------------------------|
| Catalogo centralizado de EPIs        | Implementado    | sst_catalogo_epi com CA e validade          |
| Registro digital de entregas         | Implementado    | sst_epi_entrega com todos campos NR-6       |
| Controle de estoque com minimos      | Implementado    | sst_estoque_epi com alerta visual           |
| Rastreabilidade de movimentacoes     | Implementado    | sst_estoque_movimentacao com tipo e motivo  |
| Matriz cargo x EPI                   | Esquema pronto  | sst_cargo_epi existe; tela de gestao ausente|
| Ordens de Servico versionadas        | Implementado    | sst_os_revisao com historico                |
| Registro de ciencia de OS            | Implementado    | sst_os_ciencia com meio de assinatura       |
| Integracao com score operacional     | Implementado    | Motor v2 usa dados de EPI (peso 10%)        |
| Integracao com eSocial               | Parcial         | S-2240 estruturado; vinculo EPI manual      |
| RLS no banco de dados                | Ausente         | Nenhuma tabela de EPI tem RLS               |
| Auditoria completa (audit trail)     | Parcial         | created_at/by existe; falta log de alteracoes|
| Alertas automaticos de vencimento    | Ausente         | Verificacao apenas visual no frontend       |
| Ficha de entrega digital assinada    | Parcial         | Campo assinatura_digital existe; fluxo nao  |
| Relatorios exportaveis (PDF/Excel)   | Ausente         | Somente impressao via window.print()        |

---

## 25. Criterios de aceite

| ID    | Criterio                                                                                           |
|-------|---------------------------------------------------------------------------------------------------|
| CA-01 | Usuario com perfil `rh` ou `administrador` consegue cadastrar novo EPI no catalogo com descricao, CA, validade, fabricante e categoria. |
| CA-02 | EPI com CA vencido exibe badge "Vencido" em vermelho na tabela do catalogo.                       |
| CA-03 | EPIs inativos nao aparecem nas listas de selecao para entrega.                                    |
| CA-04 | Usuario consegue buscar funcionario por nome (autocomplete com min 2 chars) na tela de entrega.   |
| CA-05 | Ao selecionar funcionario, sistema exibe nome, setor, cargo e ultimo EPI recebido.                |
| CA-06 | Usuario consegue buscar EPI por descricao ou CA (autocomplete) na tela de entrega.                |
| CA-07 | Ao selecionar EPI, sistema exibe CA e validade, com alerta visual se vencido.                     |
| CA-08 | Registro de entrega requer funcionario, EPI e motivo preenchidos; rejeita com toast de erro se ausentes. |
| CA-09 | Ao registrar entrega, sistema cria registro em `sst_epi_entrega` E movimentacao de saida em `sst_estoque_movimentacao` atomicamente. |
| CA-10 | Apos entrega bem-sucedida, formulario e limpo e lista de "Entregas Recentes" e atualizada.        |
| CA-11 | Botao "Imprimir Ficha de Entrega" aparece apos registro e aciona `window.print()`.               |
| CA-12 | Dashboard (`epis.html`) carrega 5 KPIs no topo com valores corretos.                              |
| CA-13 | Todas as 6 abas do dashboard navegam corretamente e exibem dados filtrados.                       |
| CA-14 | Drill-down de funcionario carrega entregas individuais sob demanda e exibe em tabela expandida.    |
| CA-15 | Card de setor ao ser clicado navega para aba "Por Funcionario" com filtro de setor aplicado.       |
| CA-16 | Estoque exibe status visual: OK (verde), Baixo (amarelo), Zerado (vermelho).                     |
| CA-17 | Saida de estoque e bloqueada quando quantidade solicitada > quantidade atual (toast de erro).     |
| CA-18 | Entrada de estoque cria item em `sst_estoque_epi` se nao existir previamente.                    |
| CA-19 | OS pode ser criada com titulo obrigatorio e campos opcionais.                                     |
| CA-20 | Fluxo de status da OS segue sequencia: rascunho -> em_revisao -> aprovada -> vigente.            |
| CA-21 | Ciencia so pode ser registrada em OS com status "vigente".                                        |
| CA-22 | Ciencia registra funcionario, meio de assinatura e data automatica.                               |
| CA-23 | Filtros de busca e selects funcionam em tempo real (sem necessidade de botao "Buscar").           |
| CA-24 | Todas as paginas respeitam perfil de acesso (perfisPermitidos no RHAuth.init).                    |
| CA-25 | Dados de EPI alimentam corretamente o eixo EPI do Motor Score v2 (peso 10%).                     |

---

## 26. Casos de teste

| ID    | Caso de teste                                                                                      | Pre-condicao                     | Resultado esperado                          |
|-------|---------------------------------------------------------------------------------------------------|----------------------------------|--------------------------------------------|
| CT-01 | Registrar entrega de EPI com todos os campos preenchidos                                          | Funcionario e EPI existem        | Registro criado em sst_epi_entrega + movimentacao de saida |
| CT-02 | Tentar registrar entrega sem selecionar funcionario                                               | Formulario aberto                | Toast "Selecione um funcionario" (erro)    |
| CT-03 | Tentar registrar entrega sem selecionar EPI                                                       | Funcionario selecionado          | Toast "Selecione um EPI" (erro)            |
| CT-04 | Registrar entrega com quantidade = 5                                                              | Funcionario e EPI selecionados   | Registro com quantidade=5; movimentacao saida=5 |
| CT-05 | Verificar que formulario limpa apos entrega bem-sucedida                                          | Entrega registrada               | Campos limpos, funcInfo/epiInfo ocultos    |
| CT-06 | Verificar lista "Entregas Recentes" atualiza apos registro                                        | Entrega registrada               | Nova entrega aparece no topo da lista      |
| CT-07 | Buscar funcionario com 1 caractere                                                                | Pagina carregada                 | Dropdown nao abre (min 2 chars)            |
| CT-08 | Buscar funcionario com 2+ caracteres                                                              | Pagina carregada                 | Dropdown abre com resultados filtrados     |
| CT-09 | Selecionar EPI com CA vencido                                                                     | EPI com ca_validade < hoje       | Badge "Vencido" vermelho exibido           |
| CT-10 | Navegar entre as 6 abas do dashboard                                                              | epis.html carregada              | Cada aba exibe conteudo correto            |
| CT-11 | Clicar no KPI "Catalogo Ativo"                                                                    | epis.html carregada              | Aba "Catalogo" ativada                     |
| CT-12 | Filtrar catalogo por categoria                                                                    | Catalogo com multiplas categorias| Tabela filtra corretamente                 |
| CT-13 | Filtrar catalogo por status "Inativos"                                                            | Catalogo com itens inativos      | Somente inativos exibidos                  |
| CT-14 | Filtrar entregas por periodo (data de/ate)                                                        | Entregas existentes              | Somente entregas no periodo exibidas       |
| CT-15 | Drill-down em funcionario na aba "Por Funcionario"                                                | Funcionario com entregas         | Tabela expandida com entregas individuais  |
| CT-16 | Drill-down fecha ao clicar novamente                                                              | Drill-down aberto                | Linha de detalhe removida                  |
| CT-17 | Clicar em card de setor                                                                           | Aba "Por Setor" ativa            | Navega para "Por Funcionario" com filtro   |
| CT-18 | Registrar entrada de estoque                                                                      | EPI no catalogo                  | Movimentacao criada; estoque atualizado    |
| CT-19 | Registrar saida de estoque com quantidade <= disponivel                                           | Estoque >= quantidade            | Movimentacao criada; estoque decrementado  |
| CT-20 | Tentar saida de estoque com quantidade > disponivel                                               | Estoque < quantidade             | Toast "Quantidade insuficiente em estoque" |
| CT-21 | Registrar entrada para EPI sem item de estoque existente                                          | sst_estoque_epi nao tem o item   | Novo registro criado com qtd_minima=0      |
| CT-22 | Criar nova OS com titulo preenchido                                                               | Perfil rh/admin                  | OS criada com status "rascunho"            |
| CT-23 | Criar OS sem titulo                                                                               | Modal aberto                     | Toast "Titulo e obrigatorio" (erro)        |
| CT-24 | Transicionar OS: rascunho -> em_revisao                                                          | OS em rascunho                   | Status atualizado; toast sucesso           |
| CT-25 | Transicionar OS: em_revisao -> aprovada                                                          | OS em revisao                    | Status atualizado; toast sucesso           |
| CT-26 | Transicionar OS: aprovada -> vigente                                                             | OS aprovada                      | Status atualizado; botao "Registrar Ciencia" aparece |
| CT-27 | Registrar ciencia em OS vigente                                                                   | OS vigente, funcionario existe   | Registro criado em sst_os_ciencia          |
| CT-28 | Tentar registrar ciencia sem selecionar funcionario                                               | Modal de ciencia aberto          | Toast "Selecione um funcionario" (erro)    |
| CT-29 | Verificar que ciencias aparecem no detalhe da OS                                                  | Ciencia registrada               | Lista de ciencias atualizada na modal      |
| CT-30 | Filtrar OS por status "Vigente"                                                                   | OS em varios status              | Somente vigentes exibidas                  |
| CT-31 | Buscar OS por titulo                                                                              | OS cadastradas                   | Tabela filtra por titulo corretamente      |
| CT-32 | Verificar que movimentacoes de estoque podem ser filtradas por tipo                               | Movimentacoes existentes         | Filtro funciona para entrada/saida/ajuste/devolucao |

---

## 27. Casos extremos

| ID    | Cenario                                                                                            | Comportamento esperado                          |
|-------|---------------------------------------------------------------------------------------------------|------------------------------------------------|
| CE-01 | Funcionario desligado aparece no drill-down de entregas                                           | Entregas historicas exibidas; nao permitir nova entrega (filtro status=ativo no select) |
| CE-02 | EPI com CA vencido e selecionado para entrega                                                     | Sistema exibe alerta visual mas permite a entrega (decisao do usuario) |
| CE-03 | Motivo "outro" no frontend vs. CHECK constraint do banco (nao inclui "outro")                     | INSERT falha com erro 400 do PostgREST. **Bug identificado**: frontend permite "outro" mas banco rejeita. Correcao necessaria: adicionar "outro" ao CHECK ou remover do frontend. |
| CE-04 | Dois usuarios registram entrega do mesmo EPI simultaneamente                                      | Ambas entregas sao registradas; estoque pode ficar negativo se nao houver validacao server-side. **Risco**: sem transacao atomica (frontend faz POST + PATCH separados). |
| CE-05 | Estoque zerado e usuario tenta registrar saida via `estoque_epi.html`                            | Frontend bloqueia (toast erro); porem via `entrega_epi.html` a saida e registrada sem verificar estoque. **Inconsistencia identificada**. |
| CE-06 | Funcionario sem setor_id (setor nao atribuido)                                                    | Coluna "Setor" exibe "--"; agrupacao por setor nao inclui esse funcionario. |
| CE-07 | Catalogo vazio (nenhum EPI cadastrado)                                                            | Telas de entrega e estoque exibem selects vazios; dashboard mostra KPIs zerados. |
| CE-08 | RPC `epi_kpis` retorna erro ou null                                                              | Frontend usa fallback client-side para calcular KPIs a partir dos dados ja carregados. |
| CE-09 | Mais de 2000 funcionarios ativos (limit no GET)                                                   | Funcionarios acima do limit nao aparecem em funcMap; entregas deles mostram "--" no nome. |
| CE-10 | OS com status "vigente" tem 0 ciencias registradas                                                | Exibida normalmente; KPI "Func. sem Ciencia" deveria alertar mas atualmente mostra "--". |
| CE-11 | Mesmo funcionario registra ciencia da mesma OS duas vezes                                         | Banco permite (sem UNIQUE constraint em os_id + funcionario_id). Duplicata criada. |
| CE-12 | Entrega com quantidade = 0 ou negativa                                                            | CHECK constraint `quantidade > 0` no banco rejeita; frontend valida `parseInt || 1`. |
| CE-13 | FilialContext nao disponivel (empresa sem filiais)                                                 | Frontend trata graciosamente: `typeof FilialContext !== 'undefined'` com fallback para queries sem filtro. |
| CE-14 | Movimentacao de estoque do tipo "devolucao" ou "ajuste" nao tem opcao no form de saida            | Frontend de saida oferece apenas entrega/descarte/defeito. Devolucao e ajuste so via tab de movimentacoes ou API direta. |

---

## 28. Melhorias futuras

| ID    | Melhoria                                                                                          | Prioridade | Esforco  |
|-------|--------------------------------------------------------------------------------------------------|:---:|:---:|
| MF-01 | **Implementar RPCs pendentes** (epi_kpis, epi_entregas_por_funcionario, epi_entregas_por_setor, epi_pendentes_assinatura, epi_ordens_servico) para eliminar dependencia de dados client-side. | Alta | Medio |
| MF-02 | **Adicionar RLS** a todas as tabelas de EPI (sst_catalogo_epi, sst_epi_entrega, sst_estoque_epi, sst_estoque_movimentacao, sst_cargo_epi, sst_ordem_servico, sst_os_ciencia). | Alta | Alto |
| MF-03 | **Corrigir divergencia CHECK constraint** do motivo_entrega: adicionar "outro" ao CHECK ou remover do frontend (CE-03). | Alta | Baixo |
| MF-04 | **Tela de gestao da matriz cargo x EPI** (sst_cargo_epi): atualmente a tabela existe mas nao tem interface para gerencia-la. | Alta | Medio |
| MF-05 | **Atomicidade na entrega**: mover logica de criacao de entrega + movimentacao de estoque para RPC/trigger no banco para garantir transacao atomica. | Alta | Medio |
| MF-06 | **Validar estoque na entrega**: `entrega_epi.html` nao verifica estoque antes de registrar saida (CE-05). | Media | Baixo |
| MF-07 | **UNIQUE constraint em sst_os_ciencia** para (os_id, funcionario_id) evitando ciencias duplicadas (CE-11). | Media | Baixo |
| MF-08 | **Alerta automatico de CA a vencer** (30/60/90 dias): cron job ou trigger que gera notificacao para SST. | Media | Medio |
| MF-09 | **Assinatura digital integrada**: implementar fluxo completo de assinatura digital na entrega (campo `assinatura_digital` existe na tabela mas nao e usado no frontend). | Media | Alto |
| MF-10 | **Exportacao de relatorios** em PDF e Excel para fichas de entrega, estoque e OS. | Media | Medio |
| MF-11 | **Migracao para Shell.init** nas paginas `entrega_epi.html`, `estoque_epi.html` e `ordem_servico.html` que ainda usam sidebar estatica em vez do Shell dinamico. | Media | Medio |
| MF-12 | **Historico de devolucoes**: utilizar campo `data_devolucao` de sst_epi_entrega com interface para registrar devolucao (EPI devolvido por troca/desligamento). | Media | Medio |
| MF-13 | **Notificacoes push para gestores**: notificar quando funcionario do setor nao recebe EPI ha mais de 6 meses (SIGA-08). | Baixa | Medio |
| MF-14 | **QR Code na ficha de entrega**: gerar QR Code com link para registro digital da entrega. | Baixa | Baixo |
| MF-15 | **Foto do EPI no catalogo**: upload de imagem do EPI para referencia visual. | Baixa | Medio |
| MF-16 | **Integracao BuscaEPI completa**: estabilizar heartbeat e garantir sincronizacao automatica de entregas historicas (status atual: 60%). | Alta | Alto |
| MF-17 | **Vinculo entrega-treinamento NR-6**: condicionar entrega de EPI a existencia de treinamento NR-6 valido para o funcionario (SIGA-05). | Media | Medio |
| MF-18 | **Paginacao server-side**: substituir carregamento de 200/2000 registros por paginacao real com offset/limit controlado. | Baixa | Medio |
| MF-19 | **Triggers de updated_at**: implementar triggers automaticos para atualizar `updated_at` nas tabelas que possuem o campo. | Media | Baixo |
| MF-20 | **Audit trail completo**: implementar tabela de auditoria para registrar todas as alteracoes (INSERT, UPDATE, DELETE) nas tabelas de EPI. | Media | Alto |

---

## Apendice A -- Arquitetura de arquivos

```
rh/
├── epis.html            # Dashboard EPI (6 abas, KPIs, drill-down)
├── entrega_epi.html     # Registro de entregas individuais
├── estoque_epi.html     # Controle de estoque e movimentacoes
├── ordem_servico.html   # Ordens de Servico SST
├── config.js            # Config Supabase RH
├── rh_auth.js           # Autenticacao (perfis)
├── api.js               # Camada API (get, post, patch, rpc)
├── shell.js             # Sidebar + topbar
├── ui_components.js     # Componentes UI
├── page_bootstrap.js    # Lifecycle de pagina
└── ds.css               # Design system
```

## Apendice B -- Modelo de dados (relacionamento)

```
rh_funcionarios (629)
  |
  |--- FK ---> sst_epi_entrega (41.000+)
  |                |
  |                |--- FK ---> sst_catalogo_epi (100-200)
  |                                 |
  |                                 |--- FK <--- sst_cargo_epi (200-400)
  |                                 |                |
  |                                 |                |--- FK ---> rh_cargos
  |                                 |
  |                                 |--- FK <--- sst_estoque_epi (50-100)
  |                                 |
  |                                 |--- FK <--- sst_estoque_movimentacao (5.000+)
  |
  |--- FK ---> sst_os_ciencia (500-2000)
                   |
                   |--- FK ---> sst_ordem_servico (20-50)
                                    |
                                    |--- FK ---> rh_setores
                                    |--- FK ---> rh_cargos
                                    |--- FK <--- sst_os_revisao (10-30)
```

## Apendice C -- Status dos valores de motivo_entrega

| Valor              | Frontend entrega_epi.html | Frontend epis.html | CHECK constraint SQL |
|--------------------|:---:|:---:|:---:|
| primeira_entrega   | Sim | Sim | Sim |
| substituicao       | Sim | Sim | Sim |
| desgaste           | Sim | Sim | Sim |
| perda              | Sim | Sim | Sim |
| outro              | Sim | Sim | **NAO** |
| mudanca_funcao     | **NAO** | **NAO** | Sim |

**Acao necessaria:** Alinhar frontend e banco adicionando `outro` e `mudanca_funcao` em ambos.

---

*Documento gerado em 28/07/2026. Revisao pendente de homologacao.*
