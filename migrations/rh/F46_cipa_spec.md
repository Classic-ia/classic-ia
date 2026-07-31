# F-46 — CIPA (Comissao Interna de Prevencao de Acidentes)

## Especificacao Funcional Completa

| Campo | Valor |
|-------|-------|
| Codigo | F-46 |
| Titulo | CIPA — Mandatos, Membros, Reunioes, Plano de Acao e Canal de Denuncias (NR-05) |
| Modulo | SST (Saude e Seguranca do Trabalho) |
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

**F-46 — CIPA** e o modulo de gestao da Comissao Interna de Prevencao de Acidentes (NR-05): mandatos de 2 anos, composicao da comissao (presidente, vice, secretario, titulares empregador/empregado, suplentes), registro de reunioes com pauta/ata, plano de acao com prazos, e canal publico **anonimo** de denuncias de riscos.

O modulo compreende **2 telas** (1 interna + 1 publica anonima) e **9 tabelas** `cipa_*` — e, diferente de F-43/F-44/F-45, **le e grava nas mesmas tabelas** (a correcao de naming `rh_cipa_*` → `cipa_*` apontada no DRIFT_MAP foi aplicada). Os defeitos remanescentes sao de **colunas divergentes dentro do proprio arquivo** e de tabelas de apoio sem uso.

**Telas do modulo:**

| # | Arquivo | Funcao | Linhas |
|---|---------|--------|--------|
| 1 | `rh/cipa.html` | Gestao da CIPA (4 abas: Membros, Reunioes, Mandatos, Acoes) | 1112 |
| 2 | `rh/cipa-denuncia.html` | Canal publico de denuncias (anonimo, sem login) | 107 |

**Bibliotecas:** cipa.html — config.js, rh_auth.js (`RHAuth.init`), api.js, ui_components.js, page_bootstrap.js, shell.js (nao inicializado), ds.css. cipa-denuncia.html — **so config.js + ds.css** (sem auth; POST direto com chave anon). **Sem Chart.js.**

**Arquivos SQL (backend):**

| Arquivo | Objetos |
|---------|---------|
| `TABELAS_SST.sql` | cipa_mandato, cipa_membros, cipa_reunioes, cipa_presencas, cipa_assinaturas, cipa_inspecoes, cipa_denuncias, cipa_checklist_padrao |
| `MODULO_CIPA_ACOES.sql` | cipa_acoes (+ trigger updated_at + **RLS RBAC real**: leitura autenticado, escrita rh/admin via rh_usuarios) |
| `migration_cipa_esocial.sql` (raiz) | Versao alternativa das tabelas CIPA + esocial_eventos com trigger `esocial_calcular_status()` |

---

## 2. Problema

A NR-05 obriga empresas com 20+ funcionarios a constituir CIPA com processo eleitoral, mandato de 2 anos, reunioes mensais, treinamento de 20h e estabilidade dos eleitos. Antes do sistema:

1. **Mandatos e composicao em papel** — Sem registro de quem compoe a comissao, cargos e estabilidade.
2. **Reunioes sem ata rastreavel** — Pautas e deliberacoes dispersas, sem historico.
3. **Acoes sem acompanhamento** — Deliberacoes de reuniao sem responsavel/prazo/status (a tabela `cipa_acoes` nem existia — a secao ficava vazia, motivo do MODULO_CIPA_ACOES.sql).
4. **Sem canal de denuncia** — Colaboradores sem via anonima para reportar riscos/irregularidades.

**Solucao:** modulo que (a) registra mandatos com ativacao unica, (b) gere a composicao com estabilidade automatica, (c) registra reunioes com pauta/ata/proxima data, (d) acompanha acoes com status e atraso, (e) oferece canal publico anonimo de denuncias com protocolo.

---

## 3. Processo Operacional

### 3.1 Mandato (aba Mandatos)

1. Criar mandato: data inicio (hoje) + fim (default +2 anos, NR-05). Numero gerado `CIPA-AAAA/AAAA`, status `ativo`.
2. **Ativacao unica:** ativar um mandato encerra automaticamente todos os demais ativos (PATCHs sequenciais).
3. Encerrar mandato: status → `encerrado`.
4. Mandato corrente resolvido em cascata: status ativo → vigente por data → mais recente.

### 3.2 Membros (aba Membros)

1. Exige mandato criado. Busca colaborador ativo (autocomplete).
2. Cargo CIPA: presidente, vice, secretario, titular_empregador, titular_empregado, suplente. Tipo: eleito/indicado.
3. **Estabilidade automatica** = fim do mandato + 1 ano (hint: so eleitos; calculo aplica a todos — editavel).
4. Editar/remover membro (DELETE fisico).

### 3.3 Reunioes (aba Reunioes)

1. Registrar: data, local, pauta (obrigatoria), presentes (multi-select de membros — **nao persistido**), deliberacoes, ata, proxima reuniao.
2. Numero gerado `R-AAAAMMDD`; tipo fixo `ordinaria`; status fixo `aprovada`.
3. Deliberacoes sao concatenadas na `ata_texto` (`--- Deliberacoes ---`).
4. Historico clicavel → modal de detalhe.

### 3.4 Acoes (aba Acoes)

1. Nova acao: descricao, responsavel (texto livre), prazo, reuniao vinculada (opcional). Status inicial `aberto`.
2. Status inline: aberto → andamento → concluido. Prazo vencido destacado em vermelho.
3. Filtros por status e reuniao.

### 3.5 Canal de Denuncias (`cipa-denuncia.html` — publico)

1. Pagina standalone **sem login** (acessivel por link/QR; parametro `?empresa=` pre-seleciona).
2. Seleciona empresa (Classic/Mundial/TransAnacleto/J.A.) e setor (7 fixos), descreve (min 20 chars).
3. POST anonimo direto em `cipa_denuncias` (chave anon) com `{setor, descricao, anonima:true}` — **empresa selecionada e descartada** (D-06).
4. Exibe protocolo (id retornado; fallback UUID local se sem representacao).

> **Lacuna:** nao ha tela de **triagem/resposta** das denuncias — os campos `status/resposta/respondido_por` de `cipa_denuncias` nao tem UI (canal write-only). Ver D-07.

---

## 4. Usuarios

### 4.1 Perfis de Acesso

| Tela | Controle |
|------|----------|
| cipa.html | `RHAuth.init` apenas — **sem checagem de perfil** no cliente |
| cipa-denuncia.html | **Publica** (anonima por design; POST com chave anon) |

**RLS por tabela:** `cipa_acoes` tem RBAC real (SELECT autenticado; INSERT/UPDATE so rh/admin via `rh_usuarios`) — o unico modulo SST com escrita restrita no banco. Demais tabelas cipa_* seguem o padrao SST. `cipa_denuncias` requer policy INSERT para `anon` (canal publico).

---

## 5. Fluxograma

```
MANDATO                              MEMBROS
=======                              =======
Criar (inicio + fim, default +2a)    Exige mandato ativo
  numero CIPA-AAAA/AAAA                |
  status ativo                         v
   |                                 Colaborador + cargo CIPA (6) + tipo
   v                                   (eleito/indicado)
Ativar -> encerra os demais            |
Encerrar -> badge muted                v
Corrente: ativo > vigente > recente  Estabilidade = fim mandato + 1 ano

REUNIOES                             ACOES (cipa_acoes)
========                             ==================
Data + pauta (obrig.) + local        Descricao + responsavel + prazo
Presentes (UI only — nao persiste)     + reuniao vinculada (opcional)
Deliberacoes -> concatena na ata       |
numero R-AAAAMMDD                      v
tipo=ordinaria status=aprovada       aberto -> andamento -> concluido
proxima_data (grava) x                 prazo < hoje -> atrasada (vermelho)
  proxima_reuniao (le) = BUG           RLS: escrita so rh/admin

CANAL DE DENUNCIAS (publico, anonimo)
=====================================
?empresa= pre-seleciona -> setor + descricao (min 20)
   | POST anon cipa_denuncias {setor, descricao, anonima}
   |   (empresa DESCARTADA)
   v
Protocolo = id (fallback UUID local)
   x  SEM tela de triagem/resposta (status/resposta sem UI)

NAO USADAS: cipa_presencas | cipa_assinaturas | cipa_inspecoes | cipa_checklist_padrao
```

---

## 6. Regras de Negocio

### RN-01 — Mandato de 2 Anos (NR-05)

Default de datas = hoje ate +2 anos; texto informativo na tela. **Nao validado** (datas livres, apenas fim > inicio).

### RN-02 — Mandato Ativo Unico

`ativarMandato()` encerra todos os demais mandatos ativos antes de ativar o escolhido (PATCHs sequenciais, sem transacao). Mandato corrente: status ativo → vigente por data → mais recente.

### RN-03 — Cargos da Comissao

6 cargos: presidente, vice, secretario, titular_empregador, titular_empregado, suplente. **Sem validacao de unicidade** (2 presidentes possiveis) nem de proporcionalidade empregador/empregado.

### RN-04 — Estabilidade do Cipeiro

`estabilidade_ate = data_fim do mandato + 1 ano` (auto-preenchida em novo membro, editavel). Hint restringe a eleitos; o calculo **nao distingue** eleito/indicado.

### RN-05 — Membros/Reunioes Exigem Mandato

Bloqueio com toast: "Crie um mandato antes de adicionar membros" / "Crie um mandato primeiro".

### RN-06 — Reuniao Sempre Ordinaria e Aprovada

`tipo='ordinaria'` e `status='aprovada'` hardcoded (sem extraordinaria, sem rascunho). Deliberacoes concatenadas em `ata_texto`.

### RN-07 — Numeracao Automatica

Mandato `CIPA-AAAA/AAAA`; reuniao `R-AAAAMMDD`.

### RN-08 — Acao com Atraso

`prazo < hoje && status != concluido` → prazo em vermelho negrito. Status: aberto (err) → andamento (warn) → concluido (ok); CHECK inclui `cancelado` (sem UI).

### RN-09 — RBAC de Acoes (banco)

`cipa_acoes`: leitura para autenticados; INSERT/UPDATE apenas perfis administrador/rh ativos (via `rh_usuarios`) — enforcement no banco, nao no cliente.

### RN-10 — Denuncia Anonima

Canal publico sem autenticacao; minimo 20 caracteres; setor obrigatorio; `anonima=true` sempre; protocolo = id do registro (fallback UUID local se `return=representation` falhar). Nenhum dado pessoal coletado.

### RN-11 — Status da Denuncia (backend)

`cipa_denuncias.status` default `aberta`, com resposta/respondido_por/respondido_em — **sem UI de gestao** (D-07).

### RN-12 — Colunas Fantasma na Propria Tela

Gravacao e leitura divergem dentro de cipa.html: grava `proxima_data`/le `proxima_reuniao`; grava `ata_texto`/le `ata`; le `deliberacoes`/`presentes` (nunca gravados); dropdowns usam `colaborador_id` mas grava-se `funcionario_id`; acoes nunca gravam `mandato_id` (lista mistura mandatos). Ver D-01..D-05.

---

## 7. Campos

### 7.1 Mandato (aba Mandatos)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Data Inicio | `mand-inicio` | date | Sim (default hoje) |
| Data Fim | `mand-fim` | date | Sim (default +2 anos; fim > inicio) |

Sem UI: numero (auto), CNAE, grau_risco, efetivos, suplentes, status (colunas existem no schema).

### 7.2 Membro (modal)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Colaborador | `memb-colab-search`/`memb-colab-id` | autocomplete (ativos) | Sim |
| Cargo CIPA | `memb-cargo` | select (6) | Sim |
| Tipo | `memb-tipo` | select (eleito/indicado) | Default eleito |
| Estabilidade Ate | `memb-estabilidade` | date | Nao (auto: fim+1a) |

Sem UI: categoria, data_posse, treinado, data_treinamento (colunas existem).

### 7.3 Reuniao (aba Reunioes)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Data | `reu-data` | date | Sim (default hoje) |
| Local | `reu-local` | text | Nao |
| Proxima Reuniao | `reu-proxima` | date | Nao |
| Pauta | `reu-pauta` | textarea | Sim |
| Presentes | `reu-pres-search`/tags | multi-select | Nao (**nao persistido**) |
| Deliberacoes | `reu-deliberacoes` | textarea | Nao (concatenada na ata) |
| Ata | `reu-ata` | textarea | Nao |

### 7.4 Acao (modal)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Acao | `acao-descricao` | textarea | Sim |
| Responsavel | `acao-responsavel` | text livre | Sim |
| Prazo | `acao-prazo` | date | Nao |
| Reuniao Vinculada | `acao-reuniao` | select | Nao |

Sem UI: mandato_id, prioridade, responsavel_id, data_conclusao, observacoes.

### 7.5 Denuncia (`cipa-denuncia.html`)

| Campo | ID | Tipo | Obrigatorio |
|-------|----|------|-------------|
| Empresa | `empresa` | select (4: classic/mundial/transanacleto/ja) | Default classic (**descartada no POST**) |
| Setor | `setor` | select (Ribeira/Curtimento/Acabamento/Expedicao/Manutencao/Administrativo/Outro) | Sim |
| Descricao | `descricao` | textarea (minlength 20, contador) | Sim |

---

## 8. Tabelas

| Tabela | Descricao | Colunas-chave | Uso pela UI |
|--------|-----------|---------------|-------------|
| `cipa_mandato` | Mandato da comissao | numero, empresa_id, data_inicio/fim, cnae, grau_risco, efetivos, suplentes, status | CRUD parcial (2 campos) |
| `cipa_membros` | Composicao | mandato_id, funcionario_id, nome, cargo_cipa, tipo, categoria, data_posse, estabilidade_ate, treinado, data_treinamento, ativo | CRUD (4 campos) |
| `cipa_reunioes` | Reunioes | mandato_id, numero, data, tipo, local, pauta, ata_texto, status, proxima_data | POST (sem PATCH/DELETE) |
| `cipa_presencas` | Presenca por reuniao | reuniao_id, membro_id, presente, justificativa | **Nao usada** (presentes so em memoria) |
| `cipa_assinaturas` | Assinatura de ata | reuniao_id, membro_id, assinado_em, ip_address | **Nao usada** |
| `cipa_inspecoes` | Inspecoes CIPA | data, setor, inspetor_id, resultado_json, nao_conformidades | **Nao usada** por estas telas |
| `cipa_denuncias` | Denuncias anonimas | empresa_id, setor, descricao, status, resposta, respondido_por/em, anonima | INSERT anonimo (sem triagem) |
| `cipa_checklist_padrao` | Checklist de inspecao | setor, item, ordem, ativo | **Nao usada** |
| `cipa_acoes` | Plano de acao | reuniao_id, mandato_id, acao, descricao, responsavel(_id), prazo, status (CHECK 4), prioridade (CHECK 4), data_conclusao | POST/PATCH (4 campos) + RLS RBAC |

---

## 9. RPCs

**Nenhuma RPC** — as duas telas operam por PostgREST direto (cipa-denuncia via `fetch` com chave anon).

---

## 10. Functions (Backend SQL)

| Funcao | Papel |
|--------|-------|
| `fn_cipa_acoes_updated_at()` | Trigger de atualizado_em em cipa_acoes |
| `esocial_calcular_status()` (migration_cipa_esocial) | Recalcula status do evento eSocial por prazo no INSERT/UPDATE (mitiga o D-06 do F-43 no write) |

---

## 11. Triggers

`trg_cipa_acoes_updated_at` (BEFORE UPDATE). Na migration eSocial: trigger de status por prazo em esocial_eventos. Sem triggers de negocio nas demais tabelas cipa_* (sem validacao de mandato unico no banco — feita no cliente).

---

## 12. APIs (Chamadas Frontend)

### 12.1 cipa.html (`API`)

- **GET:** rh_colaboradores (ativos), cipa_mandato (order data_inicio.desc), cipa_membros (order cargo_cipa), cipa_reunioes (order data.desc), cipa_acoes (order prazo)
- **POST:** cipa_mandato, cipa_membros, cipa_reunioes, cipa_acoes
- **PATCH:** cipa_membros (editar), cipa_mandato (ativar/encerrar ×3), cipa_acoes (status)
- **DELETE:** cipa_membros

### 12.2 cipa-denuncia.html (fetch direto)

- **POST:** `SB_URL/rest/v1/cipa_denuncias` com apikey/Authorization = SB_KEY (anon), `Prefer: return=representation`, body `{setor, descricao, anonima:true}`.

**Leitura e gravacao consistentes** (mesmas tabelas cipa_*) — sem o bug rh_*/sst_* de F-43/44/45; os defeitos sao de colunas (RN-12).

---

## 13. Integracoes

### 13.1 eSocial (via migration_cipa_esocial)

A migration da CIPA tambem versiona `esocial_eventos` (S-2210/S-2220/S-2240) com trigger de status — infra compartilhada com o F-43.

### 13.2 Treinamentos (F-44)

Treinamento CIPA (12 meses) existe no catalogo de tipos do F-44; os campos `treinado`/`data_treinamento` de cipa_membros nao tem UI nem vinculo com sst_treinamento.

### 13.3 Acidentes (F-45)

Investigacoes e acoes preventivas da CIPA complementam o modulo de acidentes (sem vinculo estrutural implementado).

### 13.4 QR/Link Publico

`cipa-denuncia.html?empresa=<slug>` permite QR por empresa no chao de fabrica.

---

## 14. Dashboards

### 14.1 KPIs (`cipa.html`)

**4 KPIs:** Mandato Atual (periodo), Membros Ativos (do mandato corrente — nao filtra `ativo`), Reunioes Realizadas (do mandato), Proxima Reuniao (**quebrado** — le `proxima_reuniao`, grava-se `proxima_data` → sempre `--`).

Sem graficos, sem KPI de acoes abertas, sem KPI de denuncias.

---

## 15. Relatorios

### 15.1 Historico de Reunioes

Tabela (Data, Pauta, Presentes, Local, Proxima) + modal de detalhe (pauta, presentes, deliberacoes/ata condicionais — **blocos de ata/deliberacoes nunca aparecem** pelos bugs de coluna).

### 15.2 Acoes Pendentes

Tabela (Acao, Responsavel, Prazo, Reuniao, Status) com atraso destacado e status inline.

> Sem exportacao (ata em PDF, lista de presenca, edital de eleicao).

---

## 16. Documentos

### 16.1 Ata de Reuniao

Registrada como texto (`ata_texto`) — sem geracao de documento formal, sem assinaturas (tabela `cipa_assinaturas` ociosa).

### 16.2 Protocolo de Denuncia

Id do registro exibido ao denunciante como protocolo (sem consulta posterior por protocolo).

---

## 17. Evidencias

### 17.1 Ata + Deliberacoes

`ata_texto` concentra ata e deliberacoes concatenadas.

### 17.2 Estabilidade

`estabilidade_ate` documenta a garantia legal do cipeiro.

### 17.3 Denuncia

Registro anonimo com timestamp; sem coleta de dados pessoais (por design).

> **Lacuna:** presencas e assinaturas nao persistidas — a evidencia legal da reuniao (quem participou/assinou) nao existe.

---

## 18. Permissoes

### 18.1 Frontend

cipa.html: gate de autenticacao apenas. cipa-denuncia.html: publica por design.

### 18.2 RLS

`cipa_acoes` com RBAC real (escrita rh/admin) — **melhor padrao do eixo SST**. Demais tabelas no padrao SST; `cipa_denuncias` com INSERT anon (necessario ao canal).

---

## 19. Auditoria

Timestamps (created_at/updated_at); criado_por em cipa_acoes; ip_address previsto em cipa_assinaturas (sem uso). **Sem RHAuth.log** nas telas.

---

## 20. Eventos

| Evento | Handler |
|--------|---------|
| Criar mandato | POST cipa_mandato (numero/status auto) |
| Ativar mandato | PATCHs: encerra demais + ativa |
| Adicionar/editar membro | POST/PATCH cipa_membros (estabilidade auto) |
| Remover membro | DELETE cipa_membros |
| Registrar reuniao | POST cipa_reunioes (ordinaria/aprovada fixos) |
| Nova acao | POST cipa_acoes (status aberto) |
| Mudar status da acao | PATCH inline |
| Filtrar acoes | client-side (status/reuniao) |
| Enviar denuncia | POST anon cipa_denuncias → protocolo |

---

## 21. Dependencias

### 21.1 Infraestrutura

| Componente | Servico |
|------------|---------|
| Banco | Supabase PostgreSQL `muiqmtnfvyffborgiwdw` (RH) |
| Auth | Supabase Auth (RHAuth) / anon (denuncia) |
| Frontend | Vercel (auto-deploy main) |

### 21.2 Bibliotecas

Internas: config.js, rh_auth.js, api.js, ui_components.js, page_bootstrap.js, ds.css. Externas: nenhuma.

### 21.3 Dependencias de Dados

rh_colaboradores, cipa_* (9 tabelas), rh_usuarios (RLS de acoes).

---

## 22. Funcionalidades Relacionadas

| Funcionalidade | Modulo | Relacao |
|----------------|--------|---------|
| Acidentes | F-45 SST | Prevencao/investigacao complementar |
| Treinamentos NR | F-44 SST | Treinamento CIPA (12m); campos treinado/data sem vinculo |
| Monitor eSocial | F-43 SST | migration compartilha esocial_eventos + trigger de status |
| Inspecoes | SST | cipa_inspecoes/cipa_checklist_padrao ociosas (tela inspecoes.html usa rh_inspecoes — fantasma, DRIFT_MAP) |
| Gestao de Colaboradores | F-10 RH | Cadastro (membros) |
| Alertas RH | RH | Destino natural das acoes (sem vinculo) |

---

## 23. Melhorias SIGA

### SIGA-01 — Corrigir Colunas Divergentes

Alinhar `proxima_data`/`proxima_reuniao`, `ata_texto`/`ata`, `funcionario_id`/`colaborador_id`; gravar `mandato_id` nas acoes; separar deliberacoes da ata.

### SIGA-02 — Persistir Presencas e Assinaturas

Gravar `cipa_presencas` (multi-select ja existe na UI) e implementar assinatura de ata (`cipa_assinaturas`, com ip).

### SIGA-03 — Triagem de Denuncias

Criar aba/tela de gestao de `cipa_denuncias` (status aberta→em analise→respondida, resposta, responsavel) e enviar `empresa_id` no POST do canal.

### SIGA-04 — Dimensionamento NR-05

Implementar o Quadro I (efetivos/suplentes por nº de funcionarios × grau de risco/CNAE) usando as colunas ja existentes em cipa_mandato.

### SIGA-05 — Processo Eleitoral e Treinamento

Alerta de eleicao (60 dias antes do fim do mandato), edital/inscricoes/apuracao; usar `treinado`/`data_treinamento` com vinculo ao F-44 (20h).

### SIGA-06 — Reuniao Mensal e Quorum

Verificar periodicidade mensal obrigatoria (lacunas), tipo extraordinaria, status rascunho→aprovada, quorum.

### SIGA-07 — Validacoes de Composicao

Unicidade de presidente/vice/secretario e paridade empregador/empregado por mandato.

### SIGA-08 — Ativacao Transacional

Mover a regra de mandato ativo unico para o banco (constraint parcial/trigger) em vez de PATCHs sequenciais no cliente.

---

## 24. Adequacao SIGA (Multiempresa) e Divergencias

### 24.1 Divergencias Detectadas

| # | Divergencia | Impacto |
|---|-------------|---------|
| D-01 | Grava `proxima_data`, le `proxima_reuniao` | KPI e coluna "Proxima" sempre `--` |
| D-02 | Grava `ata_texto`, le `ata`; `deliberacoes` nunca gravada | Blocos de ata/deliberacoes nunca aparecem no modal |
| D-03 | Presentes so em memoria (`cipa_presencas` nunca gravada); coluna "Presentes" le `r.presentes` inexistente | Presenca sempre 0; sem evidencia legal de participacao |
| D-04 | Dropdowns usam `colaborador_id`, gravacao usa `funcionario_id` | Multi-select lista `(desconhecido)`; edicao nao preenche nome |
| D-05 | `cipa_acoes.mandato_id` nunca gravado; render nao filtra por mandato | Acoes de todos os mandatos misturadas |
| D-06 | Canal de denuncia descarta a empresa selecionada (POST so setor/descricao) | Denuncia sem atribuicao de empresa (multiempresa quebrado) |
| D-07 | `cipa_denuncias` sem UI de triagem/resposta (write-only) | Denuncias entram e ninguem gerencia pelo sistema |
| D-08 | 4 tabelas ociosas (presencas, assinaturas, inspecoes, checklist); campos de membro (posse/treinado/categoria) e mandato (CNAE/grau/efetivos) sem UI | Cobertura NR-05 parcial |
| D-09 | Sem dimensionamento, eleicao, reuniao mensal verificada, quorum, alerta de fim de mandato | Conformidade NR-05 incompleta |
| D-10 | Sem checagem de perfil no cliente (mitigado no banco so para cipa_acoes) | Mandatos/membros/reunioes editaveis por qualquer autenticado |

**Nota positiva:** naming de tabelas consistente (correcao do DRIFT_MAP aplicada) e `cipa_acoes` com RLS RBAC real — os dois melhores padroes do eixo SST.

### 24.2 Multiempresa

1. Usar `empresa_id` (ja existe em cipa_mandato/cipa_denuncias) em toda a cadeia — hoje descartado
2. RLS por empresa
3. Uma CIPA por estabelecimento (NR-05) — mandatos por empresa/filial
4. Canal de denuncia por empresa (slug ja existe na URL — basta enviar)
5. Dimensionamento por CNAE/grau de risco de cada empresa

---

## 25. Criterios de Aceite

### CA-01 — Mandato

Criar com default +2 anos; numero CIPA-AAAA/AAAA; ativar encerra os demais; encerrar funciona.

### CA-02 — Membro

Exige mandato; cargo/tipo; estabilidade auto (fim+1a); editar/remover.

### CA-03 — Reuniao

Pauta obrigatoria; numero R-AAAAMMDD; historico e modal.

### CA-04 — Proxima Reuniao Visivel

KPI e coluna mostram a data gravada (requer SIGA-01).

### CA-05 — Acao

Criar com responsavel/prazo; status inline; atraso destacado; RLS de escrita rh/admin.

### CA-06 — Filtros de Acoes

Por status e por reuniao.

### CA-07 — Denuncia Anonima

Min 20 chars; setor obrigatorio; protocolo exibido; form trava apos envio.

### CA-08 — Denuncia com Empresa

`empresa_id` gravado conforme selecao (requer SIGA-03).

### CA-09 — Presencas Persistidas

Presentes gravados em cipa_presencas (requer SIGA-02).

### CA-10 — Acesso

cipa.html so autenticado; canal publico funcional sem login.

---

## 26. Casos de Teste

### CT-01 — Mandato 2 Anos

Criar hoje → fim default +2 anos; status ativo.

### CT-02 — Ativacao Unica

2 mandatos ativos → ativar o segundo encerra o primeiro.

### CT-03 — Membro com Estabilidade

Novo membro eleito → estabilidade = fim do mandato + 1 ano.

### CT-04 — Reuniao com Ata

Registrar com ata + deliberacoes → ata_texto concatenada; modal exibe (pos SIGA-01).

### CT-05 — Bug Proxima (atual)

Gravar proxima_data → KPI mostra `--` (D-01).

### CT-06 — Acao Atrasada

Prazo ontem, status aberto → prazo vermelho.

### CT-07 — Status Inline

aberto → andamento → concluido via select.

### CT-08 — Escrita de Acao por Visualizador

Perfil fora de rh/admin → INSERT bloqueado pela RLS.

### CT-09 — Denuncia Valida

20+ chars + setor → protocolo retornado; form desabilitado.

### CT-10 — Denuncia Curta

19 chars → erro "minimo 20 caracteres".

---

## 27. Casos Extremos

### CE-01 — Sem Mandato

Adicionar membro/reuniao → bloqueado com toast.

### CE-02 — Dois Presidentes

Permitido (sem validacao — D-08/SIGA-07).

### CE-03 — Membro Removido

DELETE fisico — some do historico de reunioes passadas (sem soft delete).

### CE-04 — Acoes de Mandatos Antigos

Aparecem misturadas (mandato_id nunca gravado — D-05).

### CE-05 — Presencas Perdidas

Selecionar presentes e salvar → nada persistido (D-03).

### CE-06 — Denuncia sem Representacao

POST ok sem body de retorno → protocolo fake local (UUID truncado).

### CE-07 — Empresa na URL

`?empresa=mundial` pre-seleciona; POST descarta mesmo assim (D-06).

### CE-08 — Denuncia Nao Triada

Fica `aberta` para sempre (sem UI — D-07).

---

## 28. Melhorias Futuras

### MF-01 — Processo Eleitoral Digital

Edital, inscricao de candidatos, votacao eletronica e apuracao com atas.

### MF-02 — Ata Assinada Digitalmente

Assinatura dos presentes (cipa_assinaturas + ip/dispositivo) e PDF da ata.

### MF-03 — Inspecoes CIPA

Ativar cipa_inspecoes + cipa_checklist_padrao com app de inspecao por setor e geracao automatica de acoes.

### MF-04 — Painel de Denuncias

Triagem com SLA, categorias, encaminhamento a acao/inspecao e resposta anonima consultavel por protocolo.

### MF-05 — Calendario da CIPA

Agenda anual (reunioes mensais, SIPAT, eleicao) com notificacoes.

### MF-06 — Integracao com Acidentes

Acidente grave dispara reuniao extraordinaria e acao vinculada.

### MF-07 — Portal do Cipeiro

Acesso dos membros as pautas, atas e acoes sob sua responsabilidade.

### MF-08 — Indicadores da CIPA

Acoes concluidas no prazo, presenca media, denuncias por setor, tempo de resposta.

---

*Documento gerado em 28/07/2026 — Classic IA — CIPA*
*Versao 1.0 — Especificacao Funcional Completa F-46*
