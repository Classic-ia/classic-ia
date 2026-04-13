# CLAUDE.md — Classic RH & SST

## O que e este projeto

Plataforma corporativa de RH e Seguranca do Trabalho (SST) para a Classic Couros.
Hub de governanca operacional que integra gestao de colaboradores, beneficios, ASOs,
treinamentos, EPIs, acidentes, ocorrencias, desligamentos e indicadores.

## Stack

- **Frontend:** HTML5 + JS vanilla (sem framework), design system proprio (`ds.css`)
- **Backend:** Supabase (PostgreSQL 17) via PostgREST + Edge Functions
- **Auth:** Supabase Auth com JWT, perfis via `rh_perfis_acesso` + RLS
- **Deploy:** Vercel (static) — sem build step
- **Dev local:** `npx serve . -l 3000`

## Arquivos de infraestrutura

| Arquivo | Funcao |
|---------|--------|
| `config.js` | URL e anon key do Supabase |
| `rh_auth.js` | Login, sessao, refresh token |
| `api.js` | Camada padrao de API (contrato `{ok, data, error, status}`) |
| `shell.js` | Sidebar + topbar + rotas + layout compartilhado |
| `ds.css` | Design system (cores, componentes, responsivo) |
| `rh_api.js` | API legada (fetch direto) — NAO usar em paginas novas |
| `ui_components.js` | Componentes UI reutilizaveis |
| `page_bootstrap.js` | Bootstrap para paginas |

## Contrato da API (`api.js`)

```js
// Toda resposta: { ok: boolean, data: any, error: string|null, status: number }
API.get(endpoint)        // Leitura → Array ou []
API.rpc(name, params)    // RPC → Objeto ou null. Params SEMPRE com prefixo p_
API.post(table, data)    // Criacao
API.patch(endpoint, data)// Atualizacao
API.del(endpoint)        // Exclusao
```

- Timeout: 15s GET, 30s RPC
- 401 → redireciona para login.html
- Campo de nome: SEMPRE `nome` (nunca `nome_completo`)
- View padrao: `vw_funcionario_base` para leitura de funcionarios
- Escapar HTML: `API.esc()`
- Formatar: `API.fmtNum()`, `API.fmtData()`

## Padrao para paginas novas

```html
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Classic RH - [TITULO]</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="ds.css">
  <script src="config.js"></script>
  <script src="rh_auth.js"></script>
  <script src="api.js"></script>
  <script src="shell.js"></script>
</head>
<body class="page-bg">
  <div id="shell"></div>
  <div id="shell-content" class="content-area page-padding">
    <!-- conteudo -->
  </div>
  <script>
  (async () => {
    const user = await Shell.init({ page: 'ID_PAGINA', title: 'Titulo' });
    if (!user) return;
    // carregar dados...
  })();
  </script>
</body>
</html>
```

**Obrigatorio:** loading skeleton, empty state, try/catch, formatacao pt-BR, `API.esc()` para HTML.

## Status da migracao de paginas

### Totalmente migradas (shell.js + api.js, sem rh_api.js) — TODAS (51 paginas)
- Migracao completa em 2026-04-13. Nenhuma pagina usa `rh_api.js`.
- `rh_api.js` pode ser removido do repositorio em sessao futura.

### Paginas sem rota no shell.js (por design — sub-paginas ou auth)
- `ficha.html` — sub-pagina de colaboradores (abre ao clicar no colaborador)
- `cipa-denuncia.html` — sub-pagina acessada via CIPA
- `login.html` — pagina de autenticacao
- `index.html` — redirect

## Modulos do sistema

| Grupo | Modulos |
|-------|---------|
| Pessoas | Colaboradores, Admissoes, Desligamentos |
| Processos | Ocorrencias, Contratos Experiencia |
| SST | Painel SST, ASOs, Treinamentos, EPIs, Acidentes, Entrega/Estoque EPI, Ordem Servico, CIPA, eSocial |
| Operacional | Ponto (Secullum), Beneficios, Ferias, Custos |
| Gestao | Motor Decisao, Pendencias, Plano Acao, Lideranca |
| Inteligencia | Dashboard Exec., Motor v2 Score, Auditoria Score, Rel. Atestados |
| RH Avancado | Organograma, Notificacoes, Saude Ocupacional |
| Qualidade | Painel CQ, Fornecedores, Lotes, Inspecoes, Nao Conformidades |
| Ferramentas | Agentes IA, Ficha 360 |
| Sistema | Integracoes, Relatorios |

## Perfis de acesso

`administrador`, `rh`, `sst`, `gestor`, `gestor_confianca`, `lider`, `psicologa`, `financeiro`, `diretoria`, `qualidade`, `colaborador`, `visualizador`

## Regras de negocio criticas

- **BLOQUEIO:** Admissao sem ASO admissional apto
- **BLOQUEIO:** Admissao sem treinamento NR-06
- **BLOQUEIO:** Desligamento sem exame demissional
- **ALERTA CRITICO:** ASO vencido → afastar ate regularizar
- **ALERTA CRITICO:** Contrato experiencia 90d → efetivar ou desligar
- **ALERTA:** 3+ atestados no mes → notificar gestor
- **ALERTA:** 3 advertencias → avaliar medida disciplinar
- Competencia de apuracao: dia 27 a 26 do mes seguinte
- Soft delete preferido (nunca DELETE fisico de colaboradores)

## Roadmap (de ARQUITETURA_SISTEMA_RH_SST.md)

### Fase 1 — IMEDIATA (concluida)
- [x] Dashboard Gerencial com indicadores
- [x] Importar atestados 2026
- [x] Importar beneficios
- [x] Criar tabelas `rh_sync_log` e `rh_divergencias` (com RLS + policies)
- [x] Dashboard Gerencial no menu (rota `dashboard_exec` no shell.js)

### Fase 2 — CURTO PRAZO (parcialmente concluida)
- [x] Ficha 360 do colaborador (`ficha_360.html` com seletor de colaborador)
- [ ] Motor de regras basico (ASO + experiencia)
- [ ] Painel de Integracoes
- [x] Busca global no header (campo de busca no topbar com dropdown)
- [ ] Importar treinamentos das pastas

### Fase 3 — MEDIO PRAZO
- [ ] Fluxos operacionais completos
- [ ] Sistema de alertas automaticos
- [ ] Dados bancarios + exportacao financeiro
- [ ] Importacao automatica de atestados
- [ ] Sync automatica Convenia

### Fase 4 — LONGO PRAZO
- [ ] Webhook eSocial
- [ ] App mobile (PWA)
- [ ] Relatorios automaticos semanais
- [ ] BI com historico e tendencias

## SQL schemas importantes

| Arquivo | Conteudo |
|---------|----------|
| `00_schema_supabase.sql` | Schema base com tabelas principais |
| `schema_rh_sst_completo.sql` | Schema completo RH+SST |
| `SEGURANCA_SUPABASE.sql` | RLS policies, perfis, funcoes de acesso |
| `FUNDACAO_BANCO_v2.sql` | Fundacao do banco de dados |
| `MODULO_SST_v2.sql` | Tabelas SST (acidentes, DDS, CIPA, extintores) |
| `MODULO_PROCESSOS_RH_v2.sql` | Fluxos de vagas, admissao, desligamento |
| `MODULO_SAUDE_MENTAL.sql` | Saude ocupacional (dados sensiveis — acesso restrito) |
| `MODULO_DESLIGAMENTOS_TURNOVER.sql` | Desligamentos e calculo turnover |

## Convencoes

- Idioma do codigo: portugues (variaveis, tabelas, comentarios)
- Prefixo de tabelas RH: `rh_` | SST: `sst_` | Qualidade: `cq_`
- Parametros RPC: SEMPRE prefixo `p_` (ex: `p_funcionario_id`)
- Campo nome: `nome` (nunca `nome_completo` ou `colaborador_nome`)
- Datas: `DD/MM/YYYY` no display, ISO no banco
- Numeros: formatacao pt-BR (`1.234,56`)
- IDs: UUID com `gen_random_uuid()`
- Sem dark mode (removido na Fase 3)

## Banco de dados

- **Tabela de perfis:** `rh_usuarios` (colunas: `auth_uid`, `perfil`, `ativo`)
  - NAO existe `rh_perfis_acesso` — usar `rh_usuarios` nas RLS policies
- **Tabelas de integracao:** `rh_sync_log`, `rh_divergencias` (criadas 2026-04-13)
- **RLS:** Todas as tabelas novas devem ter RLS habilitado + policies via `rh_usuarios`

## Arquitetura de navegacao

- `index.html` — Site institucional (publico, sem link de acesso)
- `login.html` — Login (acessivel somente por link direto)
- `hub.html` — Seletor de modulos (RH & SST, CQ, Estoque) apos login
- `app.html` — Dashboard RH & SST
- `cq_dashboard.html` — Dashboard Qualidade (desvinculado do sidebar RH)

## Proximos passos sugeridos

1. ~~Migrar `app.html`~~ (concluido 2026-04-13)
2. ~~Remover `rh_api.js` de todas as 33 paginas hibridas~~ (concluido 2026-04-13)
3. ~~Criar as 5 paginas do modulo Qualidade~~ (concluido 2026-04-13)
4. ~~Adicionar rotas faltantes ao `shell.js`~~ (concluido 2026-04-13)
5. ~~Criar tabelas `rh_sync_log` e `rh_divergencias`~~ (concluido 2026-04-13)
6. ~~Schema SQL do modulo Qualidade~~ (ja existia: 13 tabelas `cq_*`)
7. ~~Implementar busca global no header do shell~~ (concluido 2026-04-13)
8. ~~Corrigir paginas CQ para schema real~~ (concluido 2026-04-13)
9. ~~Criar hub.html e desvincular CQ/Estoque do sidebar~~ (concluido 2026-04-13)
10. ~~Fix importar_secullum: faltas incorretas por coluna DIA~~ (concluido 2026-04-13)
11. ~~Fix ficha_360: seletor quando sem ?id~~ (concluido 2026-04-13)
12. Motor de regras basico (ASO + experiencia)
13. Painel de Integracoes funcional

---

*Atualizado em: 2026-04-13 — Sessao: migrou app.html, criou hub.html, criou 5 paginas CQ, busca global, fix secullum faltas, fix ficha_360, 9 rotas novas, tabelas sync/divergencias, CIPA zerada, site institucional*
