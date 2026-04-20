# Trans Anacleto — Módulo Operacional de Frota

Sistema complementar ao TMS Transporte Fácil. Mobile-first para pátio/oficina,
desktop para gestão. Foco em **operação**: pneus, manutenção, abastecimento,
estoque e telemetria (Onixsat). Não substitui o TMS em funcionalidades
fiscais/financeiras.

Plano completo em
`/root/.claude/plans/eu-preciso-de-um-polymorphic-nova.md`.

## Status das Fases

Todas as fases 1 a 7 estao commitadas. Fase 8 (produção) tem docs e
`vercel.json` prontos — execução dos passos em `DEPLOY.md` depende do
usuário (Supabase cloud, DNS, credenciais Onixsat).

## Stack

- **Frontend**: HTML/JS/CSS estático, `ds.css` + `ui_components.js`
  (copiados de `/rh/` para desacoplar).
- **Auth**: Supabase Auth + `trans_usuarios` (RBAC).
- **Backend**: Supabase (Postgres + PostgREST + RLS + Edge Functions).
- **Dev local**: `supabase/` CLI via Docker.
- **Produção**: Vercel + Supabase cloud (quando Fase 8).

## Estrutura

```
trans-anacleto/
├── config.js           SB_URL + SB_KEY (dev = local Supabase)
├── auth.js             TransAuth (sessão trans_sess_v1)
├── api.js              API {ok,data,error,status}
├── shell.js            Sidebar + topbar + rotas
├── page_bootstrap.js   Lifecycle padrão
├── ds.css              Cópia de rh/ds.css
├── ui_components.js    Cópia de rh/ui_components.js
├── login.html
├── index.html          Painel inicial
├── m/                  Mobile (pendente a partir da Fase 4.5)
└── supabase/
    ├── config.toml     Porta 54321, schemas, auth
    ├── migrations/     Versionado
    └── seed.sql        Dev-only seed
```

## Como rodar localmente

```bash
# 1. Supabase local (requer Docker)
cd /home/user/classic-ia/trans-anacleto
npx supabase start
# copiar API URL + anon key em config.js (já deixado no default)

# 2. Criar primeiro usuário admin (Studio + SQL)
# Ver detalhes em supabase/README.md

# 3. Front-end
cd /home/user/classic-ia
python3 -m http.server 8080
# abrir http://localhost:8080/trans-anacleto/login.html
```

## Convenções

- Prefixo tabelas: `trans_`.
- Prefixo RPCs: `trans_`.
- Perfis: `admin`, `gestor`, `almoxarife`, `motorista`.
- Sessão: `trans_sess_v1` em `localStorage`.
- CSS: `ds.css` exclusivo (ZERO CSS inline em páginas de produção).
- Componentes: `ui_components.js`.
- Font: Inter.
