# Trans Anacleto — Módulo Operacional de Frota

Sistema complementar ao TMS Transporte Fácil. Mobile-first para pátio/oficina,
desktop para gestão. Foco em **operação**: pneus, manutenção, abastecimento,
estoque e telemetria (Onixsat). Não substitui o TMS em funcionalidades
fiscais/financeiras.

Plano completo em
`/root/.claude/plans/eu-preciso-de-um-polymorphic-nova.md`.

## Status das Fases

| Fase | Status | Entrega |
|---|---|---|
| **1 — Infra local** | ✅ Em andamento | Skeleton, auth, shell, Supabase CLI, trans_usuarios + RLS |
| 2 — Cadastros | pendente | veículos, motoristas, fornecedores, categorias, produtos (SKU + EANs) |
| 2.5 — Importar CSV | pendente | XLSX + RPC `p_importar_lote` |
| 3 — Estoque Novo + Etiquetas | pendente | lote, FEFO/FIFO, labels.js, entrada/saida/lotes/etiquetas |
| 3.2 — Estoque Usado + Conversões | pendente | A/B/C, sucata config, conversão novo→usado, usado→sucata |
| 3.5 — NFe | pendente | XML parse, match por EAN, lote por item |
| 3.7 — Inventário | pendente | contagem novo (lote+EAN) e usado (SKU+condição) |
| 4 — Pneus Desktop | pendente | schema pneus, RPCs, gestão |
| 4.5 — Pneus Mobile | pendente | /m/ shell, BarcodeDetector, fluxos mobile |
| 5 — Manutenção Desktop + Planos | pendente | planos, OS, itens, alertas |
| 5.2 — Manutenção Mobile | pendente | /m/m_manutencao_* |
| 5.5 — NFS-e serviços | pendente | serviços externos, trigger recapagem/saída estoque |
| 6 — Abastecimento | pendente | tanque + postos + consumo km/L |
| 6.2 — Integração TMS | pendente | sync veículos/motoristas, export custos |
| 6.5 — Telemetria Onixsat | pendente | webhook, km auto, motor de alertas, viagens, score |
| 7 — Dashboard Gerencial | pendente | KPIs agregados |
| 8 — Produção | pendente | cloud Supabase, DNS, transanacleto.com.br |

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
