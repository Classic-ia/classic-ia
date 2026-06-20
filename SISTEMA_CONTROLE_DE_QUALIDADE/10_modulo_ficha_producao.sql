-- ============================================================
-- MIGRATION: Ficha de Produção do chão — Classic CQ
-- Data: 2026-06-20
-- Objetivo: Produtividade do chão por colaborador e por palete,
--   lançada no celular pelo operador, com dashboard em tempo real.
--   Complementa (não substitui) o Lote Base de Produção (rastreabilidade).
--   Portado do sistema operacional "Classic Estoque" (modelo do Paulo).
--
-- EXECUTAR NO SUPABASE SQL EDITOR (projeto Classic-ia CQ)
-- Depende de: fn_set_atualizado_em() (01_migration_novo_modelo_cq.sql)
-- ============================================================

create table if not exists public.cq_ficha_producao (
  id uuid primary key default gen_random_uuid(),
  data date not null default current_date,
  setor text,                       -- ex.: Sebo, Omaso, Classificação
  turno text,
  cliente text,
  lote text,
  quantidade numeric(14,2) not null default 0,   -- quantidade do lote (peças)
  num_paletes integer not null default 1,
  -- estrutura do chão (modelo Paulo): produção por colaborador e por palete
  colaboradores jsonb not null default '[]'::jsonb,  -- [{nome, pecas}]
  paletes jsonb not null default '[]'::jsonb,        -- [{numero, pecas}]
  lancamentos jsonb not null default '[]'::jsonb,    -- [{colaborador, palete, qtd, hora}]
  total numeric(14,2) not null default 0,            -- soma dos lançamentos
  foto_url text,
  obs text,
  -- auditoria (padrao CQAuth.dadosRegistro)
  usuario_id uuid,
  usuario_email text,
  usuario_nome text,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);
create index if not exists ix_cq_ficha_data on public.cq_ficha_producao (data desc);
create index if not exists ix_cq_ficha_setor on public.cq_ficha_producao (setor);

alter table public.cq_ficha_producao enable row level security;
create policy "cq_ficha_producao_select"      on public.cq_ficha_producao for select to authenticated using (true);
create policy "cq_ficha_producao_anon_select" on public.cq_ficha_producao for select to anon using (true);
create policy "cq_ficha_producao_insert"      on public.cq_ficha_producao for insert to authenticated with check (true);
create policy "cq_ficha_producao_update"      on public.cq_ficha_producao for update to authenticated using (true) with check (true);
create policy "cq_ficha_producao_delete"      on public.cq_ficha_producao for delete to authenticated using (true);

drop trigger if exists trg_cq_ficha_producao_atualizado on public.cq_ficha_producao;
create trigger trg_cq_ficha_producao_atualizado before update on public.cq_ficha_producao
  for each row execute function public.fn_set_atualizado_em();

-- VIEW: produção por colaborador por dia (para o dashboard / produtividade)
create or replace view public.cq_ficha_prod_colaborador as
select f.data,
       (c->>'nome')              as colaborador,
       sum((c->>'pecas')::numeric) as pecas,
       count(distinct f.id)        as fichas
from public.cq_ficha_producao f,
     lateral jsonb_array_elements(f.colaboradores) c
where coalesce(c->>'nome','') <> ''
group by f.data, (c->>'nome');
