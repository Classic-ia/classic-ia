-- ============================================================
-- MIGRATION: Ficha de Produção do chão — Classic CQ
-- Data: 2026-06-20 (RLS endurecido em 2026-07-20)
-- Objetivo: Produtividade do chão por colaborador e por palete.
--   Complementa (não substitui) o Lote Base de Produção.
--
-- SEGURANCA: sem acesso anonimo; escrita exige usuario ativo em cq_usuarios;
--   exclusao restrita ao perfil 'gestor'.
--
-- EXECUTAR NO SUPABASE SQL EDITOR (projeto Classic-ia CQ)
-- Depende de: fn_set_atualizado_em(), cq_e_autorizado()/cq_e_gestor() (migration 09), cq_usuarios (01)
-- ============================================================

create table if not exists public.cq_ficha_producao (
  id uuid primary key default gen_random_uuid(),
  data date not null default current_date,
  setor text,
  turno text,
  cliente text,
  lote text,
  quantidade numeric(14,2) not null default 0,
  num_paletes integer not null default 1,
  colaboradores jsonb not null default '[]'::jsonb,
  paletes jsonb not null default '[]'::jsonb,
  lancamentos jsonb not null default '[]'::jsonb,
  total numeric(14,2) not null default 0,
  foto_url text,
  obs text,
  usuario_id uuid,
  usuario_email text,
  usuario_nome text,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);
create index if not exists ix_cq_ficha_data on public.cq_ficha_producao (data desc);
create index if not exists ix_cq_ficha_setor on public.cq_ficha_producao (setor);

alter table public.cq_ficha_producao enable row level security;
drop policy if exists "cq_ficha_producao_select" on public.cq_ficha_producao;
drop policy if exists "cq_ficha_producao_anon_select" on public.cq_ficha_producao;
drop policy if exists "cq_ficha_producao_insert" on public.cq_ficha_producao;
drop policy if exists "cq_ficha_producao_update" on public.cq_ficha_producao;
drop policy if exists "cq_ficha_producao_delete" on public.cq_ficha_producao;
create policy "cq_ficha_producao_select" on public.cq_ficha_producao for select to authenticated using (public.cq_e_autorizado());
create policy "cq_ficha_producao_insert" on public.cq_ficha_producao for insert to authenticated with check (public.cq_e_autorizado());
create policy "cq_ficha_producao_update" on public.cq_ficha_producao for update to authenticated using (public.cq_e_autorizado()) with check (public.cq_e_autorizado());
create policy "cq_ficha_producao_delete" on public.cq_ficha_producao for delete to authenticated using (public.cq_e_gestor());

drop trigger if exists trg_cq_ficha_producao_atualizado on public.cq_ficha_producao;
create trigger trg_cq_ficha_producao_atualizado before update on public.cq_ficha_producao
  for each row execute function public.fn_set_atualizado_em();

-- VIEW: produção por colaborador por dia (respeita o RLS do chamador)
create or replace view public.cq_ficha_prod_colaborador
with (security_invoker = on) as
select f.data,
       (c->>'nome')               as colaborador,
       sum((c->>'pecas')::numeric) as pecas,
       count(distinct f.id)         as fichas
from public.cq_ficha_producao f,
     lateral jsonb_array_elements(f.colaboradores) c
where coalesce(c->>'nome','') <> ''
group by f.data, (c->>'nome');
