-- ============================================================
-- MIGRATION: Modulo Controle de Sal — Classic CQ
-- Data: 2026-06-19
-- Objetivo: Controle de estoque de sal por tipo (fino/grosso/usado),
--   com entradas, saidas, consumo por setor, ajustes e transferencias.
--   Portado do sistema operacional "Classic Estoque".
--
-- EXECUTAR NO SUPABASE SQL EDITOR (projeto Classic-ia CQ)
-- Depende de: fn_normalizar_texto() e fn_set_atualizado_em() (01_migration_novo_modelo_cq.sql)
-- ============================================================

-- 1) TIPOS DE SAL (tabela mestre)
create table if not exists public.cq_sal_tipos (
  id uuid primary key default gen_random_uuid(),
  nome text not null,
  nome_normalizado text not null,
  unidade text not null default 'kg',
  saldo_inicial numeric(14,2) not null default 0,
  ativo boolean not null default true,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);
create unique index if not exists ux_cq_sal_tipos_nome on public.cq_sal_tipos (nome_normalizado);

alter table public.cq_sal_tipos enable row level security;
create policy "cq_sal_tipos_select"      on public.cq_sal_tipos for select to authenticated using (true);
create policy "cq_sal_tipos_anon_select" on public.cq_sal_tipos for select to anon using (true);
create policy "cq_sal_tipos_insert"      on public.cq_sal_tipos for insert to authenticated with check (true);
create policy "cq_sal_tipos_update"      on public.cq_sal_tipos for update to authenticated using (true) with check (true);

drop trigger if exists trg_cq_sal_tipos_atualizado on public.cq_sal_tipos;
create trigger trg_cq_sal_tipos_atualizado before update on public.cq_sal_tipos
  for each row execute function public.fn_set_atualizado_em();

-- normalizar nome automaticamente
create or replace function public.fn_cq_sal_tipos_norm()
returns trigger language plpgsql as $$
begin
  new.nome_normalizado = public.fn_normalizar_texto(new.nome);
  return new;
end;
$$;
drop trigger if exists trg_cq_sal_tipos_norm on public.cq_sal_tipos;
create trigger trg_cq_sal_tipos_norm before insert or update on public.cq_sal_tipos
  for each row execute function public.fn_cq_sal_tipos_norm();

-- 2) MOVIMENTACOES DE SAL
-- tipo: entrada | saida | consumo | ajuste | transferencia
--   entrada       (+saldo no tipo)
--   saida         (-saldo no tipo)
--   consumo       (-saldo no tipo; setor = Couro/Omaso/...)
--   ajuste        (quantidade = diferenca com sinal; pode ser + ou -)
--   transferencia (-tipo origem, +tipo destino)
create table if not exists public.cq_sal_mov (
  id uuid primary key default gen_random_uuid(),
  data date not null default current_date,
  tipo text not null check (tipo in ('entrada','saida','consumo','ajuste','transferencia')),
  tipo_sal_id uuid not null references public.cq_sal_tipos(id) on delete restrict,
  tipo_sal_destino_id uuid references public.cq_sal_tipos(id) on delete restrict,
  setor text,                       -- usado em 'consumo' (Couro, Omaso, ...)
  origem_destino text,              -- fornecedor (entrada) / cliente/destino (saida)
  quantidade_kg numeric(14,2) not null default 0,
  unidade text not null default 'kg',
  obs text,
  -- auditoria (padrao CQAuth.dadosRegistro)
  usuario_id uuid,
  usuario_email text,
  usuario_nome text,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);
create index if not exists ix_cq_sal_mov_data on public.cq_sal_mov (data desc);
create index if not exists ix_cq_sal_mov_tipo_sal on public.cq_sal_mov (tipo_sal_id);

alter table public.cq_sal_mov enable row level security;
create policy "cq_sal_mov_select"      on public.cq_sal_mov for select to authenticated using (true);
create policy "cq_sal_mov_anon_select" on public.cq_sal_mov for select to anon using (true);
create policy "cq_sal_mov_insert"      on public.cq_sal_mov for insert to authenticated with check (true);
create policy "cq_sal_mov_update"      on public.cq_sal_mov for update to authenticated using (true) with check (true);
create policy "cq_sal_mov_delete"      on public.cq_sal_mov for delete to authenticated using (true);

drop trigger if exists trg_cq_sal_mov_atualizado on public.cq_sal_mov;
create trigger trg_cq_sal_mov_atualizado before update on public.cq_sal_mov
  for each row execute function public.fn_set_atualizado_em();

-- 3) VIEW DE SALDO por tipo de sal
--    saldo = saldo_inicial + entradas - saidas - consumo + ajustes(+/-) + transfers recebidas - transfers enviadas
create or replace view public.cq_sal_saldo as
with mov as (
  select tipo_sal_id as tid,
         sum(case
               when tipo = 'entrada'        then quantidade_kg
               when tipo = 'saida'          then -quantidade_kg
               when tipo = 'consumo'        then -quantidade_kg
               when tipo = 'ajuste'         then quantidade_kg   -- diferenca com sinal
               when tipo = 'transferencia'  then -quantidade_kg  -- sai da origem
               else 0 end) as delta
  from public.cq_sal_mov group by tipo_sal_id
),
transf_in as (
  select tipo_sal_destino_id as tid, sum(quantidade_kg) as entra
  from public.cq_sal_mov where tipo = 'transferencia' and tipo_sal_destino_id is not null
  group by tipo_sal_destino_id
)
select t.id, t.nome, t.unidade, t.saldo_inicial,
       t.saldo_inicial + coalesce(m.delta,0) + coalesce(ti.entra,0) as saldo
from public.cq_sal_tipos t
left join mov m       on m.tid  = t.id
left join transf_in ti on ti.tid = t.id
where t.ativo = true;

-- 4) SEED dos tipos padrao (idempotente)
insert into public.cq_sal_tipos (nome, nome_normalizado, unidade)
values ('Sal Fino','sal fino','kg'),
       ('Sal Grosso','sal grosso','kg'),
       ('Sal Usado','sal usado','kg')
on conflict (nome_normalizado) do nothing;
