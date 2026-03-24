-- ============================================================
-- MIGRATION: Novo Modelo de Dados CQ - Classic Couros
-- Data: 2026-03-24
-- Objetivo: Estrutura correta do dominio CQ
--   Lote como entidade central
--   Fornecedor/Produto/Defeito como tabelas mestres
--   Parametros por produto (sem hardcode)
--   Classificacao e decisao 100% backend
--   A + B + C = quantidade recebida (enforced)
--
-- EXECUTAR NO SUPABASE SQL EDITOR (projeto Classic-ia CQ)
-- Executar em ordem: Fase 1, 2, 3, 4
-- ============================================================

-- ************************************************************
-- FASE 1: FUNDACAO (tabelas mestres)
-- ************************************************************

-- 0) Funcao de normalizacao de texto
create or replace function public.fn_normalizar_texto(p_texto text)
returns text language sql immutable as $$
  select trim(lower(regexp_replace(
    coalesce(p_texto, ''),
    '[àáâãäå]', 'a', 'gi'
  )));
$$;

-- 0b) Funcao de atualizado_em
create or replace function public.fn_set_atualizado_em()
returns trigger language plpgsql as $$
begin
  new.atualizado_em = now();
  return new;
end;
$$;

-- 1) FORNECEDORES
create table if not exists public.cq_fornecedores (
  id uuid primary key default gen_random_uuid(),
  codigo_atak text unique,
  nome text not null,
  nome_normalizado text not null,
  documento text,
  ativo boolean not null default true,
  cidade text,
  estado text,
  observacoes text,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

create unique index if not exists uq_cq_fornecedores_nome_normalizado
  on public.cq_fornecedores (nome_normalizado);

create index if not exists idx_cq_fornecedores_ativo
  on public.cq_fornecedores (ativo) where ativo = true;

drop trigger if exists trg_cq_fornecedores_atualizado on public.cq_fornecedores;
create trigger trg_cq_fornecedores_atualizado
  before update on public.cq_fornecedores
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_fornecedores enable row level security;
drop policy if exists "cq_fornecedores_select" on public.cq_fornecedores;
create policy "cq_fornecedores_select" on public.cq_fornecedores for select to authenticated using (true);
drop policy if exists "cq_fornecedores_anon_select" on public.cq_fornecedores;
create policy "cq_fornecedores_anon_select" on public.cq_fornecedores for select to anon using (true);
drop policy if exists "cq_fornecedores_insert" on public.cq_fornecedores;
create policy "cq_fornecedores_insert" on public.cq_fornecedores for insert to authenticated with check (true);
drop policy if exists "cq_fornecedores_update" on public.cq_fornecedores;
create policy "cq_fornecedores_update" on public.cq_fornecedores for update to authenticated using (true) with check (true);


-- 2) PRODUTOS
create table if not exists public.cq_produtos_v2 (
  id uuid primary key default gen_random_uuid(),
  codigo_atak text unique,
  codigo_interno text unique,
  nome text not null,
  nome_normalizado text not null,
  categoria text,
  unidade_medida text not null default 'UN',
  ativo boolean not null default true,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

create unique index if not exists uq_cq_produtos_v2_nome_normalizado
  on public.cq_produtos_v2 (nome_normalizado);

create index if not exists idx_cq_produtos_v2_ativo
  on public.cq_produtos_v2 (ativo) where ativo = true;

drop trigger if exists trg_cq_produtos_v2_atualizado on public.cq_produtos_v2;
create trigger trg_cq_produtos_v2_atualizado
  before update on public.cq_produtos_v2
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_produtos_v2 enable row level security;
drop policy if exists "cq_produtos_v2_select" on public.cq_produtos_v2;
create policy "cq_produtos_v2_select" on public.cq_produtos_v2 for select to authenticated using (true);
drop policy if exists "cq_produtos_v2_anon_select" on public.cq_produtos_v2;
create policy "cq_produtos_v2_anon_select" on public.cq_produtos_v2 for select to anon using (true);
drop policy if exists "cq_produtos_v2_insert" on public.cq_produtos_v2;
create policy "cq_produtos_v2_insert" on public.cq_produtos_v2 for insert to authenticated with check (true);
drop policy if exists "cq_produtos_v2_update" on public.cq_produtos_v2;
create policy "cq_produtos_v2_update" on public.cq_produtos_v2 for update to authenticated using (true) with check (true);


-- 3) DEFEITOS
create table if not exists public.cq_defeitos (
  id uuid primary key default gen_random_uuid(),
  codigo text unique not null,
  nome text not null,
  nome_normalizado text not null,
  descricao text,
  tipo text,
  criticidade_padrao text check (criticidade_padrao in ('baixa', 'media', 'alta', 'critica')),
  ativo boolean not null default true,
  ordem integer default 0,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

create unique index if not exists uq_cq_defeitos_nome_normalizado
  on public.cq_defeitos (nome_normalizado);

drop trigger if exists trg_cq_defeitos_atualizado on public.cq_defeitos;
create trigger trg_cq_defeitos_atualizado
  before update on public.cq_defeitos
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_defeitos enable row level security;
drop policy if exists "cq_defeitos_select" on public.cq_defeitos;
create policy "cq_defeitos_select" on public.cq_defeitos for select to authenticated using (true);
drop policy if exists "cq_defeitos_anon_select" on public.cq_defeitos;
create policy "cq_defeitos_anon_select" on public.cq_defeitos for select to anon using (true);
drop policy if exists "cq_defeitos_insert" on public.cq_defeitos;
create policy "cq_defeitos_insert" on public.cq_defeitos for insert to authenticated with check (true);
drop policy if exists "cq_defeitos_update" on public.cq_defeitos;
create policy "cq_defeitos_update" on public.cq_defeitos for update to authenticated using (true) with check (true);


-- 4) PARAMETROS POR PRODUTO
create table if not exists public.cq_parametros_produto (
  id uuid primary key default gen_random_uuid(),
  produto_id uuid not null references public.cq_produtos_v2(id) on delete cascade,
  perc_alerta numeric(8,4) not null check (perc_alerta >= 0),
  perc_reprovacao numeric(8,4) not null check (perc_reprovacao >= 0),
  exige_abc boolean not null default true,
  ativo boolean not null default true,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now(),
  constraint uq_cq_parametros_produto_ativo unique (produto_id, ativo)
);

drop trigger if exists trg_cq_parametros_produto_atualizado on public.cq_parametros_produto;
create trigger trg_cq_parametros_produto_atualizado
  before update on public.cq_parametros_produto
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_parametros_produto enable row level security;
drop policy if exists "cq_parametros_select" on public.cq_parametros_produto;
create policy "cq_parametros_select" on public.cq_parametros_produto for select to authenticated using (true);
drop policy if exists "cq_parametros_anon_select" on public.cq_parametros_produto;
create policy "cq_parametros_anon_select" on public.cq_parametros_produto for select to anon using (true);
drop policy if exists "cq_parametros_insert" on public.cq_parametros_produto;
create policy "cq_parametros_insert" on public.cq_parametros_produto for insert to authenticated with check (true);
drop policy if exists "cq_parametros_update" on public.cq_parametros_produto;
create policy "cq_parametros_update" on public.cq_parametros_produto for update to authenticated using (true) with check (true);


-- ************************************************************
-- FASE 2: NUCLEO OPERACIONAL
-- ************************************************************

-- 5) LOTES (entidade central)
create or replace function public.fn_hash_lote(
  p_numero_lote text,
  p_fornecedor_id uuid,
  p_produto_id uuid
) returns text language sql immutable as $$
  select md5(
    coalesce(p_numero_lote, '') || '|' ||
    coalesce(p_fornecedor_id::text, '') || '|' ||
    coalesce(p_produto_id::text, '')
  );
$$;

create table if not exists public.cq_lotes (
  id uuid primary key default gen_random_uuid(),
  numero_lote text not null,
  fornecedor_id uuid not null references public.cq_fornecedores(id),
  produto_id uuid not null references public.cq_produtos_v2(id),
  data_lote date,
  origem text not null default 'manual' check (origem in ('manual', 'atak', 'planilha', 'api')),
  hash_integridade text not null unique,
  observacoes text,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now(),
  constraint uq_cq_lote_negocio unique (numero_lote, fornecedor_id, produto_id)
);

create index if not exists idx_cq_lotes_fornecedor on public.cq_lotes (fornecedor_id);
create index if not exists idx_cq_lotes_produto on public.cq_lotes (produto_id);
create index if not exists idx_cq_lotes_data on public.cq_lotes (data_lote);

create or replace function public.fn_cq_lotes_preparar()
returns trigger language plpgsql as $$
begin
  new.hash_integridade := public.fn_hash_lote(new.numero_lote, new.fornecedor_id, new.produto_id);
  return new;
end;
$$;

drop trigger if exists trg_cq_lotes_preparar on public.cq_lotes;
create trigger trg_cq_lotes_preparar
  before insert or update on public.cq_lotes
  for each row execute function public.fn_cq_lotes_preparar();

drop trigger if exists trg_cq_lotes_atualizado on public.cq_lotes;
create trigger trg_cq_lotes_atualizado
  before update on public.cq_lotes
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_lotes enable row level security;
drop policy if exists "cq_lotes_select" on public.cq_lotes;
create policy "cq_lotes_select" on public.cq_lotes for select to authenticated using (true);
drop policy if exists "cq_lotes_anon_select" on public.cq_lotes;
create policy "cq_lotes_anon_select" on public.cq_lotes for select to anon using (true);
drop policy if exists "cq_lotes_insert" on public.cq_lotes;
create policy "cq_lotes_insert" on public.cq_lotes for insert to authenticated with check (true);
drop policy if exists "cq_lotes_update" on public.cq_lotes;
create policy "cq_lotes_update" on public.cq_lotes for update to authenticated using (true) with check (true);


-- 6) RECEBIMENTOS
create table if not exists public.cq_recebimentos_v2 (
  id uuid primary key default gen_random_uuid(),
  lote_id uuid not null references public.cq_lotes(id) on delete restrict,
  data_recebimento timestamptz not null,
  quantidade_recebida numeric(14,3) not null check (quantidade_recebida > 0),
  unidade_medida text not null default 'UN',
  peso_total numeric(14,3),
  nota_fiscal text,
  romaneio text,
  temperatura numeric(5,1),
  placa_caminhao text,
  motorista text,
  transportadora text,
  origem text not null default 'manual' check (origem in ('manual', 'atak', 'planilha', 'api')),
  importacao_id uuid,
  status_recebimento text not null default 'recebido'
    check (status_recebimento in ('recebido', 'em_inspecao', 'inspecionado', 'cancelado')),
  foto_etiqueta_url text,
  foto_carga_url text,
  observacoes text,
  criado_por uuid,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

create index if not exists idx_cq_recebimentos_v2_lote on public.cq_recebimentos_v2 (lote_id);
create index if not exists idx_cq_recebimentos_v2_data on public.cq_recebimentos_v2 (data_recebimento);
create index if not exists idx_cq_recebimentos_v2_status on public.cq_recebimentos_v2 (status_recebimento);

drop trigger if exists trg_cq_recebimentos_v2_atualizado on public.cq_recebimentos_v2;
create trigger trg_cq_recebimentos_v2_atualizado
  before update on public.cq_recebimentos_v2
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_recebimentos_v2 enable row level security;
drop policy if exists "cq_recebimentos_v2_select" on public.cq_recebimentos_v2;
create policy "cq_recebimentos_v2_select" on public.cq_recebimentos_v2 for select to authenticated using (true);
drop policy if exists "cq_recebimentos_v2_anon_select" on public.cq_recebimentos_v2;
create policy "cq_recebimentos_v2_anon_select" on public.cq_recebimentos_v2 for select to anon using (true);
drop policy if exists "cq_recebimentos_v2_insert" on public.cq_recebimentos_v2;
create policy "cq_recebimentos_v2_insert" on public.cq_recebimentos_v2 for insert to authenticated with check (true);
drop policy if exists "cq_recebimentos_v2_update" on public.cq_recebimentos_v2;
create policy "cq_recebimentos_v2_update" on public.cq_recebimentos_v2 for update to authenticated using (true) with check (true);


-- 7) INSPECOES
create table if not exists public.cq_inspecoes (
  id uuid primary key default gen_random_uuid(),
  recebimento_id uuid not null references public.cq_recebimentos_v2(id) on delete restrict,
  data_inspecao timestamptz not null default now(),
  quantidade_analisada numeric(14,3) not null check (quantidade_analisada > 0),
  total_defeitos numeric(14,3) not null default 0 check (total_defeitos >= 0),
  perc_defeitos numeric(10,4) not null default 0,
  classificacao_abc text check (classificacao_abc in ('A', 'B', 'C')),
  status_final text not null default 'pendente'
    check (status_final in ('pendente', 'aprovado', 'alerta', 'reprovado', 'revisao')),
  score_fornecedor numeric(10,4),
  observacoes text,
  override_ativo boolean not null default false,
  foto_etiqueta_url text,
  foto_produto_url text,
  foto_defeito_url text,
  video_evidencia_url text,
  criado_por uuid,
  revisado_por uuid,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now(),
  constraint ck_cq_inspecoes_total_defeitos_lte_qtd
    check (total_defeitos <= quantidade_analisada)
);

create unique index if not exists uq_cq_inspecao_por_recebimento
  on public.cq_inspecoes (recebimento_id);

create index if not exists idx_cq_inspecoes_data on public.cq_inspecoes (data_inspecao);
create index if not exists idx_cq_inspecoes_status on public.cq_inspecoes (status_final);
create index if not exists idx_cq_inspecoes_classificacao on public.cq_inspecoes (classificacao_abc);

drop trigger if exists trg_cq_inspecoes_atualizado on public.cq_inspecoes;
create trigger trg_cq_inspecoes_atualizado
  before update on public.cq_inspecoes
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_inspecoes enable row level security;
drop policy if exists "cq_inspecoes_select" on public.cq_inspecoes;
create policy "cq_inspecoes_select" on public.cq_inspecoes for select to authenticated using (true);
drop policy if exists "cq_inspecoes_anon_select" on public.cq_inspecoes;
create policy "cq_inspecoes_anon_select" on public.cq_inspecoes for select to anon using (true);
drop policy if exists "cq_inspecoes_insert" on public.cq_inspecoes;
create policy "cq_inspecoes_insert" on public.cq_inspecoes for insert to authenticated with check (true);
drop policy if exists "cq_inspecoes_update" on public.cq_inspecoes;
create policy "cq_inspecoes_update" on public.cq_inspecoes for update to authenticated using (true) with check (true);


-- 8) INSPECAO DEFEITOS (normalizado)
create table if not exists public.cq_inspecao_defeitos (
  id uuid primary key default gen_random_uuid(),
  inspecao_id uuid not null references public.cq_inspecoes(id) on delete cascade,
  defeito_id uuid not null references public.cq_defeitos(id) on delete restrict,
  quantidade numeric(14,3) not null check (quantidade >= 0),
  percentual numeric(10,4) not null default 0 check (percentual >= 0),
  criticidade text check (criticidade in ('baixa', 'media', 'alta', 'critica')),
  observacoes text,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now(),
  constraint uq_cq_inspecao_defeito unique (inspecao_id, defeito_id)
);

create index if not exists idx_cq_inspecao_defeitos_inspecao
  on public.cq_inspecao_defeitos (inspecao_id);

drop trigger if exists trg_cq_inspecao_defeitos_atualizado on public.cq_inspecao_defeitos;
create trigger trg_cq_inspecao_defeitos_atualizado
  before update on public.cq_inspecao_defeitos
  for each row execute function public.fn_set_atualizado_em();

alter table public.cq_inspecao_defeitos enable row level security;
drop policy if exists "cq_inspecao_defeitos_select" on public.cq_inspecao_defeitos;
create policy "cq_inspecao_defeitos_select" on public.cq_inspecao_defeitos for select to authenticated using (true);
drop policy if exists "cq_inspecao_defeitos_anon_select" on public.cq_inspecao_defeitos;
create policy "cq_inspecao_defeitos_anon_select" on public.cq_inspecao_defeitos for select to anon using (true);
drop policy if exists "cq_inspecao_defeitos_insert" on public.cq_inspecao_defeitos;
create policy "cq_inspecao_defeitos_insert" on public.cq_inspecao_defeitos for insert to authenticated with check (true);
drop policy if exists "cq_inspecao_defeitos_update" on public.cq_inspecao_defeitos;
create policy "cq_inspecao_defeitos_update" on public.cq_inspecao_defeitos for update to authenticated using (true) with check (true);


-- 9) CLASSIFICACAO DO RECEBIMENTO (A+B+C = qtd recebida)
create table if not exists public.cq_classificacoes_recebimento (
  id uuid primary key default gen_random_uuid(),
  recebimento_id uuid not null unique references public.cq_recebimentos_v2(id) on delete cascade,
  qtd_a numeric(14,3) not null default 0 check (qtd_a >= 0),
  qtd_b numeric(14,3) not null default 0 check (qtd_b >= 0),
  qtd_c numeric(14,3) not null default 0 check (qtd_c >= 0),
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

drop trigger if exists trg_cq_classificacoes_atualizado on public.cq_classificacoes_recebimento;
create trigger trg_cq_classificacoes_atualizado
  before update on public.cq_classificacoes_recebimento
  for each row execute function public.fn_set_atualizado_em();

-- Trigger: A + B + C = quantidade recebida (OBRIGATORIO)
create or replace function public.fn_validar_classificacao_recebimento()
returns trigger language plpgsql as $$
declare
  v_qtd_recebida numeric(14,3);
begin
  select quantidade_recebida
    into v_qtd_recebida
  from public.cq_recebimentos_v2
  where id = new.recebimento_id;

  if round(coalesce(new.qtd_a,0) + coalesce(new.qtd_b,0) + coalesce(new.qtd_c,0), 3)
     <> round(v_qtd_recebida, 3) then
    raise exception 'Classificacao inconsistente: A(%) + B(%) + C(%) = % <> quantidade recebida (%)',
      new.qtd_a, new.qtd_b, new.qtd_c,
      coalesce(new.qtd_a,0) + coalesce(new.qtd_b,0) + coalesce(new.qtd_c,0),
      v_qtd_recebida;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_validar_classificacao_recebimento on public.cq_classificacoes_recebimento;
create trigger trg_validar_classificacao_recebimento
  before insert or update on public.cq_classificacoes_recebimento
  for each row execute function public.fn_validar_classificacao_recebimento();

alter table public.cq_classificacoes_recebimento enable row level security;
drop policy if exists "cq_classificacoes_select" on public.cq_classificacoes_recebimento;
create policy "cq_classificacoes_select" on public.cq_classificacoes_recebimento for select to authenticated using (true);
drop policy if exists "cq_classificacoes_anon_select" on public.cq_classificacoes_recebimento;
create policy "cq_classificacoes_anon_select" on public.cq_classificacoes_recebimento for select to anon using (true);
drop policy if exists "cq_classificacoes_insert" on public.cq_classificacoes_recebimento;
create policy "cq_classificacoes_insert" on public.cq_classificacoes_recebimento for insert to authenticated with check (true);
drop policy if exists "cq_classificacoes_update" on public.cq_classificacoes_recebimento;
create policy "cq_classificacoes_update" on public.cq_classificacoes_recebimento for update to authenticated using (true) with check (true);


-- ************************************************************
-- FASE 3: GOVERNANCA
-- ************************************************************

-- 10) IMPORTACOES (ja existe, manter compatibilidade)
-- Tabela cq_importacoes ja existe no banco. Nao recriar.

-- 11) IMPORTACAO ERROS (ja existe)
-- Tabela cq_importacao_erros ja existe. Nao recriar.

-- 12) OVERRIDES
create table if not exists public.cq_overrides_v2 (
  id uuid primary key default gen_random_uuid(),
  inspecao_id uuid not null references public.cq_inspecoes(id) on delete cascade,
  classificacao_original text,
  classificacao_nova text not null check (classificacao_nova in ('A', 'B', 'C')),
  status_original text,
  status_novo text not null,
  justificativa text not null,
  realizado_por uuid,
  criado_em timestamptz not null default now(),
  constraint ck_cq_overrides_v2_justificativa_min
    check (char_length(trim(justificativa)) >= 10)
);

alter table public.cq_overrides_v2 enable row level security;
drop policy if exists "cq_overrides_v2_select" on public.cq_overrides_v2;
create policy "cq_overrides_v2_select" on public.cq_overrides_v2 for select to authenticated using (true);
drop policy if exists "cq_overrides_v2_insert" on public.cq_overrides_v2;
create policy "cq_overrides_v2_insert" on public.cq_overrides_v2 for insert to authenticated with check (true);

-- 13) AUDIT TRAIL
create table if not exists public.cq_audit_trail_v2 (
  id uuid primary key default gen_random_uuid(),
  tabela text not null,
  registro_id uuid not null,
  operacao text not null check (operacao in ('INSERT', 'UPDATE', 'DELETE')),
  antes jsonb,
  depois jsonb,
  realizado_por uuid,
  origem text,
  criado_em timestamptz not null default now()
);

create index if not exists idx_cq_audit_trail_v2_tabela_registro
  on public.cq_audit_trail_v2 (tabela, registro_id);
create index if not exists idx_cq_audit_trail_v2_criado
  on public.cq_audit_trail_v2 (criado_em);

alter table public.cq_audit_trail_v2 enable row level security;
drop policy if exists "cq_audit_v2_select" on public.cq_audit_trail_v2;
create policy "cq_audit_v2_select" on public.cq_audit_trail_v2 for select to authenticated using (true);
drop policy if exists "cq_audit_v2_insert" on public.cq_audit_trail_v2;
create policy "cq_audit_v2_insert" on public.cq_audit_trail_v2 for insert to authenticated with check (true);


-- ************************************************************
-- FASE 3b: FUNCOES CRITICAS
-- ************************************************************

-- Funcao de recalculo da inspecao (trigger nos defeitos)
create or replace function public.fn_recalcular_inspecao(p_inspecao_id uuid)
returns void language plpgsql as $$
declare
  v_qtd_analisada numeric(14,3);
  v_total_defeitos numeric(14,3);
  v_perc numeric(10,4);
  v_produto_id uuid;
  v_perc_alerta numeric(8,4);
  v_perc_reprovacao numeric(8,4);
  v_classificacao text;
  v_status text;
begin
  -- Buscar quantidade analisada e produto
  select i.quantidade_analisada, l.produto_id
    into v_qtd_analisada, v_produto_id
  from public.cq_inspecoes i
  join public.cq_recebimentos_v2 rec on rec.id = i.recebimento_id
  join public.cq_lotes l on l.id = rec.lote_id
  where i.id = p_inspecao_id;

  -- Somar defeitos
  select coalesce(sum(d.quantidade), 0)
    into v_total_defeitos
  from public.cq_inspecao_defeitos d
  where d.inspecao_id = p_inspecao_id;

  -- Calcular percentual
  v_perc := case
    when v_qtd_analisada > 0 then round((v_total_defeitos / v_qtd_analisada) * 100, 4)
    else 0
  end;

  -- Buscar parametros do produto
  select pp.perc_alerta, pp.perc_reprovacao
    into v_perc_alerta, v_perc_reprovacao
  from public.cq_parametros_produto pp
  where pp.produto_id = v_produto_id and pp.ativo = true
  limit 1;

  -- Fallback se nao tem parametro
  if v_perc_alerta is null then v_perc_alerta := 5.0; end if;
  if v_perc_reprovacao is null then v_perc_reprovacao := 15.0; end if;

  -- Classificar
  v_classificacao := case
    when v_perc >= v_perc_reprovacao then 'C'
    when v_perc >= v_perc_alerta then 'B'
    else 'A'
  end;

  v_status := case
    when v_perc >= v_perc_reprovacao then 'reprovado'
    when v_perc >= v_perc_alerta then 'alerta'
    else 'aprovado'
  end;

  -- Atualizar inspecao
  update public.cq_inspecoes
     set total_defeitos = v_total_defeitos,
         perc_defeitos = v_perc,
         classificacao_abc = v_classificacao,
         status_final = v_status,
         atualizado_em = now()
   where id = p_inspecao_id;
end;
$$;

-- Trigger que recalcula ao inserir/alterar/deletar defeito
create or replace function public.fn_trigger_recalcular_inspecao()
returns trigger language plpgsql as $$
begin
  perform public.fn_recalcular_inspecao(coalesce(new.inspecao_id, old.inspecao_id));
  return coalesce(new, old);
end;
$$;

drop trigger if exists trg_recalcular_inspecao_after_defeitos on public.cq_inspecao_defeitos;
create trigger trg_recalcular_inspecao_after_defeitos
  after insert or update or delete on public.cq_inspecao_defeitos
  for each row execute function public.fn_trigger_recalcular_inspecao();


-- ************************************************************
-- FASE 4: ANALITICA (views)
-- ************************************************************

-- View operacional completa
create or replace view public.vw_cq_recebimentos_completos as
select
  rec.id as recebimento_id,
  rec.data_recebimento,
  rec.quantidade_recebida,
  rec.status_recebimento,
  rec.temperatura,
  rec.placa_caminhao,
  rec.motorista,
  l.id as lote_id,
  l.numero_lote,
  l.data_lote,
  l.origem,
  f.id as fornecedor_id,
  f.nome as fornecedor_nome,
  f.codigo_atak as fornecedor_codigo,
  p.id as produto_id,
  p.nome as produto_nome,
  p.codigo_atak as produto_codigo,
  i.id as inspecao_id,
  i.quantidade_analisada,
  i.total_defeitos,
  i.perc_defeitos,
  i.classificacao_abc,
  i.status_final,
  cl.qtd_a,
  cl.qtd_b,
  cl.qtd_c
from public.cq_recebimentos_v2 rec
join public.cq_lotes l on l.id = rec.lote_id
join public.cq_fornecedores f on f.id = l.fornecedor_id
join public.cq_produtos_v2 p on p.id = l.produto_id
left join public.cq_inspecoes i on i.recebimento_id = rec.id
left join public.cq_classificacoes_recebimento cl on cl.recebimento_id = rec.id;


-- Ranking por fornecedor
create or replace view public.vw_cq_ranking_fornecedores as
select
  f.id as fornecedor_id,
  f.nome as fornecedor_nome,
  f.codigo_atak,
  count(distinct rec.id) as total_recebimentos,
  count(distinct i.id) as total_inspecoes,
  round(avg(i.perc_defeitos), 4) as media_perc_defeitos,
  sum(case when i.classificacao_abc = 'A' then 1 else 0 end) as total_a,
  sum(case when i.classificacao_abc = 'B' then 1 else 0 end) as total_b,
  sum(case when i.classificacao_abc = 'C' then 1 else 0 end) as total_c
from public.cq_fornecedores f
left join public.cq_lotes l on l.fornecedor_id = f.id
left join public.cq_recebimentos_v2 rec on rec.lote_id = l.id
left join public.cq_inspecoes i on i.recebimento_id = rec.id
where f.ativo = true
group by f.id, f.nome, f.codigo_atak;


-- Tendencia 30 dias
create or replace view public.vw_cq_tendencia_30d as
select
  date_trunc('day', rec.data_recebimento) as dia,
  p.nome as produto,
  f.nome as fornecedor,
  count(distinct rec.id) as recebimentos,
  round(avg(i.perc_defeitos), 4) as media_perc_defeitos,
  sum(case when i.classificacao_abc = 'A' then 1 else 0 end) as qtd_a,
  sum(case when i.classificacao_abc = 'B' then 1 else 0 end) as qtd_b,
  sum(case when i.classificacao_abc = 'C' then 1 else 0 end) as qtd_c
from public.cq_recebimentos_v2 rec
join public.cq_lotes l on l.id = rec.lote_id
join public.cq_fornecedores f on f.id = l.fornecedor_id
join public.cq_produtos_v2 p on p.id = l.produto_id
left join public.cq_inspecoes i on i.recebimento_id = rec.id
where rec.data_recebimento >= now() - interval '30 days'
group by 1, 2, 3;


-- Defeitos por fornecedor
create or replace view public.vw_cq_defeitos_fornecedor as
select
  f.id as fornecedor_id,
  f.nome as fornecedor_nome,
  d.id as defeito_id,
  d.nome as defeito_nome,
  d.codigo as defeito_codigo,
  sum(id2.quantidade) as total_quantidade,
  round(avg(id2.percentual), 4) as media_percentual
from public.cq_inspecao_defeitos id2
join public.cq_inspecoes i on i.id = id2.inspecao_id
join public.cq_recebimentos_v2 rec on rec.id = i.recebimento_id
join public.cq_lotes l on l.id = rec.lote_id
join public.cq_fornecedores f on f.id = l.fornecedor_id
join public.cq_defeitos d on d.id = id2.defeito_id
group by f.id, f.nome, d.id, d.nome, d.codigo;


-- ************************************************************
-- FASE 4b: COMENTARIO FINAL
-- ************************************************************
-- Este script cria a estrutura NOVA (v2) ao lado da estrutura existente.
-- Tabelas com sufixo _v2 evitam conflito com as existentes.
-- Apos validar, migrar dados das tabelas antigas para as novas.
-- Tabelas novas: cq_fornecedores, cq_produtos_v2, cq_defeitos,
--   cq_parametros_produto, cq_lotes, cq_recebimentos_v2,
--   cq_inspecoes, cq_inspecao_defeitos, cq_classificacoes_recebimento,
--   cq_overrides_v2, cq_audit_trail_v2
-- Views: vw_cq_recebimentos_completos, vw_cq_ranking_fornecedores,
--   vw_cq_tendencia_30d, vw_cq_defeitos_fornecedor
