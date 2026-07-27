-- ============================================================
-- MIGRATION: RNC (Registro de Não Conformidade ao Fornecedor)
-- Data: 2026-06-20
-- Objetivo: habilitar o fluxo de RNC ao fornecedor sobre a tabela
--   existente cq_nao_conformidades (tipo='fornecedor'):
--   - campos de envio/resposta do fornecedor
--   - contato/e-mail no cadastro de fornecedores (para envio)
--   - função de geração do código RNC-AAAAMMDD-NNN
--
-- EXECUTAR NO SUPABASE SQL EDITOR (projeto Classic-ia CQ)
-- Depende de: cq_nao_conformidades, cq_fornecedores (01_migration_novo_modelo_cq.sql)
-- ============================================================

-- 1) Campos de envio/resposta na NC (idempotente)
alter table public.cq_nao_conformidades add column if not exists prazo_resposta date;
alter table public.cq_nao_conformidades add column if not exists enviado_fornecedor boolean not null default false;
alter table public.cq_nao_conformidades add column if not exists data_envio timestamptz;
alter table public.cq_nao_conformidades add column if not exists resposta_fornecedor text;
alter table public.cq_nao_conformidades add column if not exists respondido_em timestamptz;

-- 2) Contato do fornecedor (para gerar/enviar o RNC)
alter table public.cq_fornecedores add column if not exists email text;
alter table public.cq_fornecedores add column if not exists contato text;
alter table public.cq_fornecedores add column if not exists telefone text;

-- 3) Geração do código RNC-AAAAMMDD-NNN (sequencial por dia)
create or replace function public.fn_proximo_codigo_rnc()
returns text language plpgsql as $$
declare
  v_data text := to_char(now(), 'YYYYMMDD');
  v_seq  int;
begin
  select coalesce(max(split_part(codigo, '-', 3)::int), 0) + 1
    into v_seq
  from public.cq_nao_conformidades
  where codigo like 'RNC-' || v_data || '-%';
  return 'RNC-' || v_data || '-' || lpad(v_seq::text, 3, '0');
end;
$$;

-- RPC chamável pelo frontend (PostgREST) para obter o próximo código
create or replace function public.cq_proximo_codigo_rnc()
returns text language sql security definer as $$
  select public.fn_proximo_codigo_rnc();
$$;

-- 4) View: RNCs (não conformidades direcionadas a fornecedor) com nome do fornecedor
create or replace view public.cq_rnc as
select n.*, f.nome as fornecedor_nome, f.email as fornecedor_email,
       f.contato as fornecedor_contato, f.cidade as fornecedor_cidade
from public.cq_nao_conformidades n
left join public.cq_fornecedores f on f.codigo_atak = n.fornecedor_codigo
where n.tipo = 'fornecedor';
