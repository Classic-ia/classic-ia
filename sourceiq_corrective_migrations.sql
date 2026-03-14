-- ============================================================
-- SOURCEIQ — CORRECTIVE MIGRATIONS (from Strict Architecture Review)
-- Apply AFTER sourceiq_hardening_migrations.sql
-- These 6 new migrations fix issues found in the second review pass.
-- Numbered 012–017, continuing from the 011 hardening migrations.
-- ============================================================

-- ============================================================
-- MIGRATION 012: fn_commit_import() — Atomic Import Promotion
-- Fixes: Issue #1 (CRITICAL) — function referenced everywhere but never implemented
-- ============================================================
create or replace function public.fn_commit_import(p_import_file_id uuid)
returns jsonb language plpgsql security definer as $$
declare
  v_entity   text;
  v_promoted integer := 0;
  v_rejected integer;
begin
  -- Guard: file must be in committable state
  if not exists (
    select 1 from public.import_files where id = p_import_file_id
    and status in ('validated','validated_with_errors')
  ) then
    raise exception 'import_wrong_status: file % cannot be committed',
      p_import_file_id using errcode = 'P0010';
  end if;

  select entity_type into v_entity
  from public.import_files where id = p_import_file_id;

  -- ── ENTITY: receipt_load_closure ──────────────────────────────
  if v_entity = 'receipt_load_closure' then
    insert into public.receipt_load_closures (
      driver_name, closure_date, collection_date, city,
      supplier_name_raw, storage_type,
      frigo_count, classic_count,
      difference_count, difference_percent,
      difference_count_source,
      torn_count, pierced_count, peeling_count, no_leaf_count, dirty_count,
      data_source, source_reference, import_file_id
    )
    select
      raw_fields->>'driver_name',
      (raw_fields->>'closure_date')::date,
      (raw_fields->>'collection_date')::date,
      raw_fields->>'city',
      raw_supplier,
      raw_fields->>'storage_type',
      (raw_fields->>'frigo_count')::numeric,
      (raw_fields->>'classic_count')::numeric,
      (raw_fields->>'difference_count')::numeric,
      (raw_fields->>'difference_percent')::numeric,
      (raw_fields->>'difference_count_source')::numeric,
      (raw_fields->>'torn_count')::numeric,
      (raw_fields->>'pierced_count')::numeric,
      (raw_fields->>'peeling_count')::numeric,
      (raw_fields->>'no_leaf_count')::numeric,
      (raw_fields->>'dirty_count')::numeric,
      data_source, source_reference, p_import_file_id
    from public.staging_quality_import
    where import_file_id = p_import_file_id
    and   validation_status in ('valid','warning');
    get diagnostics v_promoted = row_count;

  -- ── ENTITY: load_classification ───────────────────────────────
  elsif v_entity = 'load_classification' then
    insert into public.load_quality_classifications (
      sheet_name, arrival_date, supplier_name_raw,
      frigo_count, classic_count,
      class_a_count, class_b_count, class_c_count, repass_count,
      class_a_percent, class_b_percent, class_c_percent,
      has_formula_mismatch,
      data_source, source_reference, import_file_id
    )
    select
      raw_fields->>'sheet_name',
      (raw_fields->>'arrival_date')::date,
      raw_supplier,
      (raw_fields->>'frigo_count')::numeric,
      (raw_fields->>'classic_count')::numeric,
      (raw_fields->>'class_a_count')::numeric,
      (raw_fields->>'class_b_count')::numeric,
      (raw_fields->>'class_c_count')::numeric,
      (raw_fields->>'repass_count')::numeric,
      (raw_fields->>'class_a_percent')::numeric,
      (raw_fields->>'class_b_percent')::numeric,
      (raw_fields->>'class_c_percent')::numeric,
      coalesce((raw_fields->>'has_formula_mismatch')::boolean, false),
      data_source, source_reference, p_import_file_id
    from public.staging_quality_import
    where import_file_id = p_import_file_id
    and   validation_status in ('valid','warning');
    get diagnostics v_promoted = row_count;

  -- ── ENTITY: employee_production ───────────────────────────────
  elsif v_entity = 'employee_production' then
    insert into public.employee_production_logs (
      sector_id, sector_name_raw, employee_name, employee_code,
      week_reference, pieces_produced, hours_worked, pieces_per_hour,
      data_source, source_reference, import_file_id
    )
    select
      ps.id,  -- FK resolved by normalized name lookup
      raw_fields->>'sector_name_raw',
      raw_fields->>'employee_name',
      raw_fields->>'employee_code',
      raw_fields->>'week_reference',
      (raw_fields->>'pieces_produced')::numeric,
      (raw_fields->>'hours_worked')::numeric,
      (raw_fields->>'pieces_per_hour')::numeric,
      data_source, source_reference, p_import_file_id
    from public.staging_quality_import sq
    left join public.production_sectors ps
      on ps.name_normalized = lower(regexp_replace(
        translate(raw_fields->>'sector_name_raw',
          'áàâãéèêíìîóòôõúùûçÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ',
          'aaaaeeeiiioooouuucAAAAEEEIIIOOOOUUUC'),
        '\s+', ' ', 'g'))
    where sq.import_file_id = p_import_file_id
    and   sq.validation_status in ('valid','warning');
    get diagnostics v_promoted = row_count;

  -- ── ENTITY: receipt_requisition (two-pass: receipts then defects) ─
  elsif v_entity = 'receipt_requisition' then
    with inserted_receipts as (
      insert into public.atak_receipts (
        document_number, pcr_date, driver_name, supplier_name,
        vehicle_plate, origin_city, product_name,
        quantity_expected, quantity_received, qty_expected_missing,
        data_source, source_reference, import_file_id
      )
      select
        raw_fields->>'document_number',
        (raw_fields->>'pcr_date')::date,
        raw_fields->>'driver_name',
        raw_fields->>'supplier_name',
        raw_fields->>'vehicle_plate',
        raw_fields->>'origin_city',
        raw_fields->>'product_name',
        (raw_fields->>'quantity_expected')::numeric,
        (raw_fields->>'quantity_received')::numeric,
        coalesce((raw_fields->>'qty_expected_missing')::boolean, false),
        data_source, source_reference, p_import_file_id
      from public.staging_quality_import
      where import_file_id = p_import_file_id
      and   validation_status in ('valid','warning')
      returning id, document_number, pcr_date
    ),
    defect_source as (
      select
        ir.id as receipt_id,
        jsonb_array_elements(sq.raw_fields->'defects') as defect
      from public.staging_quality_import sq
      join inserted_receipts ir
        on ir.document_number = sq.raw_fields->>'document_number'
        and ir.pcr_date = (sq.raw_fields->>'pcr_date')::date
      where sq.import_file_id = p_import_file_id
    )
    insert into public.atak_quality_defects (
      receipt_id, defect_name_raw, quantity, data_source, source_reference)
    select
      receipt_id,
      defect->>'defect_name_raw',
      (defect->>'quantity')::numeric,
      'atak',
      p_import_file_id::text
    from defect_source
    where defect->>'quantity' is not null;

    -- Row count = receipts (defects are bonus rows)
    select count(*) into v_promoted from public.staging_quality_import
    where import_file_id = p_import_file_id
    and   validation_status in ('valid','warning');

  -- ── ENTITY: atak_employee_production ──────────────────────────
  elsif v_entity = 'atak_employee_production' then
    insert into public.employee_production_atak (
      production_date, employee_code, employee_name,
      product_code, product_name_raw,
      hours_worked, quantity_produced, quantity_per_hour,
      data_source, source_reference, import_file_id
    )
    select
      (raw_fields->>'production_date')::date,
      raw_fields->>'employee_code',
      raw_fields->>'employee_name',
      raw_fields->>'product_code',
      raw_fields->>'product_name_raw',
      (raw_fields->>'hours_worked')::numeric,
      (raw_fields->>'quantity_produced')::numeric,
      (raw_fields->>'quantity_per_hour')::numeric,
      data_source, source_reference, p_import_file_id
    from public.staging_quality_import
    where import_file_id = p_import_file_id
    and   validation_status in ('valid','warning');
    get diagnostics v_promoted = row_count;
  end if;

  -- Count rejected rows
  select count(*) into v_rejected
  from public.staging_quality_import
  where import_file_id = p_import_file_id
  and   validation_status = 'invalid';

  -- Mark staged rows as imported
  update public.staging_quality_import
  set validation_status = 'imported'
  where import_file_id = p_import_file_id
  and   validation_status in ('valid','warning');

  -- Update import_files in the same transaction
  update public.import_files
  set status        = case when v_rejected = 0 then 'imported' else 'imported_with_errors' end,
      imported_rows = v_promoted,
      rejected_rows = v_rejected,
      committed_at  = now(),
      committed_by  = auth.uid()
  where id = p_import_file_id;

  return jsonb_build_object('promoted', v_promoted, 'rejected', v_rejected);
  -- Any exception above rolls back everything. No partial commits.
end;
$$;

-- ============================================================
-- MIGRATION 013: mv_bi_executive_overview (materialized view)
-- Fixes: Issue #3 (CRITICAL) — documented as MV but only regular view exists
-- ============================================================
create materialized view if not exists public.mv_bi_executive_overview as
select * from public.v_bi_executive_overview;

create or replace function public.fn_refresh_executive_mv(
  trigger_source text default 'manual')
returns void language plpgsql security definer as $$
declare v_start timestamptz := clock_timestamp();
begin
  refresh materialized view public.mv_bi_executive_overview;
  insert into public.mv_refresh_log(view_name, duration_ms, row_count, triggered_by)
  values('mv_bi_executive_overview',
    extract(milliseconds from clock_timestamp()-v_start)::int,
    1,  -- single-row view
    trigger_source);
exception when others then
  insert into public.mv_refresh_log(view_name, error_msg, triggered_by)
  values('mv_bi_executive_overview', sqlerrm, trigger_source);
  raise;
end;
$$;

-- pg_cron setup — requires Supabase Pro plan. Run manually after plan confirmation:
-- select cron.schedule(
--   'sourceiq_executive_mv_refresh',
--   '*/5 * * * *',
--   $$select public.fn_refresh_executive_mv('scheduled')$$
-- );

-- ============================================================
-- MIGRATION 014: production_sectors.name_normalized for fuzzy matching
-- Fixes: Issue #4 (HIGH) — accent variants produce NULL sector_id
-- ============================================================
alter table public.production_sectors
  add column if not exists name_normalized text;

update public.production_sectors
set name_normalized = lower(regexp_replace(
  translate(sector_name,
    'áàâãéèêíìîóòôõúùûçÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ',
    'aaaaeeeiiioooouuucAAAAEEEIIIOOOOUUUC'),
  '\s+', ' ', 'g'));

create unique index if not exists idx_sectors_normalized
  on public.production_sectors(name_normalized);

alter table public.production_sectors
  alter column name_normalized set not null;

-- Trigger to keep name_normalized in sync
create or replace function public.fn_normalize_sector_name()
returns trigger language plpgsql as $$
begin
  new.name_normalized := lower(regexp_replace(
    translate(new.sector_name,
      'áàâãéèêíìîóòôõúùûçÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ',
      'aaaaeeeiiioooouuucAAAAEEEIIIOOOOUUUC'),
    '\s+', ' ', 'g'));
  return new;
end;
$$;

create or replace trigger trg_normalize_sector_name
  before insert or update of sector_name
  on public.production_sectors
  for each row execute function public.fn_normalize_sector_name();

-- ============================================================
-- MIGRATION 015: cost_parameters reference_month CHECK constraint
-- Fixes: Issue #6 (HIGH) — non-first-day dates create unfindable rows
-- ============================================================
alter table public.cost_parameters
  add constraint cost_params_first_day_of_month
    check (reference_month = date_trunc('month', reference_month)::date);

-- ============================================================
-- MIGRATION 016: Receipt divergence auto-alert trigger
-- Fixes: Issue #7 (HIGH) — alerts documented but never auto-created
-- ============================================================
create or replace function public.fn_alert_on_receipt_divergence()
returns trigger language plpgsql security definer as $$
declare
  v_pct       numeric;
  v_threshold numeric := 0.05;  -- 5% — adjust via reconciliation_tolerances if needed
begin
  if new.quantity_expected is null or new.quantity_expected = 0 then
    return new;
  end if;

  v_pct := abs(coalesce(new.quantity_received, 0) - new.quantity_expected)
           / new.quantity_expected;

  if v_pct > v_threshold then
    insert into public.dashboard_alerts (
      dedup_key, alert_type, severity, area, title, body,
      reference_id, reference_table
    ) values (
      'receipt_divergence_' || coalesce(new.document_number, new.id::text),
      'receipt_divergence',
      case when v_pct > 0.15 then 'high' else 'medium' end,
      'logistics',
      'Divergência de recebimento: ' || coalesce(new.document_number, '?'),
      'Motorista: ' || coalesce(new.driver_name, '?') || '. ' ||
        'Divergência: ' || round(v_pct * 100, 1)::text || '%.',
      new.id,
      'atak_receipts'
    ) on conflict (dedup_key) do nothing;
  end if;
  return new;
end;
$$;

create or replace trigger trg_alert_receipt_divergence
  after insert on public.atak_receipts
  for each row execute function public.fn_alert_on_receipt_divergence();

-- ============================================================
-- MIGRATION 017: v_sector_productivity is_reconciled filter
-- Fixes: Issue #8 (HIGH) — tactical production view uses unconfirmed data
-- ============================================================
create or replace view public.v_sector_productivity as
select
  coalesce(ps.sector_name, epl.sector_name_raw)   as sector_name,
  epl.week_reference,
  sum(coalesce(epl.pieces_produced, 0))            as total_pieces,
  sum(coalesce(epl.hours_worked, 0))               as total_hours,
  case when sum(coalesce(epl.hours_worked, 0)) > 0
    then round(sum(coalesce(epl.pieces_produced,0)) / sum(coalesce(epl.hours_worked,0)), 4)
    else 0 end                                     as pieces_per_hour,
  count(distinct epl.employee_code)                as employee_count
from public.employee_production_logs epl
left join public.production_sectors ps on ps.id = epl.sector_id
where epl.is_reconciled = true   -- ADDED: was missing, causing unconfirmed data in production BI
group by coalesce(ps.sector_name, epl.sector_name_raw), epl.week_reference
order by epl.week_reference desc, pieces_per_hour desc;

-- ============================================================
-- PATCH: fn_current_role() — also patch base schema
-- Fixes: Issue #2 — base schema lacks search_path and null guard
-- This migration is idempotent; safe to run even if 004 already applied.
-- ============================================================
create or replace function public.fn_current_role()
returns public.user_role
language sql stable security definer
set search_path = public
as $$
  select coalesce(
    (select role from public.profiles where id = auth.uid()),
    'viewer'::public.user_role
  );
$$;

revoke execute on function public.fn_current_role() from public;
grant  execute on function public.fn_current_role() to authenticated;
