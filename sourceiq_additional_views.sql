-- ============================================================
-- SOURCEIQ — ADDITIONAL BI VIEWS (Delta from production schema)
-- Add these to sourceiq_production_schema.sql
-- ============================================================

-- fact_inspections: non-aggregated base dataset for all quality views
create or replace view public.fact_inspections as
select
  i.id                                    as inspection_id,
  i.lot_id,
  l.internal_lot_code                     as lot_code,
  l.atak_lot_code,
  coalesce(s.name,'[SEM FORNECEDOR]')     as supplier_name,
  s.id                                    as supplier_id,
  coalesce(p.name,'[SEM PRODUTO]')        as product_name,
  p.id                                    as product_id,
  p.category                              as product_category,
  pr.full_name                            as inspector_name,
  i.inspection_date,
  i.inspection_date::date                 as inspection_day,
  date_trunc('month',i.inspection_date)::date as inspection_month,
  i.inspection_mode,
  i.inspected_quantity,
  i.defective_quantity,
  i.defect_percent,
  i.final_status,
  i.threshold_restricted_pct,
  i.threshold_blocked_pct,
  i.notes,
  i.data_source,
  i.source_reference,
  i.is_reconciled,
  i.reconciliation_status,
  i.created_at
from public.inspections i
left join public.lots l       on l.id = i.lot_id
left join public.suppliers s  on s.id = l.supplier_id
left join public.products p   on p.id = l.product_id
left join public.profiles pr  on pr.id = i.inspector_id;

-- fact_receipts: non-aggregated base for logistics views
create or replace view public.fact_receipts as
select
  r.id                                    as receipt_id,
  r.document_number,
  r.pcr_date,
  r.pcr_date::date                        as event_day,
  date_trunc('month',r.pcr_date)::date    as event_month,
  coalesce(d.name, r.driver_name,'[SEM MOTORISTA]') as driver_name,
  d.id                                    as driver_id,
  coalesce(s.name, r.supplier_name,'[SEM FORNECEDOR]') as supplier_name,
  s.id                                    as supplier_id,
  coalesce(v.plate, r.vehicle_plate,'[SEM PLACA]') as vehicle_plate,
  v.id                                    as vehicle_id,
  coalesce(p.name, r.product_name)        as product_name,
  p.id                                    as product_id,
  r.origin_city,
  r.quantity_expected,
  r.quantity_received,
  r.quantity_difference,
  r.lot_id,
  r.is_reconciled,
  r.reconciliation_status,
  r.data_source,
  r.source_reference
from public.atak_receipts r
left join public.drivers d    on d.id = r.driver_id
left join public.suppliers s  on s.id = r.supplier_id
left join public.vehicles v   on v.id = r.vehicle_id
left join public.products p   on p.id = r.product_id;

-- fact_nc: non-conformity base view with action plan status
create or replace view public.fact_nc as
select
  nc.id                                   as nc_id,
  nc.code                                 as nc_code,
  coalesce(s.name,'[SEM FORNECEDOR]')     as supplier_name,
  s.id                                    as supplier_id,
  l.internal_lot_code                     as lot_code,
  nc.inspection_id,
  nc.severity,
  nc.status                               as nc_status,
  nc.title,
  nc.description,
  nc.created_at::date                     as created_day,
  date_trunc('month',nc.created_at)::date as created_month,
  extract(day from now() - nc.created_at) as days_open,
  sap.status                              as action_plan_status,
  sap.deadline                            as action_plan_deadline,
  case
    when sap.id is null then 'no_plan'
    when sap.deadline < current_date and sap.status not in ('completed','cancelled') then 'overdue'
    else sap.status
  end                                     as action_plan_health,
  nc.is_reconciled,
  nc.data_source,
  nc.source_reference
from public.non_conformities nc
left join public.suppliers s  on s.id = nc.supplier_id
left join public.lots l       on l.id = nc.lot_id
left join public.supplier_action_plans sap on sap.non_conformity_id = nc.id;

-- v_lots_pending_inspection: lots received but not yet inspected
create or replace view public.v_lots_pending_inspection as
select
  l.id                                    as lot_id,
  l.internal_lot_code,
  l.atak_lot_code,
  coalesce(s.name,'[SEM FORNECEDOR]')     as supplier_name,
  coalesce(p.name,'[SEM PRODUTO]')        as product_name,
  l.receipt_date::date                    as receipt_day,
  extract(day from now() - l.receipt_date) as days_since_receipt,
  l.total_quantity,
  l.available_quantity,
  l.status,
  l.data_source
from public.lots l
left join public.suppliers s  on s.id = l.supplier_id
left join public.products p   on p.id = l.product_id
where l.status = 'received'
  and not exists (
    select 1 from public.inspections i
    where i.lot_id = l.id
  )
order by l.receipt_date;

-- v_supplier_nc_summary: supplier NC performance summary
create or replace view public.v_supplier_nc_summary as
select
  coalesce(s.name,'[SEM FORNECEDOR]')     as supplier_name,
  s.id                                    as supplier_id,
  count(*) filter (where nc.status not in ('closed','cancelled')) as open_nc_count,
  count(*) filter (where nc.status in ('closed'))                 as closed_nc_count,
  round(avg(
    extract(day from coalesce(nc.resolved_at, now()) - nc.created_at)
  ) filter (where nc.status = 'closed'), 1)                       as avg_days_to_close,
  count(*) filter (where sap.id is null
    and nc.status not in ('closed','cancelled'))                   as nc_without_plan_count,
  max(nc.created_at)::date                                         as last_nc_date
from public.non_conformities nc
left join public.suppliers s  on s.id = nc.supplier_id
left join public.supplier_action_plans sap on sap.non_conformity_id = nc.id
group by s.id, coalesce(s.name,'[SEM FORNECEDOR]')
order by open_nc_count desc;

-- v_defect_frequency: defect type ranking
create or replace view public.v_defect_frequency as
select
  dt.code                                 as defect_code,
  dt.name                                 as defect_name,
  dt.category,
  dt.severity_default,
  dt.affects_discount,
  count(ii.id)                            as inspection_count,
  round(sum(coalesce(ii.quantity_affected,0)),2) as total_quantity_affected,
  round(avg(coalesce(ii.quantity_affected,0)),2) as avg_quantity_per_inspection,
  round(
    count(ii.id)::numeric /
    nullif((select count(*) from public.inspections),0) * 100, 4
  )                                       as pct_of_total_inspections
from public.defect_types dt
left join public.inspection_items ii on ii.defect_type_id = dt.id
where dt.is_active = true
group by dt.id, dt.code, dt.name, dt.category, dt.severity_default, dt.affects_discount
order by total_quantity_affected desc nulls last;

-- v_reconciliation_rate_by_domain: data reliability per domain per month
create or replace view public.v_reconciliation_rate_by_domain as
select
  'quality'    as domain,
  date_trunc('month',inspection_date)::date as reference_month,
  count(*)     as total_records,
  count(*) filter (where is_reconciled=true) as reconciled_records,
  round(count(*) filter (where is_reconciled=true)::numeric / nullif(count(*),0) * 100, 2)
               as reconciliation_rate_pct
from public.inspections
group by date_trunc('month',inspection_date)::date
union all
select 'logistics',
  date_trunc('month',pcr_date)::date,
  count(*),
  count(*) filter (where is_reconciled=true),
  round(count(*) filter (where is_reconciled=true)::numeric / nullif(count(*),0) * 100, 2)
from public.atak_receipts where pcr_date is not null
group by date_trunc('month',pcr_date)::date
union all
select 'production',
  date_trunc('month',production_date)::date,
  count(*),
  count(*) filter (where is_reconciled=true),
  round(count(*) filter (where is_reconciled=true)::numeric / nullif(count(*),0) * 100, 2)
from public.employee_production_atak where production_date is not null
group by date_trunc('month',production_date)::date
union all
select 'commercial',
  date_trunc('month',sale_date)::date,
  count(*),
  count(*) filter (where is_reconciled=true),
  round(count(*) filter (where is_reconciled=true)::numeric / nullif(count(*),0) * 100, 2)
from public.sales where sale_date is not null
group by date_trunc('month',sale_date)::date
union all
select 'financial',
  date_trunc('month',transaction_date)::date,
  count(*),
  count(*) filter (where is_reconciled=true),
  round(count(*) filter (where is_reconciled=true)::numeric / nullif(count(*),0) * 100, 2)
from public.financial_transactions where transaction_date is not null
group by date_trunc('month',transaction_date)::date
order by reference_month desc, domain;

-- v_financial_cash_forecast: rolling 4-week cash position
create or replace view public.v_financial_cash_forecast as
select
  date_trunc('week', due_date)::date      as week_start,
  round(sum(case when flow_type='receivable'
    then amount-amount_paid else 0 end),2) as receivable_due,
  round(sum(case when flow_type='payable'
    then amount-amount_paid else 0 end),2) as payable_due,
  round(
    sum(case when flow_type='receivable' then amount-amount_paid else -(amount-amount_paid) end),2
  )                                        as net_forecast
from public.financial_transactions
where due_date between current_date and current_date + interval '28 days'
  and status not in ('paid','cancelled')
group by date_trunc('week', due_date)::date
order by week_start;

-- v_commercial_client_health: client activity and risk flags
create or replace view public.v_commercial_client_health as
select
  coalesce(c.name, s.client_name_raw)     as client_name,
  c.id                                    as client_id,
  count(s.id)                             as total_orders_6m,
  round(sum(s.net_value),2)               as total_revenue_6m,
  round(avg(s.estimated_margin_pct),4)    as avg_margin_pct_6m,
  max(s.sale_date)                        as last_order_date,
  extract(day from now() - max(s.sale_date)) as days_since_last_order,
  case
    when max(s.sale_date) < current_date - interval '30 days' then true
    else false
  end                                      as is_at_risk,
  round(sum(s.quantity),2)                as total_volume_6m
from public.sales s
left join public.clients c on c.id = s.client_id
where s.sale_date >= current_date - interval '6 months'
group by coalesce(c.name, s.client_name_raw), c.id
order by total_revenue_6m desc;

-- ============================================================
-- MATERIALIZED VIEW CANDIDATES
-- Run these after base views are confirmed working.
-- ============================================================

-- Materialized quality tactical (refresh after reconciliation)
create materialized view if not exists public.mv_bi_quality_tactical as
select * from public.v_bi_quality_tactical;
create unique index if not exists mv_quality_tactical_supplier
  on public.mv_bi_quality_tactical(supplier_name);

-- Materialized intelligence (refresh nightly)
create materialized view if not exists public.mv_bi_intelligence_supplier_impact as
select * from public.v_bi_intelligence_supplier_impact;
create unique index if not exists mv_intelligence_supplier
  on public.mv_bi_intelligence_supplier_impact(supplier_name);

-- Refresh functions (called by application after reconciliation run or import commit)
create or replace function public.fn_refresh_quality_mv()
returns void language sql security definer as $$
  refresh materialized view concurrently public.mv_bi_quality_tactical;
$$;

create or replace function public.fn_refresh_intelligence_mv()
returns void language sql security definer as $$
  refresh materialized view concurrently public.mv_bi_intelligence_supplier_impact;
$$;

-- RLS for new views (authenticated read)
create policy "read_fact_inspections"  on public.inspections  for select to authenticated using (true);
create policy "read_fact_receipts"     on public.atak_receipts for select to authenticated using (true);
