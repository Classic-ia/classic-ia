// ============================================================
// Edge Function: bi-jr
// Endpoint publico para BI externo (Metabase, Power BI, n8n).
// Autenticacao via header x-api-key (segredo em JR_BI_API_KEY).
// Entrega JSON agregado consultando as views analiticas.
//
// Rotas (via query ?r=):
//   ?r=dre&ini=YYYY-MM-DD&fim=YYYY-MM-DD[&cliente=UUID][&maquina=UUID]
//   ?r=os&ini=...&fim=...
//   ?r=maquinas
//   ?r=clientes
//   ?r=horimetros
//   ?r=deslocamentos
//   ?r=resumo&ini=...&fim=...
// ============================================================
import { serve } from 'https://deno.land/std@0.177.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SERVICE_KEY  = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const BI_API_KEY   = Deno.env.get('JR_BI_API_KEY') || '';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-api-key, content-type',
  'Access-Control-Allow-Methods': 'GET, OPTIONS'
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
  });
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });
  if (req.method !== 'GET')     return json({ ok: false, error: 'method_not_allowed' }, 405);

  const key = req.headers.get('x-api-key');
  if (!BI_API_KEY || key !== BI_API_KEY) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }

  const url = new URL(req.url);
  const rota  = url.searchParams.get('r') || 'resumo';
  const ini   = url.searchParams.get('ini');
  const fim   = url.searchParams.get('fim');
  const cli   = url.searchParams.get('cliente') || null;
  const maq   = url.searchParams.get('maquina') || null;

  const sb = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

  try {
    if (rota === 'dre') {
      if (!ini || !fim) return json({ ok: false, error: 'missing_period' }, 400);
      const { data, error } = await sb.rpc('jr_relatorio_dre', {
        p_data_ini: ini, p_data_fim: fim, p_cliente_id: cli, p_maquina_id: maq
      });
      if (error) return json({ ok: false, error: error.message }, 500);
      return json(data);
    }

    if (rota === 'os') {
      let q = sb.from('v_jr_resultado_os').select('*').order('data_abertura', { ascending: false });
      if (ini) q = q.gte('data_abertura', ini);
      if (fim) q = q.lte('data_abertura', fim);
      if (cli) q = q.eq('cliente_id', cli);
      const { data, error } = await q.limit(2000);
      if (error) return json({ ok: false, error: error.message }, 500);
      return json({ ok: true, rows: data });
    }

    if (rota === 'maquinas') {
      let q = sb.from('v_jr_resultado_maquina').select('*').order('resultado_operacional', { ascending: false, nullsFirst: false });
      if (maq) q = q.eq('maquina_id', maq);
      const { data, error } = await q.limit(200);
      if (error) return json({ ok: false, error: error.message }, 500);
      return json({ ok: true, rows: data });
    }

    if (rota === 'clientes') {
      let q = sb.from('v_jr_resultado_cliente').select('*').order('resultado_liquido', { ascending: false, nullsFirst: false });
      if (cli) q = q.eq('cliente_id', cli);
      const { data, error } = await q.limit(200);
      if (error) return json({ ok: false, error: error.message }, 500);
      return json({ ok: true, rows: data });
    }

    if (rota === 'horimetros') {
      const { data, error } = await sb.from('v_jr_horimetro_atual').select('*').order('codigo');
      if (error) return json({ ok: false, error: error.message }, 500);
      return json({ ok: true, rows: data });
    }

    if (rota === 'deslocamentos') {
      const { data, error } = await sb.from('v_jr_km_deslocamento').select('*').order('km_total', { ascending: false });
      if (error) return json({ ok: false, error: error.message }, 500);
      return json({ ok: true, rows: data });
    }

    if (rota === 'resumo') {
      // Snapshot consolidado: DRE + top 5 maquinas + top 5 clientes
      const hoje = fim || new Date().toISOString().slice(0, 10);
      const inicio = ini || (() => { const d = new Date(); d.setDate(1); return d.toISOString().slice(0, 10); })();
      const [dre, maqs, clis, horis] = await Promise.all([
        sb.rpc('jr_relatorio_dre', { p_data_ini: inicio, p_data_fim: hoje, p_cliente_id: null, p_maquina_id: null }),
        sb.from('v_jr_resultado_maquina').select('maquina_id,codigo,descricao,horas_totais,receita_horas,resultado_operacional').order('resultado_operacional', { ascending: false, nullsFirst: false }).limit(5),
        sb.from('v_jr_resultado_cliente').select('cliente_id,cliente_nome,oss_total,receita_total,resultado_liquido,saldo_receber_total').order('resultado_liquido', { ascending: false, nullsFirst: false }).limit(5),
        sb.from('v_jr_horimetro_atual').select('*').order('codigo').limit(50)
      ]);
      return json({
        ok: true,
        periodo: { ini: inicio, fim: hoje },
        dre: dre.data,
        top_maquinas: maqs.data || [],
        top_clientes: clis.data || [],
        horimetros: horis.data || []
      });
    }

    return json({ ok: false, error: 'route_not_found', rota }, 404);
  } catch (e) {
    return json({ ok: false, error: String(e) }, 500);
  }
});
