import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // Get BuscaEPI credentials from integ_conector
    const { data: conector } = await supabase
      .from("integ_conector")
      .select("config_extra")
      .eq("nome", "buscaepi")
      .single();

    if (!conector?.config_extra) {
      return new Response(JSON.stringify({ error: "BuscaEPI connector not configured" }), {
        status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    const { token_auth, token_access } = conector.config_extra;
    const baseUrl = "https://buscaepi.com.br/api/v1";
    const headers = {
      "Authorization": `Basic ${token_auth}`,
      "x-access-token": token_access,
      "Content-Type": "application/json",
    };

    // Get params from request
    const body = await req.json().catch(() => ({}));
    const perPage = body.per_page || 500;
    const maxPages = body.max_pages || 20;  // safety limit: 500 * 20 = 10,000 max
    const entity = body.entity || "entregas"; // entregas, colaboradores, epis, os, ca

    // Determine start page from last known id
    const { data: lastStg } = await supabase
      .from(`stg_buscaepi_${entity}`)
      .select("id_origem")
      .order("id_origem", { ascending: false })
      .limit(1);

    const lastId = lastStg?.[0]?.id_origem || "0";

    // Create execution log
    const { data: exec } = await supabase
      .from("integ_execucao")
      .insert({
        conector_id: (await supabase.from("integ_conector").select("id").eq("nome", "buscaepi").single()).data?.id,
        tipo_entidade: entity,
        modo: "incremental",
        registros_lidos: 0,
        registros_novos: 0,
        registros_erro: 0,
        status: "executando",
      })
      .select("id")
      .single();

    const execId = exec?.id;
    let totalRead = 0;
    let totalNew = 0;
    let totalError = 0;
    let page = 1;
    let hasMore = true;

    while (hasMore && page <= maxPages) {
      // Fetch page from BuscaEPI API
      const url = `${baseUrl}/${entity}?page=${page}&per_page=${perPage}&order=id_entrega&direction=desc`;
      const apiRes = await fetch(url, { headers });

      if (!apiRes.ok) {
        const errText = await apiRes.text();
        throw new Error(`BuscaEPI API error ${apiRes.status}: ${errText.slice(0, 200)}`);
      }

      const apiData = await apiRes.json();
      const items = apiData.data || apiData.items || apiData || [];

      if (!Array.isArray(items) || items.length === 0) {
        hasMore = false;
        break;
      }

      totalRead += items.length;

      // Prepare staging records
      const stagingRows = items.map((item: any) => {
        const idOrigem = String(
          item.id_entrega || item.id_colaborador || item.id_item_entrega || item.id || page + "-" + Math.random()
        );
        const hash = new TextEncoder().encode(JSON.stringify(item));
        return {
          id_origem: idOrigem,
          origem_sistema: "buscaepi",
          payload_json: item,
          hash_registro: Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("").slice(0, 64),
          importacao_id: execId,
          status_processamento: "pendente",
        };
      });

      // Upsert to staging (skip duplicates)
      const { data: inserted, error: insertErr } = await supabase
        .from(`stg_buscaepi_${entity}`)
        .upsert(stagingRows, { onConflict: "id_origem,hash_registro", ignoreDuplicates: true })
        .select("id");

      const newCount = inserted?.length || 0;
      totalNew += newCount;

      if (items.length < perPage) {
        hasMore = false; // last page
      }
      page++;
    }

    // Process staging -> core (for entregas only)
    let processed = null;
    if (entity === "entregas" && totalNew > 0) {
      const { data: procResult } = await supabase.rpc("etl_processar_buscaepi_entregas");
      processed = procResult;
    }

    // Update execution log
    await supabase
      .from("integ_execucao")
      .update({
        status: "sucesso",
        registros_lidos: totalRead,
        registros_novos: totalNew,
        registros_erro: totalError,
        fim: new Date().toISOString(),
      })
      .eq("id", execId);

    // Update heartbeat
    await supabase
      .from("integ_conector")
      .update({ ultimo_heartbeat: new Date().toISOString() })
      .eq("nome", "buscaepi");

    return new Response(
      JSON.stringify({
        ok: true,
        entity,
        pages_fetched: page - 1,
        total_read: totalRead,
        total_new_staging: totalNew,
        processed,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (err) {
    return new Response(
      JSON.stringify({ error: err.message }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});
