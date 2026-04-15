import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// ════════════════════════════════════════════════════════════════════════════
// integ-convenia-proxy — Edge Function server-side para Convenia API v3
// Token fica no banco (integ_conector.config_extra), NUNCA no frontend.
// ════════════════════════════════════════════════════════════════════════════

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
};

const CONVENIA_BASE = "https://public-api.convenia.com.br/api/v3";

// Entidades suportadas e suas tabelas de staging
const ENTITY_MAP: Record<string, { endpoint: string; staging: string; perPage: number }> = {
  funcionarios:  { endpoint: "employees",  staging: "stg_convenia_funcionarios",  perPage: 100 },
  afastamentos:  { endpoint: "leaves",     staging: "stg_convenia_afastamentos",  perPage: 100 },
  ferias:        { endpoint: "vacations",  staging: "stg_convenia_ferias",        perPage: 100 },
  folha:         { endpoint: "payrolls",   staging: "stg_convenia_documentos",    perPage: 24  },
  beneficios:    { endpoint: "benefits",   staging: "stg_convenia_documentos",    perPage: 100 },
};

function jsonRes(body: Record<string, unknown>, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function hashPayload(item: unknown): string {
  const bytes = new TextEncoder().encode(JSON.stringify(item));
  return Array.from(new Uint8Array(bytes))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
    .slice(0, 64);
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // ── Ler conector do banco (sempre necessario para id) ─────────────────
    const { data: conector } = await supabase
      .from("integ_conector")
      .select("id, config_extra")
      .eq("nome", "convenia")
      .single();

    if (!conector) {
      return jsonRes({ success: false, error: "Conector 'convenia' nao encontrado em integ_conector" }, 400);
    }

    const conectorId: string = conector.id;

    // ── Parse request body ────────────────────────────────────────────────
    const body = await req.json().catch(() => ({}));
    const action: string = body.action || "test";

    // ══════════════════════════════════════════════════════════════════════
    // ACTION: save_token — salva/atualiza token (nao precisa de token existente)
    // ══════════════════════════════════════════════════════════════════════
    if (action === "save_token") {
      const newToken = body.token;
      if (!newToken || typeof newToken !== "string" || newToken.length < 10) {
        return jsonRes({ success: false, error: "Token invalido (minimo 10 caracteres)" }, 400);
      }

      // Testar token antes de salvar (headers frescos, sem depender de token anterior)
      const testRes = await fetch(`${CONVENIA_BASE}/employees?per_page=1`, {
        headers: { "api-token": newToken, "Content-Type": "application/json", "Accept": "application/json" },
      });
      if (!testRes.ok) {
        return jsonRes({ success: false, error: `Token rejeitado pela Convenia (HTTP ${testRes.status})` }, 400);
      }

      // Salvar no banco
      const existingExtra = conector.config_extra || {};
      await supabase
        .from("integ_conector")
        .update({
          config_extra: { ...existingExtra, token: newToken },
          ultimo_heartbeat: new Date().toISOString(),
          status: "ativo",
        })
        .eq("id", conectorId);

      return jsonRes({ success: true, message: "Token salvo e validado" });
    }

    // ── Para todas as outras actions, token obrigatorio ───────────────────
    const token: string = conector.config_extra?.token;
    if (!token) {
      return jsonRes({ success: false, error: "Token Convenia nao configurado. Use save_token primeiro." }, 400);
    }

    const convHeaders = {
      "api-token": token,
      "Content-Type": "application/json",
      "Accept": "application/json",
    };

    // ══════════════════════════════════════════════════════════════════════
    // ACTION: test — valida token com 1 registro
    // ══════════════════════════════════════════════════════════════════════
    if (action === "test") {
      const res = await fetch(`${CONVENIA_BASE}/employees?per_page=1`, {
        headers: convHeaders,
      });
      if (res.ok) {
        await supabase
          .from("integ_conector")
          .update({ ultimo_heartbeat: new Date().toISOString(), status: "ativo" })
          .eq("id", conectorId);

        return jsonRes({ success: true, status: res.status, message: "Conexao OK" });
      }
      const errText = await res.text().catch(() => "");
      return jsonRes({ success: false, status: res.status, error: errText.slice(0, 300) });
    }

    // ══════════════════════════════════════════════════════════════════════
    // ACTION: sync_* — sincroniza entidade (funcionarios, afastamentos, etc.)
    // ══════════════════════════════════════════════════════════════════════
    const entityKey = action.replace("sync_", "");
    const entityConf = ENTITY_MAP[entityKey];

    if (!entityConf) {
      return jsonRes({ success: false, error: `Acao desconhecida: ${action}` }, 400);
    }

    // Criar log de execucao
    const { data: exec } = await supabase
      .from("integ_execucao")
      .insert({
        conector_id: conectorId,
        tipo_entidade: entityKey,
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

    try {
      // ── Paginacao completa ────────────────────────────────────────────
      let page = 1;
      const maxPages = body.max_pages || 30;
      let hasMore = true;

      while (hasMore && page <= maxPages) {
        const url = `${CONVENIA_BASE}/${entityConf.endpoint}?page=${page}&per_page=${entityConf.perPage}`;
        const apiRes = await fetch(url, { headers: convHeaders });

        if (!apiRes.ok) {
          const errText = await apiRes.text().catch(() => "");
          throw new Error(`Convenia API HTTP ${apiRes.status}: ${errText.slice(0, 200)}`);
        }

        const apiData = await apiRes.json();
        const items = apiData.data || apiData[entityConf.endpoint] || apiData || [];

        if (!Array.isArray(items) || items.length === 0) {
          hasMore = false;
          break;
        }

        totalRead += items.length;

        // ── Preparar registros para staging ────────────────────────────
        const stagingRows = items.map((item: Record<string, unknown>) => {
          const idOrigem = String(item.id || item.uuid || `pg${page}-${Math.random().toString(36).slice(2, 8)}`);
          return {
            id_origem: idOrigem,
            origem_sistema: "convenia",
            payload_json: item,
            hash_registro: hashPayload(item),
            importacao_id: execId,
            status_processamento: "pendente",
          };
        });

        // ── Upsert no staging (ignora duplicados por hash) ────────────
        const { data: inserted, error: insertErr } = await supabase
          .from(entityConf.staging)
          .upsert(stagingRows, { onConflict: "id_origem,hash_registro", ignoreDuplicates: true })
          .select("id");

        if (insertErr) {
          console.error(`Staging insert error (${entityConf.staging}):`, insertErr.message);
          totalError += stagingRows.length;
        } else {
          totalNew += inserted?.length || 0;
        }

        // Verificar se ha proxima pagina
        const meta = apiData.meta;
        if (meta) {
          const lastPage = meta.last_page || meta.total_pages || 1;
          if (page >= lastPage) hasMore = false;
        } else if (items.length < entityConf.perPage) {
          hasMore = false;
        }

        page++;
      }

      // ── Processar staging -> core (apenas funcionarios) ─────────────
      let processed = null;
      if (entityKey === "funcionarios" && totalNew > 0) {
        const { data: procResult, error: procErr } = await supabase.rpc(
          "etl_processar_convenia_funcionarios",
          { p_importacao_id: execId }
        );
        if (procErr) {
          console.error("ETL processing error:", procErr.message);
        } else {
          processed = procResult;
        }
      }

      // ── Atualizar log de execucao ────────────────────────────────────
      await supabase
        .from("integ_execucao")
        .update({
          status: totalError > 0 ? "erro_parcial" : "sucesso",
          registros_lidos: totalRead,
          registros_novos: totalNew,
          registros_erro: totalError,
          fim: new Date().toISOString(),
          duracao_ms: Date.now() - new Date(exec?.inicio || Date.now()).getTime(),
        })
        .eq("id", execId);

      // ── Heartbeat ────────────────────────────────────────────────────
      await supabase
        .from("integ_conector")
        .update({ ultimo_heartbeat: new Date().toISOString(), status: "ativo" })
        .eq("id", conectorId);

      return jsonRes({
        success: true,
        entity: entityKey,
        total_lidos: totalRead,
        total_novos_staging: totalNew,
        total_erros: totalError,
        paginas: page - 1,
        processed,
      });

    } catch (syncErr) {
      // Registrar erro na execucao
      await supabase
        .from("integ_execucao")
        .update({
          status: "erro_total",
          registros_lidos: totalRead,
          registros_novos: totalNew,
          registros_erro: totalError,
          fim: new Date().toISOString(),
          erro_detalhe: (syncErr as Error).message,
        })
        .eq("id", execId);

      await supabase
        .from("integ_conector")
        .update({ status: "erro" })
        .eq("id", conectorId);

      throw syncErr;
    }

  } catch (err) {
    return jsonRes({ success: false, error: (err as Error).message }, 500);
  }
});
