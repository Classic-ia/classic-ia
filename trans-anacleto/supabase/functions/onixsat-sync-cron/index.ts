// Edge Function: onixsat-sync-cron
// Modo fallback pull: quando Onixsat nao suporta webhook.
// Executada por cron (a cada N minutos conforme trans_onixsat_config.pull_interval_min).
// Busca eventos desde ultimo_sync e chama trans_onixsat_ingestar.
import { supa, jsonResponse } from "../_shared/supa.ts";

interface OnixsatConfig {
  api_url: string | null;
  modo_ingestao: string;
  ultimo_sync: string | null;
  ativo: boolean;
}

Deno.serve(async (_req: Request) => {
  const { data: cfg, error: cfgErr } = await supa()
    .from("trans_onixsat_config")
    .select("api_url, modo_ingestao, ultimo_sync, ativo")
    .limit(1)
    .maybeSingle<OnixsatConfig>();

  if (cfgErr || !cfg) return jsonResponse({ error: "config not found" }, 500);
  if (!cfg.ativo) return jsonResponse({ skipped: "inactive" });
  if (cfg.modo_ingestao !== "pull") return jsonResponse({ skipped: "modo=" + cfg.modo_ingestao });
  if (!cfg.api_url) return jsonResponse({ error: "api_url nao configurada" }, 500);

  // TODO: adaptar ao contrato real da API Onixsat quando disponivel.
  // Implementacao atual e stub que le ?since=<iso>, espera array JSON
  // de eventos compatives com trans_onixsat_ingestar.
  const since = cfg.ultimo_sync || new Date(Date.now() - 3600_000).toISOString();
  const apiKey = Deno.env.get("ONIXSAT_API_KEY") || "";

  let resp: Response;
  try {
    resp = await fetch(`${cfg.api_url}?since=${encodeURIComponent(since)}`, {
      headers: { "Authorization": `Bearer ${apiKey}` },
    });
  } catch (e) {
    await supa().from("trans_onixsat_config")
      .update({ ultimo_erro: (e as Error).message })
      .eq("ativo", true);
    return jsonResponse({ error: (e as Error).message }, 502);
  }

  if (!resp.ok) {
    const err = `HTTP ${resp.status}`;
    await supa().from("trans_onixsat_config").update({ ultimo_erro: err }).eq("ativo", true);
    return jsonResponse({ error: err }, 502);
  }

  const eventos = await resp.json();
  if (!Array.isArray(eventos)) {
    return jsonResponse({ error: "resposta onixsat nao e array" }, 500);
  }

  const { data, error } = await supa().rpc("trans_onixsat_ingestar", {
    p_eventos: eventos,
  });
  if (error) return jsonResponse({ error: error.message }, 500);

  return jsonResponse({ since, result: data });
});
