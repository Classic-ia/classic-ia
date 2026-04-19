// Edge Function: onixsat-webhook
// Recebe batch de eventos do Onixsat via POST, valida HMAC e chama o
// RPC trans_onixsat_ingestar. Deve ser deployada publicamente.
//
// Invoke:
//   POST <SUPABASE_URL>/functions/v1/onixsat-webhook
//   Headers: X-Signature: sha256=<hex>
//   Body: [{ placa, data_hora, tipo_evento, km_veiculo, velocidade, ...}]
import { supa, jsonResponse } from "../_shared/supa.ts";
import { verifyHmac } from "../_shared/hmac.ts";

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") {
    return jsonResponse({ error: "method not allowed" }, 405);
  }

  const body = await req.text();

  // carrega config (secret e ativo)
  const { data: cfg, error: cfgErr } = await supa()
    .from("trans_onixsat_config")
    .select("webhook_secret, ativo")
    .limit(1)
    .maybeSingle();

  if (cfgErr || !cfg) {
    return jsonResponse({ error: "config not found" }, 500);
  }
  if (!cfg.ativo) {
    return jsonResponse({ error: "integration inactive" }, 403);
  }

  const sig = req.headers.get("x-signature");
  const ok = await verifyHmac(body, sig, cfg.webhook_secret || "");
  if (!ok) {
    await supa()
      .from("trans_onixsat_config")
      .update({ ultimo_erro: "HMAC invalido em " + new Date().toISOString() })
      .eq("ativo", true);
    return jsonResponse({ error: "invalid signature" }, 401);
  }

  let payload: unknown;
  try {
    payload = JSON.parse(body);
  } catch {
    return jsonResponse({ error: "invalid json" }, 400);
  }
  if (!Array.isArray(payload)) {
    return jsonResponse({ error: "payload must be array" }, 400);
  }

  const { data, error } = await supa().rpc("trans_onixsat_ingestar", {
    p_eventos: payload,
  });

  if (error) {
    await supa()
      .from("trans_onixsat_config")
      .update({ ultimo_erro: error.message })
      .eq("ativo", true);
    return jsonResponse({ error: error.message }, 500);
  }

  return jsonResponse({ result: data });
});
