// Edge Function: motor-alertas
// Cron horario que chama trans_alertas_avaliar() para re-avaliar
// regras de manutencao_vencida, lote_vencendo, pneu_vida_util e
// parado_excessivo. UPSERT deduplica por (tipo, entidade_id, dia).
import { supa, jsonResponse } from "../_shared/supa.ts";

Deno.serve(async (_req: Request) => {
  const { data, error } = await supa().rpc("trans_alertas_avaliar", {});
  if (error) return jsonResponse({ error: error.message }, 500);
  return jsonResponse({ result: data });
});
