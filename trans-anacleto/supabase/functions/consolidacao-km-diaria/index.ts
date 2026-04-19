// Edge Function: consolidacao-km-diaria
// Cron diario (00:10) que chama trans_telemetria_consolidar_dia para ontem.
// Configurar em supabase/config.toml ou via supabase scheduler.
import { supa, jsonResponse } from "../_shared/supa.ts";

Deno.serve(async (_req: Request) => {
  const ontem = new Date();
  ontem.setUTCDate(ontem.getUTCDate() - 1);
  const dataRef = ontem.toISOString().slice(0, 10);

  const { data, error } = await supa().rpc("trans_telemetria_consolidar_dia", {
    p_data: dataRef,
    p_veiculo_id: null,
  });

  if (error) return jsonResponse({ error: error.message }, 500);
  return jsonResponse({ data_ref: dataRef, veiculos_consolidados: data });
});
