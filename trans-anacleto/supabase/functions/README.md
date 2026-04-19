# Edge Functions - Trans Anacleto

Funcoes Deno deployadas via `supabase functions deploy <nome>`.

## onixsat-webhook (publica, HMAC-protegida)

Recebe POST com batch de eventos do Onixsat. Valida `X-Signature` contra
o `webhook_secret` armazenado em `trans_onixsat_config` e chama a RPC
`trans_onixsat_ingestar`.

```bash
supabase functions deploy onixsat-webhook --no-verify-jwt
```

Onixsat deve enviar:
- Header: `X-Signature: sha256=<hex-hmac-sha256-do-body>`
- Body: array JSON de eventos `[{placa, data_hora, tipo_evento, ...}]`

## consolidacao-km-diaria (cron 00:10 UTC)

Consolida o dia anterior em `trans_telemetria_km_diario` e marca eventos
como processados.

```bash
supabase functions deploy consolidacao-km-diaria
# Agendar cron no Studio: 10 0 * * *
```

## motor-alertas (cron horario)

Re-avalia regras (manutencao_vencida, lote_vencendo, pneu_vida_util,
parado_excessivo). Deduplica por dia/entidade.

```bash
supabase functions deploy motor-alertas
# Agendar cron: 0 * * * *
```

## onixsat-sync-cron (pull mode, fallback)

Para quando Onixsat nao suporta webhook. Le `trans_onixsat_config.api_url`
e faz GET com auth Bearer. Executar via cron conforme `pull_interval_min`.

```bash
supabase functions deploy onixsat-sync-cron
# Agendar: */5 * * * *  (5 min)
```

## Envs necessarias

- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` (injetadas automaticamente)
- `ONIXSAT_API_KEY` (secret manual via `supabase secrets set`)
