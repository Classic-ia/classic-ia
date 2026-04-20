# Trans Anacleto - Deploy (Fase 8)

Passo a passo para colocar o modulo em producao em `transanacleto.com.br`.

## Pre-requisitos

- Conta Supabase (projeto novo, dedicado).
- Conta Vercel com acesso ao monorepo.
- Dominio `transanacleto.com.br` registrado e com acesso ao DNS.
- Credenciais Onixsat (API URL + API key + segredo HMAC do webhook).

## 1. Supabase cloud

```bash
cd /home/user/classic-ia/trans-anacleto
supabase init          # se ainda nao houver supabase/config.toml
supabase link --project-ref <ref-do-projeto>

# Aplica as 20 migrations (schema + RLS + RPCs + telemetria + TMS)
supabase db push

# Secrets
supabase secrets set ONIXSAT_API_URL="https://api.onixsat.com.br"
supabase secrets set ONIXSAT_API_KEY="..."
supabase secrets set ONIXSAT_WEBHOOK_SECRET="$(openssl rand -hex 32)"

# Edge Functions de telemetria (Fase 6.5)
supabase functions deploy onixsat-webhook --no-verify-jwt
supabase functions deploy onixsat-sync-cron
supabase functions deploy consolidacao-km-diaria
supabase functions deploy motor-alertas
```

### Cron schedules (Supabase Scheduler ou pg_cron)

| Funcao | Frequencia | Comando |
|---|---|---|
| `consolidacao-km-diaria` | diario 00:10 | UTC 03:10 |
| `motor-alertas` | horaria | */1h |
| `onixsat-sync-cron` | 5 em 5 min (se `modo=pull`) | */5 * * * * |

No Studio Supabase > Database > Cron Jobs, configurar apontando para as
Edge Functions.

## 2. Configurar `config.js`

```js
// /home/user/classic-ia/trans-anacleto/config.js
window.SB_URL = 'https://<ref>.supabase.co';
window.SB_ANON_KEY = '<anon-public-key>';
```

Commitar somente apos validar smoke test.

## 3. Vercel - projeto dedicado

1. **New Project** na Vercel apontando para este monorepo.
2. **Root Directory** = `trans-anacleto`.
3. **Framework Preset** = Other (estatico).
4. **Domains** > adicionar `transanacleto.com.br` (e `www`, se desejar).
5. Apontar DNS conforme Vercel:
   - `A` `@` → `76.76.21.21` (ou CNAME Vercel)
   - `CNAME` `www` → `cname.vercel-dns.com`
6. O `vercel.json` local ja configura cleanUrls, redirects raiz/`/m` e
   cache curto para HTMLs.

## 4. Primeiro usuario admin

```sql
insert into public.trans_usuarios (auth_user_id, email, nome, perfil)
values ('<uid-auth>', 'admin@transanacleto.com.br', 'Admin', 'admin');
```

## 5. Migracao de cadastros

1. `https://transanacleto.com.br/importar.html`.
2. Subir CSV/Excel de veiculos, motoristas, fornecedores e produtos
   (com EANs se disponiveis).
3. Validar com uma entrada de estoque completa (NFe -> lote -> saida).

## 6. Integracao TMS Transporte Facil

- V1 (CSV): usar `tms_sync.html` para upload manual das planilhas de
  veiculos e motoristas exportadas do TMS.
- Exportar custos do periodo via `tms_sync.html > Exportar` (CSV).
- V2 (API): quando o TMS liberar endpoints, criar Edge Function
  `tms-sync` e agendar.

## 7. Telemetria Onixsat

- Fornecer URL publica do webhook (deploy acima):
  `https://<ref>.functions.supabase.co/onixsat-webhook`
- Configurar HMAC compartilhado (ONIXSAT_WEBHOOK_SECRET).
- Em `telemetria_config.html`: API URL, API key, modo (`webhook` ou `pull`).
- Validar na tela `telemetria_eventos.html` que eventos chegam.

## 8. Pneus e estoque mobile

- Imprimir etiquetas iniciais via `etiquetas.html` (QR/Barcode/PDF A4).
- Colar nos pneus do estoque.
- Operadores instalam o app adicionando `/m/` a tela inicial (PWA-like).

## 9. Pos-deploy

- Monitorar Supabase > Logs (DB + Functions).
- Verificar `trans_alertas_automaticos` diariamente na primeira semana.
- Conciliar KM telemetria vs manual em amostras.

## Checklist final

- [ ] 20 migrations aplicadas sem erro em `supabase db push`
- [ ] 4 Edge Functions deployadas e respondendo
- [ ] Cron jobs agendados
- [ ] `config.js` aponta para cloud (nao `127.0.0.1`)
- [ ] Login funcional em `https://transanacleto.com.br/login.html`
- [ ] Cadastros importados (veiculos, motoristas, produtos)
- [ ] Webhook Onixsat recebendo eventos
- [ ] Primeiro pneu cadastrado com etiqueta QR impressa
- [ ] Primeira manutencao finalizada com proximo vencimento calculado
- [ ] TMS sync testado (CSV roundtrip)
- [ ] Dashboard gerencial carrega com dados reais

## Rollback

- Vercel: **Deployments > Promote** em versao anterior.
- Supabase: restaurar via backup (Pro+) ou reaplicar migration de hotfix.
- Em ultimo caso, apontar DNS para staging enquanto corrige producao.
