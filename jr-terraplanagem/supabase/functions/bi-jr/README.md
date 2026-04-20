# Edge Function `bi-jr`

Endpoint de BI externo para o modulo JR Terraplanagem.

## Deploy

```bash
cd /home/user/classic-ia/jr-terraplanagem
supabase secrets set JR_BI_API_KEY=<gere_uma_chave_forte>
supabase functions deploy bi-jr --no-verify-jwt
```

`--no-verify-jwt` porque o gate aqui e o header `x-api-key` (nao
dependemos de token de usuario).

## Uso

```
GET https://<project-ref>.functions.supabase.co/bi-jr?r=<rota>&ini=...&fim=...
Headers: x-api-key: <JR_BI_API_KEY>
```

### Rotas

| r | Descricao | Parametros |
|---|---|---|
| `resumo` | DRE + top 5 maquinas/clientes + horimetros (default) | `ini`, `fim` (opcional) |
| `dre` | DRE simplificado | `ini`, `fim` (obrigatorio), `cliente`, `maquina` |
| `os` | Resultado por OS | `ini`, `fim`, `cliente` |
| `maquinas` | Resultado por maquina | `maquina` |
| `clientes` | Resultado por cliente | `cliente` |
| `horimetros` | Snapshot operacional por maquina | — |
| `deslocamentos` | KM agregado por veiculo | — |

### Exemplos

```bash
curl "https://xxx.functions.supabase.co/bi-jr?r=resumo" \
     -H "x-api-key: $JR_BI_API_KEY"

curl "https://xxx.functions.supabase.co/bi-jr?r=dre&ini=2026-04-01&fim=2026-04-30" \
     -H "x-api-key: $JR_BI_API_KEY"
```

### Seguranca

- Autenticacao exclusivamente por `x-api-key`; sem cookie/JWT.
- Uso de `SUPABASE_SERVICE_ROLE_KEY` interno (agregados somente leitura).
- CORS aberto para facilitar consumo de ferramentas BI.
- Rotacionar `JR_BI_API_KEY` periodicamente.
