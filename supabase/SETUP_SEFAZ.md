# Configuração SEFAZ — Classic Couros

## Pré-requisitos
- Supabase CLI instalado (`npm install -g supabase`)
- Certificado digital A1 (.pfx) da empresa
- OpenSSL instalado

## Dados dos Certificados

| Unidade | CNPJ | IE | UF |
|---------|------|----|----|
| Matriz (Jardim Alegre) | 08.849.964/0001-10 | 25285845 | PR |
| Filial (Álvares Machado) | 08.849.964/0003-82 | 25292366 | SP |
| Filial (Triunfo) | 08.849.964/000X-XX | 25292356 | RS |

## Passo 1: Converter o certificado PFX para PEM

```bash
# Na pasta onde está o .pfx (C:\Users\janai\OneDrive\Área de Trabalho\certi)
# Extrair certificado (parte pública)
openssl pkcs12 -in certificado.pfx -out cert.pem -clcerts -nokeys

# Extrair chave privada (sem senha)
openssl pkcs12 -in certificado.pfx -out key.pem -nocerts -nodes
```

Vai pedir a senha do certificado.

## Passo 2: Configurar secrets no Supabase

```bash
# Login no Supabase
supabase login

# Linkar projeto
supabase link --project-ref nvqxsulntpftcwtkjedu

# Enviar certificado como secret
supabase secrets set CERT_PEM="$(cat cert.pem)"
supabase secrets set CERT_KEY="$(cat key.pem)"
supabase secrets set CERT_PASSWORD="sua-senha-aqui"
supabase secrets set CERT_PFX_BASE64="$(base64 -w0 certificado.pfx)"
```

## Passo 3: Deploy das Edge Functions

```bash
# Na pasta do projeto
cd classic-ia

# Deploy da consulta NFe (verificar status)
supabase functions deploy consulta-nfe

# Deploy da distribuição DFe (buscar notas automaticamente)
supabase functions deploy distribuicao-nfe
```

## Passo 4: Testar

```bash
# Testar consulta de status de uma NFe
curl -X POST https://nvqxsulntpftcwtkjedu.supabase.co/functions/v1/consulta-nfe \
  -H "Authorization: Bearer SEU_TOKEN" \
  -H "apikey: SUA_ANON_KEY" \
  -H "Content-Type: application/json" \
  -d '{"chave_nfe": "41240208849964000110550010000012345678901234"}'

# Testar busca de notas recebidas
curl -X POST https://nvqxsulntpftcwtkjedu.supabase.co/functions/v1/distribuicao-nfe \
  -H "Authorization: Bearer SEU_TOKEN" \
  -H "apikey: SUA_ANON_KEY" \
  -H "Content-Type: application/json" \
  -d '{"cnpj": "08849964000110", "uf": "PR", "ultimo_nsu": "0"}'
```

## Funções Disponíveis

### consulta-nfe
Consulta o status de uma NFe na SEFAZ pelo número da chave.
- **Entrada**: `{ "chave_nfe": "44 dígitos" }`
- **Saída**: status, protocolo, data de autorização

### distribuicao-nfe
Busca todas as NFes destinadas ao CNPJ direto da SEFAZ (sem precisar de XML local).
- **Entrada**: `{ "cnpj": "...", "uf": "PR", "ultimo_nsu": "0" }`
- **Saída**: XMLs das NFes recebidas (até 50 por chamada, paginar pelo último NSU)

## Notas
- O certificado A1 tem validade de 1 ano. Quando renovar, repita os passos 1-3.
- A distribuição DFe retorna até 50 documentos por chamada. Use `ultimo_nsu` para paginar.
- O serviço funciona para todas as UFs (usa SEFAZ Ambiente Nacional).
