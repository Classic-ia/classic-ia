# JR Terraplanagem - Deploy (Fase J9)

Passo a passo para colocar o modulo em producao num dominio proprio.

## Pre-requisitos

- Conta na Vercel com acesso ao projeto Classic Couros (ou novo projeto).
- Conta Supabase (pode ser a mesma usada pelo modulo RH/CQ ou um projeto novo).
- Dominio registrado (ex: `jrterraplanagem.com.br`) com acesso ao DNS.

## 1. Supabase - projeto cloud

Escolha uma das opcoes:

**A) Projeto compartilhado** (reusa o Supabase de Trans Anacleto)

```bash
cd /home/user/classic-ia/trans-anacleto
supabase link --project-ref <ref-do-projeto>
supabase db push         # aplica TODAS as migrations (trans + jr)
supabase secrets set JR_BI_API_KEY=$(openssl rand -hex 32)
supabase functions deploy bi-jr --no-verify-jwt
```

**B) Projeto isolado** (criar um novo so para JR)

```bash
cd /home/user/classic-ia/jr-terraplanagem
# Opcional: gerar config.toml local
supabase init
# Linkar ao projeto novo
supabase link --project-ref <novo-ref>
# Aplicar apenas as migrations JR
supabase db push
# Deploy da Edge Function BI
supabase secrets set JR_BI_API_KEY=$(openssl rand -hex 32)
supabase functions deploy bi-jr --no-verify-jwt
```

## 2. Configurar `config.js` de producao

Atualize `/home/user/classic-ia/jr-terraplanagem/config.js` com a URL
e a chave anon do projeto Supabase:

```js
window.SB_URL = 'https://<ref>.supabase.co';
window.SB_ANON_KEY = '<anon-public-key>';
```

Commit este arquivo so apos validar que o projeto cloud esta OK.

## 3. Vercel

### Opcao A - projeto dedicado (dominio proprio)

1. Crie um novo projeto Vercel apontando para este monorepo.
2. Em **Settings > General > Root Directory** informe:
   `jr-terraplanagem`
3. Em **Settings > Domains**, adicione `jrterraplanagem.com.br` (ou
   o subdominio escolhido) e siga as instrucoes de DNS.
4. O `vercel.json` na pasta `jr-terraplanagem/` configura:
   - `cleanUrls` para abrir `/login` em vez de `/login.html`
   - `Cache-Control` curto para paginas HTML
   - Redireciona `/` para `/login.html`

### Opcao B - convivendo no mesmo projeto Classic (nao recomendado para producao final)

Basta pushar para `main`: o modulo fica acessivel em
`https://app.classiccouros.com.br/jr-terraplanagem/login.html`.

## 4. Primeiro usuario

1. No Studio Supabase (Authentication > Users) crie o e-mail do admin.
2. Execute:

```sql
insert into public.jr_usuarios (auth_user_id, email, nome, perfil)
values ('<uid-do-auth>', 'admin@jrterraplanagem.com.br', 'Admin', 'admin');
```

3. Acesse `/login.html` com este usuario.
4. Va em **Sistema > Importar Legacy** e suba o JSON do sistema antigo.
5. Cadastre planos de manutencao (sugerido via seed).

## 5. Integracoes

### BI externo

- Endpoint: `https://<ref>.functions.supabase.co/bi-jr?r=<rota>`
- Header: `x-api-key: $JR_BI_API_KEY`
- Rotas disponiveis: `resumo`, `dre`, `os`, `maquinas`, `clientes`,
  `horimetros`, `deslocamentos`. Ver `supabase/functions/bi-jr/README.md`.

### Mobile (PWA)

- Acessivel em `https://<dominio>/m/m_home.html`.
- Os HTMLs em `m/` ja trazem `<meta name="theme-color" content="#D97706">`
  e `viewport` mobile. Se quiser instalacao home-screen completa, adicione
  um `manifest.json` e service worker (backlog).

## 6. Monitoramento pos-deploy

- Supabase Studio > Database > Logs para erros de RPC.
- Supabase Studio > Edge Functions > bi-jr > Logs para requisicoes BI.
- Conferir `v_jr_horimetro_atual` para saber se apontamentos mobile estao
  chegando corretamente.

## 7. Rollback

- Vercel: **Deployments > ... > Promote to Production** em versao anterior.
- Supabase: `supabase db reset --linked` (DESTRUTIVO) ou restaurar via
  backup automatico (Pro+).

## Checklist final

- [ ] `supabase db push` aplicou todas as migrations (16 JR + Trans)
- [ ] Edge Function `bi-jr` responde 200 com a chave correta
- [ ] `config.js` aponta para o projeto cloud (nao mais `127.0.0.1:54321`)
- [ ] Login funcional em `https://<dominio>/login.html`
- [ ] Importacao legacy concluida (7 maquinas + 24 OS historicas)
- [ ] Pelo menos 1 DRE do mes corrente com dados reais
- [ ] Mobile: operador consegue bipar QR e registrar periodo de OS
- [ ] DNS apontado e SSL ativo na Vercel
