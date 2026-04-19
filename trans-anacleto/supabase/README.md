# Supabase local — Trans Anacleto

Projeto Supabase CLI dedicado ao módulo Trans Anacleto. Roda em Docker e
fica 100% isolado do Supabase do Classic RH/CQ.

## Requisitos

- Docker Desktop em execução
- `npx supabase --version` ≥ 1.150

## Subir o ambiente

```bash
cd /home/user/classic-ia/trans-anacleto
npx supabase start
```

Ao final, o comando imprime:

- `API URL` — usar em `config.js` (SB_URL)
- `anon key` — usar em `config.js` (SB_KEY)
- `Studio URL` — UI web em http://127.0.0.1:54323

Verificar status a qualquer momento:

```bash
npx supabase status
```

## Aplicar migrations

```bash
npx supabase db reset     # dropa, recria e aplica migrations + seed
```

Criar nova migration:

```bash
npx supabase migration new <nome_descritivo>
```

## Criar primeiro usuário admin

1. Abrir Studio em `http://127.0.0.1:54323`
2. **Authentication → Users → Add user** com `admin@transanacleto.local` + senha.
3. Copiar o `UUID` gerado.
4. **SQL Editor** e executar:

```sql
INSERT INTO public.trans_usuarios (auth_user_id, email, nome, perfil)
VALUES ('COLE_UUID_AQUI', 'admin@transanacleto.local', 'Administrador', 'admin');
```

5. Servir o front-end e testar login:

```bash
cd /home/user/classic-ia
python3 -m http.server 8080
# abrir http://localhost:8080/trans-anacleto/login.html
```

## Migrar para produção (transanacleto.com.br)

1. Criar projeto Supabase cloud em https://supabase.com/dashboard.
2. `supabase link --project-ref <ref>`
3. `supabase db push` — aplica as mesmas migrations na cloud.
4. Atualizar `config.js` com URL/anon_key do projeto cloud.
