# JR Terraplanagem - Modulo Operacional

Modulo de gestao de obras, maquinario, manutencao e financeiro.

## Estado do desenvolvimento

- **J1 a J8** concluidos.
- **J9 (producao)** - ver `DEPLOY.md` para o passo a passo.

## Arquitetura

- **Supabase compartilhado** com Trans Anacleto (mesmo projeto), isolado
  por prefixo `jr_*`. Sessoes separadas (`jr_sess_v1` vs `trans_sess_v1`).
- Frontend estatico HTML/JS/CSS via Vercel.
- Auth via Supabase Auth + tabela `jr_usuarios` com RBAC.

## Perfis

- `admin` - CRUD completo, configuracoes, importacao legacy
- `gestor` - operacional (OS, pagamentos, relatorios), sem delete
- `operador` - apontamento no campo (OS, abastecimento, manutencao, deslocamento)
- `financeiro` - pagamentos, despesas, relatorios

## Rodando localmente

```bash
# Pre-requisito: Docker Desktop rodando
cd /home/user/classic-ia/trans-anacleto   # Supabase CLI vive aqui (compartilhado)
npx supabase start
npx supabase db reset                      # aplica todas as migrations (trans + jr)
```

Migrations JR em `/home/user/classic-ia/jr-terraplanagem/supabase/migrations/`
devem ser simlinkadas ou copiadas para `/trans-anacleto/supabase/migrations/`
na ordem correta para o `db reset` aplicar ambos os modulos.

Frontend servido por `python3 -m http.server 8080` em
`/home/user/classic-ia/` -> `http://localhost:8080/jr-terraplanagem/login.html`.

## Primeiro acesso

1. Criar usuario via Supabase Auth (Studio ou CLI).
2. Inserir linha em `jr_usuarios` com `auth_user_id = <uid>` e `perfil = 'admin'`.
3. Login em `/login.html`.
4. Ir em **Sistema > Importar Legacy** e fazer upload do JSON exportado
   do sistema antigo para trazer maquinas, OS, despesas, deslocamentos.
