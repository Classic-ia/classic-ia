## Classic IA — app.classiccouros.com.br

Frontend estático (HTML/JS/CSS) do sistema unificado Classic (RH/SST + CQ). Deploy
automático via Vercel a partir da branch `main`. Consulte `CLAUDE.md` para
contexto de arquitetura, tabelas, RPCs e convenções.

### Estrutura

- **Raiz:** páginas ativas em produção (`home.html`, `gestao_rh_sst.html`, `app.html`, dashboards, importadores, etc.)
- **`_archive/rh-source/`:** código legacy do repositório separado `classic-rh` (publicado em `classic-rh.vercel.app`). Acessado em runtime via rewrite `/rhesst/*` configurado em `vercel.json`. Não modificar — referência histórica apenas.
- **`migrations/`, `supabase/`:** scripts SQL.
- **`integracao_atak_cq/`, `etl/`, `n8n/`:** integrações externas e pipelines.
- **`SISTEMA_CONTROLE_DE_QUALIDADE/`:** documentação do módulo CQ.

### Convenções críticas

- Rotas legacy RH: **sempre** usar prefixo `/rhesst/nome.html`, nunca linkar direto para `alertas_rh.html`, `asos.html`, `acidentes.html`, `treinamentos.html`, etc.
- Timestamps: `rh_*` / `sst_*` usam `created_at`; `cq_*` usam `criado_em`. Ver `CLAUDE.md`.
- Ferramentas locais (`primeiro_acesso.html`, `gerar_hash.html`) possuem guards de ambiente — não desabilitar.

<!-- -->
