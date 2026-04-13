# Checklist Obrigatório para Novas Páginas

## Antes de criar

- [ ] Rota definida no `Shell.ROUTES` em `shell.js`
- [ ] ID único da página definido (ex: `minha_pagina`)
- [ ] Grupo do menu definido (Pessoas/SST/Gestao/etc)
- [ ] Perfis que acessam definidos
- [ ] View ou RPC de dados definida no backend
- [ ] Parâmetros de RPC com prefixo `p_`

## Estrutura da página

```html
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Classic RH - [TITULO]</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="ds.css">
<script src="config.js"></script>
<script src="rh_auth.js"></script>
<script src="api.js"></script>
<script src="shell.js"></script>
</head>
<body class="page-bg">
<div id="shell"></div>
<div id="shell-content">
  <!-- CONTEUDO DA PAGINA -->
</div>
<script>
(async () => {
  const user = await Shell.init({ page: 'minha_pagina', title: 'Minha Pagina' });
  if (!user) return;
  // carregar dados...
})();
</script>
</body>
</html>
```

## Padrões de dados

- [ ] Leitura de funcionários via `vw_funcionario_base` (campo `nome`, não `nome_completo`)
- [ ] Escrita em tabelas reais (`rh_funcionarios`, `sst_aso`, etc)
- [ ] Usar `API.get()` para leitura
- [ ] Usar `API.rpc()` para RPCs
- [ ] Usar `API.post()` para criação
- [ ] Usar `API.patch()` para atualização

## UI obrigatória

- [ ] Loading state (skeleton ou spinner)
- [ ] Empty state quando sem dados
- [ ] Tratamento de erro (try/catch + console.warn)
- [ ] Formatação pt-BR (usar `API.fmtNum`, `API.fmtData`, etc)
- [ ] Fallback seguro (nunca exibir UUID, null ou undefined)
- [ ] Usar `API.esc()` para escapar HTML

## Campos padronizados

| Campo | Usar | NÃO usar |
|-------|------|----------|
| Nome | `nome` | `nome_completo`, `colaborador_nome`, `funcionario_nome` |
| Setor | `setor` | `setor_atual`, `departamento` |
| Cargo | `cargo` | `cargo_atual` |
| Status | `status` | `situacao` |
| ID | `id` | `funcionario_id` (exceto em tabelas relacionadas) |

## Navegação

- [ ] Página adicionada em `Shell.ROUTES` no `shell.js`
- [ ] KPIs clicáveis apontam para página correta
- [ ] Links de "ver mais" apontam para página correta
- [ ] Botão voltar funciona

## Perfis

- [ ] Dados sensíveis (CID, salário, CPF) só para admin/rh
- [ ] Gestor vê apenas agregados do setor
- [ ] Visualizador vê apenas leitura
