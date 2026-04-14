# Contrato Global da API — Classic RH

## Resposta Padrao

TODA chamada via `api.js` retorna:

```js
{
  ok: boolean,    // true se HTTP 2xx
  data: any,      // dados retornados (array, objeto ou null)
  error: string,  // mensagem de erro ou null
  status: number  // HTTP status code (0 = erro de rede)
}
```

## Metodos

| Metodo | Funcao | Retorno data |
|--------|--------|-------------|
| `API.get(endpoint)` | Leitura | Array ou [] |
| `API.rpc(name, params)` | RPC | Objeto unwrapped ou null |
| `API.post(table, data)` | Criacao | null (ou objeto se representation) |
| `API.patch(endpoint, data)` | Atualizacao | null |
| `API.del(endpoint)` | Exclusao | null |

## Tratamento de Erros

| Status | Comportamento |
|--------|--------------|
| 401 | Redireciona para login.html |
| 403 | Retorna `{ok:false, error:'Acesso negado'}` |
| Timeout (15s GET, 30s RPC) | Retorna `{ok:false, error:'Timeout'}` |
| Rede | Retorna `{ok:false, error:'Erro de rede', status:0}` |

## Regras de Parametros RPC

- SEMPRE usar prefixo `p_`: `p_funcionario_id`, `p_periodo`, `p_ano`
- NUNCA: `funcionario_id`, `periodo` (sem prefixo)

## Views Padrao

| Alias | Endpoint |
|-------|----------|
| `API.VIEWS.funcionarios_ativos` | `vw_funcionario_base?status=eq.ativo&order=nome` |
| `API.VIEWS.funcionarios_equipe` | `vw_funcionario_base?status=in.(ativo,afastado,ferias,experiencia)&order=nome` |
| `API.VIEWS.setores` | `rh_setores?ativo=eq.true&order=nome` |
| `API.VIEWS.cargos` | `rh_cargos?order=nome` |
| `API.VIEWS.catalogo_epi` | `sst_catalogo_epi?ativo=eq.true&order=descricao` |

## Campo Padrao de Nome

SEMPRE usar `nome` (nunca `nome_completo`, `colaborador_nome`, `funcionario_nome`).
A view `vw_funcionario_base` alias `nome_completo` como `nome`.
