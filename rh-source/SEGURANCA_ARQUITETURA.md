# Arquitetura de Seguranca Completa — Classic RH & SST

## 1. Matriz de Acesso por Perfil

| Recurso | colaborador | lider | rh | sst | psicologa | financeiro | diretoria | admin |
|---|---|---|---|---|---|---|---|---|
| **Colaboradores (dados gerais)** | Proprio | Setor | Todos | Todos | Todos | Todos | Todos | Todos |
| **Salario** | Proprio | NAO | Todos | NAO | NAO | Todos | Todos | Todos |
| **ASOs** | Proprio | Setor | Todos | Todos | NAO | NAO | NAO | Todos |
| **Treinamentos** | Proprio | Setor | Todos | Todos | NAO | NAO | NAO | Todos |
| **EPI Entregas** | Proprio | Setor | Todos | Todos | NAO | NAO | NAO | Todos |
| **Ocorrencias** | Proprio | Setor | Todos | Todos | NAO | NAO | NAO | Todos |
| **Saude Mental** | NAO | NAO | NAO | NAO | Todos | NAO | Agregado | Todos |
| **Desligamentos** | NAO | NAO | Todos | NAO | NAO | Todos | Todos | Todos |
| **Beneficios** | Proprio | NAO | Todos | NAO | NAO | Todos | NAO | Todos |
| **Producao** | Proprio | Setor | Todos | NAO | NAO | NAO | Todos | Todos |
| **Audit Log** | NAO | NAO | NAO | NAO | NAO | NAO | Todos | Todos |
| **Perfis Acesso** | Proprio | NAO | NAO | NAO | NAO | NAO | NAO | Todos |

### Legenda
- **Proprio**: ve apenas seus proprios dados
- **Setor**: ve dados do setor que gerencia
- **Todos**: ve todos os registros
- **Agregado**: ve apenas views agregadas, sem dados individuais
- **NAO**: sem acesso

## 2. Operacoes por Perfil

### INSERT (quem pode criar)

| Tabela | Perfis |
|---|---|
| Colaboradores | admin, rh |
| ASOs | admin, sst |
| Treinamentos | admin, sst |
| EPI Entregas | admin, sst |
| Ocorrencias | admin, rh |
| Saude Mental | admin, psicologa |
| Desligamentos | admin (via RPC) |
| Beneficios | admin, rh |
| Producao | admin, rh, lider |
| Perfis Acesso | admin |

### UPDATE (quem pode editar)

| Tabela | Perfis |
|---|---|
| Colaboradores | admin, rh |
| ASOs | admin, sst |
| Treinamentos | admin, sst |
| EPI Entregas | admin, sst |
| Ocorrencias | admin, rh |
| Saude Mental | admin, psicologa |
| Desligamentos | admin |
| Beneficios | admin, rh |
| Producao | admin, rh |
| Perfis Acesso | admin |

### DELETE
**Apenas admin pode deletar em qualquer tabela.**
Preferir soft delete (status = 'inativo') em vez de DELETE real.

## 3. Operacoes que DEVEM passar por Backend (Edge Functions)

| Operacao | Motivo |
|---|---|
| **Desligamento** | Atualiza multiplas tabelas (colaborador + desligamento + audit). Precisa de validacao de negocio (calculos trabalhistas). |
| **Alteracao salarial** | Impacto financeiro alto. Requer dupla validacao e auditoria obrigatoria. |
| **ASO apto/inapto** | Decisao medica com consequencias legais (afastamento, readaptacao). Nao pode ser alterada casualmente. |
| **Classificacao risco ALTO** | Dispara alertas automaticos e acoes obrigatorias do RH. Precisa de validacao. |
| **Exportacao de dados** | Dados pessoais (LGPD). Backend deve sanitizar e logar quem exportou. |

## 4. Seguranca de Chaves

### anon key (frontend)
- Usada para autenticacao (login/signup)
- Apos login, todas as requests usam JWT do usuario
- RLS garante que cada usuario ve apenas o que pode
- **NUNCA** usar anon key para operacoes sem autenticacao

### service_role key (backend APENAS)
- **NUNCA** expor no frontend
- Usada apenas em Edge Functions / backend
- Bypassa RLS — por isso eh perigosa
- Usar apenas para: migrations, jobs automaticos, alertas do sistema

### Frontend pode:
- Fazer login/logout
- Queries com JWT autenticado (RLS protege)
- Chamar RPCs permitidas
- Upload de arquivos ao storage (com policies)

### Backend deve:
- Processar desligamentos
- Alterar salarios
- Gerar relatorios exportados
- Executar jobs agendados (vencimento de ASOs, alertas)
- Qualquer operacao que modifique multiplas tabelas

## 5. Fluxo de Seguranca

```
Usuario -> Login (anon key) -> JWT Token
    |
    v
Request com JWT -> Supabase RLS
    |
    v
has_perfil() -> Verifica rh_perfis_acesso
    |
    ├── admin -> Acesso total
    ├── rh -> Dados administrativos
    ├── sst -> Dados de saude ocupacional
    ├── psicologa -> Apenas saude mental
    ├── lider -> Apenas seu setor
    ├── financeiro -> Beneficios + desligamentos
    ├── diretoria -> Views agregadas
    └── colaborador -> Apenas proprio registro
```

## 6. Checklist de Validacao

### Antes de ir para producao:
- [ ] Todas as tabelas tem RLS habilitado (ENABLE ROW LEVEL SECURITY)
- [ ] Todas as tabelas tem pelo menos uma policy de SELECT
- [ ] Tabelas sem policy de DELETE = ninguem deleta (comportamento correto)
- [ ] anon key NAO retorna dados sem autenticacao
- [ ] service_role key NAO esta no frontend
- [ ] Perfil admin cadastrado para usuario principal
- [ ] Indices criados nas colunas usadas por has_perfil() e get_setores_lider()
- [ ] Audit log registra alteracoes de: cargo, salario, status, classificacao_risco
- [ ] Dados de saude mental NAO sao acessiveis por RH/lider/financeiro
- [ ] Diretoria ve apenas views agregadas de saude mental
- [ ] RPCs de desligamento e salario exigem perfil correto internamente
- [ ] Nenhuma tabela esta com RLS desabilitado

### Revisao periodica (mensal):
- [ ] Verificar perfis ativos (remover ex-funcionarios)
- [ ] Revisar audit log para acoes suspeitas
- [ ] Confirmar que novas tabelas tem RLS
- [ ] Testar acesso com cada perfil
- [ ] Verificar se Edge Functions usam service_role corretamente

## 7. Riscos e Falhas Comuns

| Risco | Impacto | Mitigacao |
|---|---|---|
| RLS desabilitado em tabela nova | Dados expostos a todos | Checklist obrigatorio ao criar tabela |
| service_role no frontend | Acesso total sem restricao | Code review, variaveis de ambiente |
| Perfil admin em excesso | Acoes nao rastreadas | Limitar admin a 1-2 pessoas |
| has_perfil() sem indice | Queries lentas (full scan) | Indice em (auth_uid, perfil, ativo) |
| Dados clinicos no rh_saude_mental | Violacao etica e legal | Campo observacoes_gerais limitado, sem CID/diagnostico |
| Export sem log | LGPD — nao sabe quem acessou | Toda exportacao via Edge Function com log |
| JWT expirado nao tratado | Erro 401 confuso para usuario | Frontend intercepta 401 e redireciona para login |
| Trigger de auditoria quebrado | Alteracoes sem registro | Monitorar tabela audit_log periodicamente |
