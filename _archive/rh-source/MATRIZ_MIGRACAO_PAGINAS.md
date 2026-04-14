# Matriz de Migracao de Paginas

## Legenda
- **Crit**: CRITICA / MEDIA / BAIXA
- **FD**: Usa fetch direto (sem api.js)
- **RPC**: Usa RPC
- **SB**: Tem sidebar propria (duplicada)
- **Status**: OK / MIGRAR / LEGADO

| # | Pagina | Crit | FD | RPC | SB | Risco | Prioridade | Status |
|---|--------|------|----|----|-------|-------|------------|--------|
| 1 | app.html | CRITICA | Sim | Nao | Sim | Alto | P1 | MIGRAR |
| 2 | colaboradores.html | CRITICA | Sim | Nao | Nao | Alto | P1 | MIGRAR |
| 3 | ficha.html | CRITICA | Sim | Sim | Sim | Medio | P1 | MIGRAR |
| 4 | alertas_rh.html | CRITICA | Sim | Nao | Nao | Medio | P1 | MIGRAR |
| 5 | motor_v2.html | CRITICA | Sim | Sim | Nao | Alto | P1 | MIGRAR |
| 6 | asos.html | MEDIA | Sim | Nao | Nao | Medio | P2 | LEGADO |
| 7 | treinamentos.html | MEDIA | Sim | Nao | Nao | Medio | P2 | LEGADO |
| 8 | epis.html | MEDIA | Sim | Nao | Sim | Baixo | P2 | LEGADO |
| 9 | desligamentos.html | MEDIA | Sim | Nao | Nao | Medio | P2 | LEGADO |
| 10 | lideranca.html | MEDIA | Sim | Nao | Nao | Medio | P2 | LEGADO |
| 11 | sst_dashboard.html | MEDIA | Nao | Sim | Sim | Baixo | P2 | LEGADO |
| 12 | ferias.html | MEDIA | Nao | Sim | Sim | Baixo | P2 | LEGADO |
| 13 | notificacoes.html | MEDIA | Nao | Sim | Sim | Medio | P2 | LEGADO |
| 14 | relatorio_atestados.html | MEDIA | Nao | Sim | Sim | Baixo | P2 | LEGADO |
| 15 | organograma.html | MEDIA | Sim | Nao | Sim | Baixo | P2 | LEGADO |
| 16 | ocorrencias.html | MEDIA | Sim | Nao | Nao | Medio | P3 | LEGADO |
| 17 | beneficios.html | MEDIA | Sim | Nao | Nao | Baixo | P3 | LEGADO |
| 18 | acidentes.html | MEDIA | Sim | Nao | Nao | Medio | P3 | LEGADO |
| 19 | integracoes.html | MEDIA | Sim | Nao | Nao | Baixo | P3 | LEGADO |
| 20 | motor_decisoes.html | MEDIA | Sim | Sim | Nao | Baixo | P3 | LEGADO |
| 21 | motor_v2_auditoria.html | BAIXA | Nao | Sim | Nao | Baixo | P3 | LEGADO |
| 22 | dashboard_gerencial.html | BAIXA | Sim | Nao | Nao | Baixo | P3 | LEGADO |
| 23-41 | Demais | BAIXA | Var | Var | Var | Baixo | P4 | LEGADO |

## 5 Paginas Selecionadas para Fase 2

1. **colaboradores.html** — Pagina mais usada, leitura+escrita, precisa de filtro e vw_funcionario_base
2. **ficha.html** — Consolidacao de todos os dados de um funcionario
3. **alertas_rh.html** — Central de pendencias e planos de acao
4. **motor_v2.html** — Dashboard do motor com RPC que ja teve erro de parametro
5. **epis.html** — Modulo SST critico, novo, deve nascer padronizado
