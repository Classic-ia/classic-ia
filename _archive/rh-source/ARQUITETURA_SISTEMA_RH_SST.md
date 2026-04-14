# ARQUITETURA SISTEMA CLASSIC RH & SST
## Hub de Governanca Operacional
### Versao 1.0 — Março 2026

---

## 1. ARQUITETURA COMPLETA DO SISTEMA

### Camadas

```
[CAMADA 1: INTEGRACAO]     Convenia | BuscaEPI | Secullum | eSocial
        |                          |
[CAMADA 2: VALIDACAO]      Motor de Regras + Validacao de Consistencia
        |                          |
[CAMADA 3: DADOS]          Supabase (fonte de verdade) + Log de Sync
        |                          |
[CAMADA 4: OPERACAO]       Modulos Operacionais + Fluxos
        |                          |
[CAMADA 5: INTELIGENCIA]   Indicadores + Alertas + Dashboard
        |                          |
[CAMADA 6: SAIDA]          Relatorios + Exportacao Financeiro + Impressao
```

### Fonte de Verdade (Data Governance)

| Dado | Fonte Primaria | Fonte Secundaria | Validacao |
|------|---------------|-----------------|-----------|
| Dados pessoais (CPF, nome, nascimento) | Convenia | Pasta RH (fichas) | CPF duplicado, nome divergente |
| Status ativo/inativo | Convenia | Sistema interno | Sync automatica, alerta se diverge |
| Cargo e salario | Convenia | Folha de pagamento | Conferir com folha mensal |
| Setor | Sistema interno | Convenia (departamento) | Mapeamento manual aprovado |
| EPIs entregues | BuscaEPI | Sistema interno | CA valido, vinculo colaborador |
| ASOs | Sistema interno | Clinica (importacao) | Tipo + validade obrigatorios |
| Ponto/frequencia | Secullum | Cartao ponto PDF | Importacao + conferencia |
| Atestados | Pasta OneDrive | Sistema interno | Match colaborador obrigatorio |
| Beneficios (VA, deslocamento) | Planilha folha | Sistema interno | Conferencia mensal com contabilidade |
| Treinamentos/NRs | Certificados PDF | Sistema interno | Validade + reciclagem |

---

## 2. MODULOS PADRONIZADOS

### Grupo 1: CADASTRO
| Codigo | Modulo | Descricao |
|--------|--------|-----------|
| CAD-01 | Gestao de Colaboradores | Cadastro completo, historico, documentos |
| CAD-02 | Gestao de Beneficios | VA, assiduidade, ajuda de custo, Alianca |
| CAD-03 | Gestao de Cargos e Salarios | Progressao, historico, faixas salariais |

### Grupo 2: OPERACAO
| Codigo | Modulo | Descricao |
|--------|--------|-----------|
| OPE-01 | Gestao de ASO | Admissional, periodico, retorno, demissional |
| OPE-02 | Gestao de Treinamentos | Certificados, NRs, reciclagem, vencimentos |
| OPE-03 | Gestao de Ocorrencias | Atestados, advertencias, suspensoes |
| OPE-04 | Apuracao Mensal | Calculo beneficios por competencia (27-26) |
| OPE-05 | Importacao Secullum | Dados de ponto: faltas, atrasos, atestados |

### Grupo 3: FLUXOS (Processos)
| Codigo | Modulo | Descricao |
|--------|--------|-----------|
| FLX-01 | Fluxo de Vagas | Abertura > divulgacao > selecao > contratacao |
| FLX-02 | Fluxo de Admissao | Documentacao > ASO > treinamento > integracao |
| FLX-03 | Fluxo de Desligamento | Aviso > exame > rescisao > homologacao |
| FLX-04 | Fluxo de Acidente | Registro > investigacao > CAT > plano acao |
| FLX-05 | Fluxo de Ocorrencia | Registro > validacao > notificacao > fechamento |

### Grupo 4: SEGURANCA DO TRABALHO
| Codigo | Modulo | Descricao |
|--------|--------|-----------|
| SST-01 | Gestao de Acidentes | Registro CAT, investigacao, 5 porques, taxas |
| SST-02 | Gestao de DDS | Dialogo diario, temas, participantes, frequencia |
| SST-03 | Gestao de CIPA | Mandatos, membros, reunioes, acoes |
| SST-04 | Gestao de Extintores | Cadastro, validade, inspecao, recarga |
| SST-05 | Gestao de Inspecoes | Checklist, nao conformidades, plano acao |

### Grupo 5: AUDITORIA
| Codigo | Modulo | Descricao |
|--------|--------|-----------|
| AUD-01 | Auditoria de Folha | Importar PDF folha, conferir beneficios |
| AUD-02 | Auditoria de Rescisao | Conferir TRCT da contabilidade |
| AUD-03 | Auditoria de Consistencia | Validacao cruzada de dados (novo) |

### Grupo 6: INTEGRACOES
| Codigo | Modulo | Descricao |
|--------|--------|-----------|
| INT-01 | Painel de Integracoes | Status APIs, sync, erros, volume |
| INT-02 | Log de Sincronizacao | Historico completo de todas as syncs |
| INT-03 | Validacao de Divergencias | Cruzamento sistema vs APIs externas |

### Grupo 7: INDICADORES
| Codigo | Modulo | Descricao |
|--------|--------|-----------|
| IND-01 | Dashboard Gerencial | Turnover, absenteismo, perfil, tendencias |
| IND-02 | Dashboard SST | Acidentes, DDS, treinamentos, EPIs |
| IND-03 | Relatorio de Beneficios | Consolidado mensal para folha |
| IND-04 | Central de Alertas | Tudo que precisa de atencao HOJE |

---

## 3. ESTRUTURA DOS FLUXOS

### FLX-01: Fluxo de Vagas
```
[ABERTA] → [DIVULGADA] → [EM SELECAO] → [ENTREVISTA] → [APROVADO] → [ADMISSAO]
                                              ↓
                                        [REPROVADO] → [BANCO DE TALENTOS]
```
**Regras:**
- Vaga so pode ser aberta com aprovacao do gestor
- Candidato reprovado vai automaticamente para banco de talentos
- Admissao so inicia com vaga aprovada

### FLX-02: Fluxo de Admissao
```
[DOCUMENTACAO] → [ASO ADMISSIONAL] → [TREINAMENTO NR-06] → [INTEGRACAO] → [ATIVO]
      ↓                  ↓                    ↓
  [PENDENTE]         [INAPTO]            [PENDENTE]
```
**Regras:**
- BLOQUEIO: Nao pode ativar sem ASO admissional APTO
- BLOQUEIO: Nao pode ativar sem treinamento NR-06
- BLOQUEIO: Nao pode ativar sem documentacao completa (CPF, RG, CTPS, foto)

### FLX-03: Fluxo de Desligamento
```
[AVISO PREVIO] → [EXAME DEMISSIONAL] → [CALCULO RESCISAO] → [HOMOLOGACAO] → [ENCERRADO]
       ↓                  ↓                     ↓
  [TRABALHADO]        [INAPTO]            [AUDITORIA]
  [INDENIZADO]        → RETORNO
```
**Regras:**
- BLOQUEIO: Nao pode encerrar sem exame demissional
- ALERTA: Rescisao deve ser conferida (auditoria) antes de homologar
- LOG: Data e motivo obrigatorios

### FLX-04: Fluxo de Acidente
```
[REGISTRO] → [INVESTIGACAO] → [CAT] → [PLANO ACAO] → [ACOMPANHAMENTO] → [ENCERRADO]
                  ↓
            [5 PORQUES]
            [ARVORE CAUSAS]
```
**Regras:**
- CAT obrigatoria em ate 24h para acidentes com afastamento
- Investigacao obrigatoria para todos os acidentes
- Plano de acao com responsavel e prazo

### FLX-05: Fluxo de Ocorrencia (Atestado/Advertencia)
```
[REGISTRO] → [VALIDACAO MATCH] → [APROVACAO RH] → [LANCADO] → [FECHADO]
                    ↓
             [MATCH MANUAL]
             [SEM MATCH] → alerta
```
**Regras:**
- Atestado de 3+ dias exige CID obrigatorio
- Advertencia requer ciencia do colaborador
- 3 advertencias = alerta critico para gestor

---

## 4. MODULO DE INTEGRACOES (INT-01)

### Tela principal: Painel de Status

```
┌─────────────────────────────────────────────────┐
│  INTEGRACOES                                     │
├──────────┬──────────┬───────────┬───────────────┤
│ API      │ Status   │ Ult.Sync  │ Registros     │
├──────────┼──────────┼───────────┼───────────────┤
│ Convenia │ ● Online │ 20/03 08h │ 480 colabs    │
│ BuscaEPI │ ● Online │ 20/03 10h │ 28.621 EPIs   │
│ Secullum │ ○ Manual │ —         │ —             │
│ eSocial  │ ○ Futuro │ —         │ —             │
├──────────┴──────────┴───────────┴───────────────┤
│ DIVERGENCIAS ENCONTRADAS: 12                     │
│ [Ver detalhes]                                   │
└─────────────────────────────────────────────────┘
```

### Tabela: rh_sync_log
```sql
CREATE TABLE rh_sync_log (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  origem TEXT NOT NULL,          -- 'convenia', 'buscaepi', 'secullum', 'manual'
  tipo TEXT NOT NULL,            -- 'colaboradores', 'epis', 'asos', 'ponto'
  acao TEXT NOT NULL,            -- 'importar', 'atualizar', 'excluir'
  total_processados INT DEFAULT 0,
  total_ok INT DEFAULT 0,
  total_erros INT DEFAULT 0,
  detalhes JSONB,               -- erros detalhados, IDs afetados
  usuario_id UUID,
  iniciado_em TIMESTAMPTZ DEFAULT now(),
  finalizado_em TIMESTAMPTZ
);
```

### Tabela: rh_divergencias
```sql
CREATE TABLE rh_divergencias (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tipo TEXT NOT NULL,            -- 'status', 'nome', 'cargo', 'epi_sem_vinculo', 'aso_sem_colab'
  nivel TEXT DEFAULT 'medio',   -- 'baixo', 'medio', 'alto', 'critico'
  colaborador_id UUID,
  descricao TEXT NOT NULL,
  dado_sistema TEXT,
  dado_externo TEXT,
  origem TEXT,                  -- 'convenia', 'buscaepi'
  status TEXT DEFAULT 'pendente', -- 'pendente', 'resolvido', 'ignorado'
  resolvido_em TIMESTAMPTZ,
  resolvido_por UUID,
  criado_em TIMESTAMPTZ DEFAULT now()
);
```

### Estrategia de Sincronizacao

| API | Modo | Frequencia | Retry | Fallback |
|-----|------|-----------|-------|----------|
| Convenia | Batch | Diario 06h | 3x com backoff | Usar cache local |
| BuscaEPI | Batch | Semanal seg 07h | 3x | Manter ultimo sync |
| Secullum | Manual | Sob demanda | — | Importar arquivo |
| eSocial | Webhook (futuro) | Tempo real | Fila | Log + reprocessar |

---

## 5. MOTOR DE REGRAS

### Tabela: rh_regras
```sql
CREATE TABLE rh_regras (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  codigo TEXT UNIQUE NOT NULL,   -- 'REG-ADM-001'
  nome TEXT NOT NULL,
  modulo TEXT NOT NULL,          -- 'admissao', 'epi', 'aso', 'ocorrencia'
  tipo TEXT NOT NULL,            -- 'bloqueio', 'alerta', 'validacao'
  condicao JSONB NOT NULL,       -- regra em formato estruturado
  acao TEXT NOT NULL,            -- 'bloquear', 'alertar', 'notificar', 'registrar'
  nivel TEXT DEFAULT 'medio',
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);
```

### Regras Implementaveis (com codigo)

| Codigo | Regra | Tipo | Condicao | Acao |
|--------|-------|------|----------|------|
| REG-ADM-001 | Admissao sem ASO | BLOQUEIO | colaborador.status='admissao' AND NOT EXISTS(aso WHERE tipo='admissional' AND resultado='apto') | Bloquear ativacao |
| REG-ADM-002 | Admissao sem NR-06 | BLOQUEIO | colaborador.status='admissao' AND NOT EXISTS(treinamento WHERE tipo='NR-06') | Bloquear ativacao |
| REG-ADM-003 | Admissao sem documentos | ALERTA | colaborador.cpf IS NULL OR colaborador.data_nascimento IS NULL | Alertar RH |
| REG-EPI-001 | EPI sem treinamento | BLOQUEIO | entrega_epi.colaborador NOT IN (treinamentos WHERE tipo='NR-06' AND valido) | Bloquear entrega |
| REG-EPI-002 | CA vencido | ALERTA | epi.ca_validade < hoje | Alertar SST |
| REG-ASO-001 | ASO vencido | ALERTA CRITICO | aso.data_validade < hoje AND colaborador.status='ativo' | Notificar gestor + RH |
| REG-ASO-002 | ASO vence em 30d | ALERTA | aso.data_validade < hoje+30 | Agendar exame |
| REG-OCC-001 | 3 atestados no mes | ALERTA | COUNT(atestado WHERE competencia=atual) >= 3 | Notificar gestor |
| REG-OCC-002 | 3 advertencias | ALERTA CRITICO | COUNT(advertencia WHERE colaborador=X) >= 3 | Alerta para RH avaliar |
| REG-EXP-001 | Contrato experiencia 45d | ALERTA | data_admissao + 45 dias = hoje+7 | Notificar gestor para avaliar |
| REG-EXP-002 | Contrato experiencia 90d | ALERTA CRITICO | data_admissao + 90 dias = hoje+7 | Decisao obrigatoria: efetivar ou desligar |
| REG-DES-001 | Desligamento sem exame | BLOQUEIO | desligamento.status='em_processo' AND NOT EXISTS(aso WHERE tipo='demissional') | Bloquear encerramento |
| REG-TRE-001 | Treinamento vencido | ALERTA | treinamento.validade < hoje AND treinamento.obrigatorio=true | Agendar reciclagem |
| REG-INC-001 | Inconsistencia Convenia | ALERTA | sistema.status != convenia.status | Registrar divergencia |

### Implementacao no Frontend
```javascript
// Motor de regras executado ao salvar/carregar
async function validarRegras(modulo, dados) {
  const regras = await RH_API.get('rh_regras?modulo=eq.' + modulo + '&ativo=eq.true');
  const violacoes = [];

  for (const regra of regras) {
    const resultado = avaliarCondicao(regra.condicao, dados);
    if (resultado.violado) {
      violacoes.push({
        codigo: regra.codigo,
        nome: regra.nome,
        tipo: regra.tipo,
        nivel: regra.nivel,
        mensagem: resultado.mensagem
      });

      if (regra.tipo === 'bloqueio') {
        return { bloqueado: true, violacoes };
      }
    }
  }

  return { bloqueado: false, violacoes };
}
```

---

## 6. INDICADORES COM FORMULAS

### Indicador 1: Taxa de Turnover
```
Formula: (Desligamentos no periodo / Headcount medio) × 100
Headcount medio: (Ativos inicio + Ativos fim) / 2
Origem: rh_desligamentos + rh_colaboradores
Atualizacao: Automatica diaria
Meta sugerida: < 5% mensal / < 30% anual
```

### Indicador 2: Taxa de Absenteismo
```
Formula: (Total dias atestado / (Dias uteis × Headcount)) × 100
Origem: rh_ocorrencias (tipo=atestado) + rh_colaboradores (ativos)
Atualizacao: Automatica por competencia
Meta sugerida: < 3%
```

### Indicador 3: % ASO Vencido
```
Formula: (Colaboradores com ASO vencido / Total ativos) × 100
Vencido: ultimo ASO do colaborador com data_validade < hoje
Origem: rh_aso + rh_colaboradores
Atualizacao: Automatica diaria
Meta: 0%
```

### Indicador 4: Taxa de Frequencia de Acidentes
```
Formula: (N acidentes × 1.000.000) / HHT
HHT (Horas Homem Trabalhadas): Headcount × 220 × meses
Origem: rh_acidentes + rh_colaboradores
Atualizacao: Mensal
```

### Indicador 5: Taxa de Gravidade de Acidentes
```
Formula: (Dias perdidos × 1.000.000) / HHT
Origem: rh_acidentes (dias_afastamento) + rh_colaboradores
Atualizacao: Mensal
```

### Indicador 6: Tempo Medio de Contratacao
```
Formula: Media(data_admissao - data_abertura_vaga)
Origem: rs_vagas + rh_colaboradores
Atualizacao: Por vaga fechada
Meta sugerida: < 30 dias
```

### Indicador 7: Taxa de Aprovacao na Experiencia
```
Formula: (Efetivados / Total que completaram 90d) × 100
Efetivados: status='ativo' AND tempo_casa > 90 dias
Nao aprovados: desligamentos WHERE tempo_empresa_dias <= 90
Origem: rh_desligamentos + rh_colaboradores
Atual: 40.7% (160 de 272 saem antes de 90 dias — ALARMANTE)
Meta sugerida: > 70%
```

### Indicador 8: Custo de Beneficios por Colaborador
```
Formula: (Total VA + Total deslocamento + Total alianca) / Headcount
Origem: rh_vale_alimentacao + rh_auxilio_deslocamento + rh_plano_alianca
Atualizacao: Mensal
```

### Indicador 9: Indice de Conformidade SST
```
Formula: (Treinamentos em dia + ASOs em dia + EPIs entregues) / (Total obrigatorio) × 100
Origem: rh_treinamentos + rh_aso + rh_epi_entregas
Meta: > 95%
```

### Indicador 10: Absenteismo por Setor
```
Formula: (Dias atestado do setor / (Dias uteis × Headcount setor)) × 100
Desdobramento do indicador 2 por setor
Permite identificar setores problematicos
```

---

## 7. SISTEMA DE ALERTAS

### Tabela: rh_alertas
```sql
CREATE TABLE rh_alertas (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  tipo TEXT NOT NULL,
  nivel TEXT NOT NULL,           -- 'baixo', 'medio', 'alto', 'critico'
  titulo TEXT NOT NULL,
  descricao TEXT,
  modulo TEXT,                  -- modulo de origem
  colaborador_id UUID,
  dados JSONB,                  -- dados contextuais
  acao_sugerida TEXT,
  status TEXT DEFAULT 'ativo',  -- 'ativo', 'visto', 'resolvido', 'ignorado'
  resolvido_em TIMESTAMPTZ,
  resolvido_por UUID,
  criado_em TIMESTAMPTZ DEFAULT now()
);
```

### Gatilhos de Alertas

| Gatilho | Nivel | Titulo | Acao Sugerida |
|---------|-------|--------|---------------|
| ASO vence em 30 dias | MEDIO | ASO proximo ao vencimento | Agendar exame periodico |
| ASO vencido | CRITICO | ASO VENCIDO — colaborador irregular | Afastar ate regularizar |
| Contrato exp. 45 dias | MEDIO | Avaliacao de experiencia (1o periodo) | Avaliar desempenho com gestor |
| Contrato exp. 90 dias | CRITICO | Decisao de efetivacao obrigatoria | Efetivar ou desligar antes do prazo |
| 3+ atestados no mes | ALTO | Absenteismo elevado | Conversa com colaborador + gestor |
| 3 advertencias | ALTO | Limite de advertencias atingido | Avaliar medida disciplinar |
| Treinamento NR vencido | ALTO | Treinamento obrigatorio vencido | Agendar reciclagem imediata |
| EPI nao entregue em 30d | MEDIO | Colaborador sem EPI | Providenciar entrega |
| Sync API falhou | ALTO | Falha na integracao | Verificar conexao e reprocessar |
| Divergencia de dados | MEDIO | Dado divergente entre sistema e API | Verificar e corrigir fonte |
| Aniversario do colaborador | BAIXO | Aniversario hoje | Parabenizar |
| Ferias vencendo | ALTO | Ferias acumuladas > 12 meses | Programar ferias |

### Implementacao: Verificacao Automatica
```javascript
// Executar na abertura do sistema (app.html) e na Central de Alertas
async function verificarAlertas() {
  const hoje = new Date().toISOString().split('T')[0];
  const em30d = addDays(hoje, 30).toISOString().split('T')[0];
  const em7d = addDays(hoje, 7).toISOString().split('T')[0];

  // ASOs vencidos
  const asos = await RH_API.get('rh_aso?data_validade=lt.' + hoje);
  // Contratos experiencia
  const exp45 = await RH_API.get('rh_colaboradores?status=eq.ativo&data_admissao=lte.' + subDays(hoje, 38));
  // Atestados do mes
  const atestados = await RH_API.get('rh_ocorrencias?tipo=eq.atestado&competencia=eq.' + competenciaAtual);

  // Gerar alertas automaticamente
  for (const aso of asos) {
    await gerarAlerta('critico', 'ASO VENCIDO', aso.colaborador_nome, ...);
  }
}
```

---

## 8. SUGESTOES DE AUTOMACAO

### Automacao 1: Sync Diaria Convenia (06h)
- Buscar colaboradores atualizados
- Comparar com base local
- Registrar divergencias
- Atualizar status automaticamente

### Automacao 2: Verificacao Matinal de Alertas (07h)
- Rodar todas as regras de alerta
- Gerar notificacoes para RH
- Enviar email com resumo critico (futuro)

### Automacao 3: Importacao Automatica de Atestados
- Monitorar pasta OneDrive
- Parser automatico de nome de arquivo
- Match com colaborador
- Inserir em rh_ocorrencias

### Automacao 4: Calculo Automatico de Beneficios
- No dia 27 de cada mes, calcular:
  - Faltas do periodo
  - Desconto proporcional do VA
  - Elegibilidade de assiduidade
  - Ajuda de custo proporcional

### Automacao 5: Relatorio Semanal Automatico
- Gerar PDF com KPIs da semana
- Comparar com semana anterior
- Destacar variações > 10%

### Automacao 6: Classificacao Financeira
- Ao cadastrar colaborador, definir banco (Itau, Sicredi, Pix, cheque)
- Na apuracao, gerar arquivo separado por banco
- Exportar CSV/XLSX para financeiro

---

## 9. MELHORIAS DE UX

### 9.1 Acoes Rapidas nos Cards
```
┌──────────────────────────────────┐
│ ⚠ ASO VENCIDO                    │
│ JOAO DA SILVA — vencido ha 15d   │
│                                  │
│ [Agendar Exame] [Marcar Resolvido]│
└──────────────────────────────────┘
```
- Botoes de acao diretamente no card de alerta
- Sem precisar navegar para outro modulo

### 9.2 Busca Global
- Campo de busca no header que pesquisa em TODOS os modulos
- Digitar nome do colaborador → mostra resumo com: status, ASO, treinamentos, ocorrencias

### 9.3 Ficha do Colaborador (360°)
- Pagina unica com TUDO do colaborador:
  - Dados pessoais
  - Beneficios ativos
  - ASOs (historico)
  - EPIs recebidos
  - Treinamentos
  - Ocorrencias
  - Historico salarial
  - Alertas pendentes

### 9.4 Mobile First
- Cards empilhados em coluna unica
- Botoes grandes para toque
- Swipe para acoes rapidas
- Camera para escanear documentos

### 9.5 Painel de Pendencias do Dia
```
BOM DIA, JANAINA                    20/03/2026
─────────────────────────────────────────────
📋 3 ASOs vencem esta semana
⚠️ 2 contratos experiencia vencem em 7 dias
📄 5 atestados para validar
🔴 1 divergencia Convenia pendente
─────────────────────────────────────────────
```

### 9.6 Notificacoes por Cores
- 🔴 CRITICO: vermelho pulsante, acao imediata
- 🟠 ALTO: laranja, resolver em 24h
- 🟡 MEDIO: amarelo, resolver na semana
- 🟢 BAIXO: verde, informativo

### 9.7 Dashboard com Drill-Down
- Clicar no KPI → abre lista detalhada
- Clicar no grafico → filtra por periodo/setor
- Hover nos dados → tooltip com contexto

### 9.8 Exportacao Inteligente
- Qualquer tabela → Exportar Excel com 1 clique
- Relatorios com logo da empresa
- Filtros aplicados refletem na exportacao

---

## 10. INTEGRACAO COM FINANCEIRO

### Tabela: rh_dados_bancarios
```sql
CREATE TABLE rh_dados_bancarios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  colaborador_id UUID REFERENCES rh_colaboradores(id),
  banco TEXT NOT NULL,           -- 'itau', 'sicredi', 'bradesco', 'pix', 'cheque'
  agencia TEXT,
  conta TEXT,
  tipo_conta TEXT,              -- 'corrente', 'poupanca', 'salario'
  chave_pix TEXT,
  ativo BOOLEAN DEFAULT true,
  criado_em TIMESTAMPTZ DEFAULT now()
);
```

### Fluxo de Pagamento
```
[APURACAO MENSAL] → [CLASSIFICAR POR BANCO] → [GERAR ARQUIVO]
                                                    ↓
                                          [ITAU.csv] [SICREDI.csv] [PIX.csv]
                                                    ↓
                                          [ENVIAR P/ FINANCEIRO]
```

---

## 11. ROADMAP DE IMPLEMENTACAO

### Fase 1 — IMEDIATA (esta semana)
- [x] Dashboard Gerencial com indicadores
- [x] Importar atestados 2026
- [x] Importar beneficios (VA, assiduidade, ajuda custo)
- [ ] Criar tabelas rh_sync_log e rh_divergencias
- [ ] Adicionar Dashboard Gerencial ao menu

### Fase 2 — CURTO PRAZO (2 semanas)
- [ ] Ficha 360° do colaborador
- [ ] Motor de regras basico (ASO + experiencia)
- [ ] Painel de Integracoes
- [ ] Busca global no header
- [ ] Importar treinamentos das pastas

### Fase 3 — MEDIO PRAZO (1 mes)
- [ ] Fluxos operacionais completos
- [ ] Sistema de alertas automaticos
- [ ] Dados bancarios + exportacao financeiro
- [ ] Importacao automatica de atestados (monitorar pasta)
- [ ] Sync automatica Convenia (diaria)

### Fase 4 — LONGO PRAZO (2-3 meses)
- [ ] Webhook eSocial
- [ ] App mobile (PWA)
- [ ] Relatorios automaticos semanais
- [ ] BI com historico e tendencias
- [ ] Integracao contabilidade (envio de arquivos)

---

*Documento gerado em 20/03/2026 — Classic RH & SST v2026.03*
*Arquiteto: Claude AI — Validado por: RH Classic Couros*
