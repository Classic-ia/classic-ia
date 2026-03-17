// ══════════════════════════════════════════════════════════════════════════════
// regras_validacao.js — Regras de validação CQ para integração ATAK
// Usado tanto no frontend (formulários) quanto no n8n (Code nodes)
// ══════════════════════════════════════════════════════════════════════════════

const REGRAS_VALIDACAO = {

  // ─── Limites configuráveis ──────────────────────────────────────────────
  LIMITES: {
    PCT_C_ATENCAO: 10.0,         // %C acima disso = atenção
    PCT_C_CRITICO: 15.0,         // %C acima disso = fornecedor crítico
    PCT_C_BLOQUEIO: 25.0,        // %C acima disso = bloqueio automático
    DIFERENCA_CONTAGEM_ATENCAO: 5,   // diferença de contagem frigo vs classic
    DIFERENCA_CONTAGEM_CRITICA: 10,  // diferença de contagem crítica
    SCORE_ATENCAO: 60,
    SCORE_CRITICO: 30,
    JANELA_DIAS: 90,             // janela de cálculo do score
  },

  // ─── REGRA 1: A + B + C = Total Classificado ───────────────────────────
  validarABCTotal(carga) {
    const a = parseInt(carga.class_a) || 0;
    const b = parseInt(carga.class_b) || 0;
    const c = parseInt(carga.class_c) || 0;
    const total = parseInt(carga.total_classificado) || 0;
    const soma = a + b + c;

    if (total > 0 && soma !== total) {
      return {
        valido: false,
        tipo: 'abc_total_mismatch',
        gravidade: 'critica',
        campo: 'class_a+class_b+class_c',
        esperado: total,
        encontrado: soma,
        diferenca: Math.abs(total - soma),
        mensagem: `A(${a}) + B(${b}) + C(${c}) = ${soma} ≠ Total classificado(${total})`
      };
    }
    return { valido: true };
  },

  // ─── REGRA 2: Divergência contagem frigorífico vs contagem interna ─────
  validarContagemFrigoVsClassic(carga) {
    const qtdFrigo = parseInt(carga.qtd_frigo) || 0;
    const qtdClassic = parseInt(carga.qtd_classic) || 0;

    if (qtdFrigo === 0 && qtdClassic === 0) return { valido: true };

    const diferenca = Math.abs(qtdFrigo - qtdClassic);

    if (diferenca > 0) {
      const gravidade = diferenca > this.LIMITES.DIFERENCA_CONTAGEM_CRITICA
        ? 'critica'
        : diferenca > this.LIMITES.DIFERENCA_CONTAGEM_ATENCAO
          ? 'atencao'
          : 'informativa';

      return {
        valido: false,
        tipo: 'contagem_frigo_vs_classic',
        gravidade,
        campo: 'qtd_frigo vs qtd_classic',
        esperado: qtdClassic,
        encontrado: qtdFrigo,
        diferenca,
        mensagem: `Contagem frigorífico(${qtdFrigo}) ≠ Contagem interna(${qtdClassic}), diferença: ${diferenca}`
      };
    }
    return { valido: true };
  },

  // ─── REGRA 3: Impedir duplicidade de carga/documento ───────────────────
  async verificarDuplicidade(supabase, carga) {
    const { numero_documento, numero_pcr } = carga;

    // Verificar por numero_pcr (campo UNIQUE em cq_cargas)
    if (numero_pcr) {
      const { data: existePCR } = await supabase
        .from('atak_cargas_raw')
        .select('id, numero_pcr, criado_em')
        .eq('numero_pcr', numero_pcr)
        .limit(1);

      if (existePCR && existePCR.length > 0) {
        return {
          valido: false,
          tipo: 'duplicidade_carga',
          gravidade: 'atencao',
          campo: 'numero_pcr',
          esperado: 'Registro único',
          encontrado: `Duplicata de ${existePCR[0].id}`,
          mensagem: `PCR ${numero_pcr} já existe (ID: ${existePCR[0].id}, criado: ${existePCR[0].criado_em})`
        };
      }
    }

    // Verificar por numero_documento
    if (numero_documento) {
      const { data: existeDoc } = await supabase
        .from('atak_cargas_raw')
        .select('id, numero_documento, criado_em')
        .eq('numero_documento', numero_documento)
        .limit(1);

      if (existeDoc && existeDoc.length > 0) {
        return {
          valido: false,
          tipo: 'duplicidade_carga',
          gravidade: 'atencao',
          campo: 'numero_documento',
          esperado: 'Registro único',
          encontrado: `Duplicata de ${existeDoc[0].id}`,
          mensagem: `Documento ${numero_documento} já existe (ID: ${existeDoc[0].id})`
        };
      }
    }

    return { valido: true };
  },

  // ─── REGRA 4: Marcar fornecedor crítico quando %C > limite ─────────────
  avaliarFornecedorCritico(score) {
    const pctC = parseFloat(score.pct_c) || 0;
    const limiteC = parseFloat(score.limite_pct_c) || this.LIMITES.PCT_C_CRITICO;

    if (pctC > limiteC * 1.5) {
      return {
        valido: false,
        tipo: 'fornecedor_critico',
        gravidade: 'critica',
        status_sugerido: 'bloqueado',
        campo: 'pct_c',
        esperado: `≤${limiteC}%`,
        encontrado: `${pctC.toFixed(2)}%`,
        mensagem: `%C = ${pctC.toFixed(1)}% EXCEDE ${(limiteC * 1.5).toFixed(0)}% → BLOQUEIO RECOMENDADO`
      };
    }

    if (pctC > limiteC) {
      return {
        valido: false,
        tipo: 'fornecedor_critico',
        gravidade: 'atencao',
        status_sugerido: 'critico',
        campo: 'pct_c',
        esperado: `≤${limiteC}%`,
        encontrado: `${pctC.toFixed(2)}%`,
        mensagem: `%C = ${pctC.toFixed(1)}% acima do limite de ${limiteC}% → FORNECEDOR CRÍTICO`
      };
    }

    return { valido: true };
  },

  // ─── REGRA 5: Validar dados básicos da carga ──────────────────────────
  validarDadosBasicos(carga) {
    const erros = [];

    if (!carga.fornecedor_codigo && !carga.fornecedor_nome && !carga.frigorifico) {
      erros.push({
        tipo: 'documento_ausente',
        gravidade: 'critica',
        campo: 'fornecedor',
        mensagem: 'Fornecedor não identificado na carga'
      });
    }

    if (!carga.data_coleta && !carga.data_chegada) {
      erros.push({
        tipo: 'data_inconsistente',
        gravidade: 'atencao',
        campo: 'data_coleta/data_chegada',
        mensagem: 'Nenhuma data de coleta ou chegada informada'
      });
    }

    // Data futura
    const dataRef = carga.data_coleta || carga.data_chegada;
    if (dataRef) {
      const d = new Date(dataRef);
      const hoje = new Date();
      hoje.setHours(23, 59, 59, 999);
      if (d > hoje) {
        erros.push({
          tipo: 'data_inconsistente',
          gravidade: 'atencao',
          campo: 'data_coleta',
          mensagem: `Data futura detectada: ${dataRef}`
        });
      }
    }

    // Valores negativos
    const camposNumericos = ['qtd_frigo', 'class_a', 'class_b', 'class_c', 'total_classificado'];
    for (const campo of camposNumericos) {
      if (carga[campo] !== undefined && parseInt(carga[campo]) < 0) {
        erros.push({
          tipo: 'quantidade_negativa',
          gravidade: 'critica',
          campo,
          mensagem: `Valor negativo em ${campo}: ${carga[campo]}`
        });
      }
    }

    return erros.length > 0
      ? { valido: false, erros }
      : { valido: true };
  },

  // ─── Executar todas as regras em uma carga ─────────────────────────────
  validarCargaCompleta(carga, scoresFornecedor = null) {
    const resultados = [];

    // Regra 1
    const r1 = this.validarABCTotal(carga);
    if (!r1.valido) resultados.push(r1);

    // Regra 2
    const r2 = this.validarContagemFrigoVsClassic(carga);
    if (!r2.valido) resultados.push(r2);

    // Regra 4 (se tiver scores)
    if (scoresFornecedor) {
      const r4 = this.avaliarFornecedorCritico(scoresFornecedor);
      if (!r4.valido) resultados.push(r4);
    }

    // Regra 5
    const r5 = this.validarDadosBasicos(carga);
    if (!r5.valido) resultados.push(...r5.erros);

    return {
      valido: resultados.length === 0,
      divergencias: resultados,
      totalCriticas: resultados.filter(r => r.gravidade === 'critica').length,
      totalAtencao: resultados.filter(r => r.gravidade === 'atencao').length,
      totalInformativas: resultados.filter(r => r.gravidade === 'informativa').length,
    };
  }
};

// Exportar para uso no frontend (browser) e n8n (Node.js)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = REGRAS_VALIDACAO;
}
