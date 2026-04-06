// =====================================================
// CORREÇÃO: app.html — Lógica de status contratos experiência
// =====================================================
//
// COMO APLICAR:
//   1. Abrir app.html no repo classic-rh
//   2. Encontrar o trecho dentro de loadExperiencia() que calcula statusBadge
//   3. Substituir a lógica de status pelo código abaixo
//
// LÓGICA ANTERIOR (muito simples):
//   dias >= 80 → "Vence em breve"
//   dias 40-50 → "45 dias"
//   resto → "OK"
//
// LÓGICA NOVA (baseada nos marcos legais CLT Art. 445):
//   > 90 dias → "Vencido" (vermelho) — contrato expirou, acao obrigatoria
//   85-90 dias → "Urgente 90d" (vermelho) — 5 dias para decidir
//   81-84 dias → "Vence em breve" (laranja) — planejar decisao
//   46-50 dias → "Avaliar 45d" (amarelo) — avaliar renovacao 1o periodo
//   40-45 dias → "45d proximo" (amarelo claro) — preparar avaliacao
//   < 40 dias → "OK" (verde) — dentro do periodo normal
//   51-80 dias → "2o periodo" (azul) — em andamento, sem urgencia

// Dentro de loadExperiencia(), substituir o bloco que gera statusBadge:

const today = new Date();
tbody.innerHTML = data.map(f => {
  const admDate = new Date(f.data_admissao);
  const dias = Math.floor((today - admDate) / 86400000);
  const v45 = new Date(admDate); v45.setDate(v45.getDate() + 45);
  const v90 = new Date(admDate); v90.setDate(v90.getDate() + 90);

  // Dias restantes para cada marco
  const diasPara45 = Math.floor((v45 - today) / 86400000);
  const diasPara90 = Math.floor((v90 - today) / 86400000);

  let statusBadge, statusTooltip;

  if (diasPara90 < 0) {
    // Passou dos 90 dias — contrato venceu
    statusBadge = '<span class="badge badge-danger" title="Contrato expirado! Acao obrigatoria">Vencido</span>';
    statusTooltip = 'Contrato expirado ha ' + Math.abs(diasPara90) + ' dia(s)';
  } else if (diasPara90 <= 5) {
    // 5 dias ou menos para vencer 90d
    statusBadge = '<span class="badge badge-danger" title="' + diasPara90 + ' dia(s) para vencer">Urgente ' + diasPara90 + 'd</span>';
  } else if (diasPara90 <= 10) {
    // 6-10 dias para 90d
    statusBadge = '<span class="badge badge-warning" title="' + diasPara90 + ' dia(s) para vencer 90d">Vence em breve</span>';
  } else if (diasPara45 >= 0 && diasPara45 <= 5) {
    // 5 dias ou menos para marco 45d
    statusBadge = '<span class="badge badge-warning" title="' + diasPara45 + ' dia(s) para avaliar 45d">Avaliar 45d</span>';
  } else if (diasPara45 < 0 && diasPara45 >= -5) {
    // Acabou de passar 45d (1-5 dias apos)
    statusBadge = '<span class="badge badge-info" title="Passou marco 45d - verificar renovacao">Pos 45d</span>';
  } else if (dias > 45 && dias <= 80) {
    // 2o periodo, sem urgencia
    statusBadge = '<span class="badge badge-success">2o periodo</span>';
  } else {
    // Dentro do 1o periodo, tudo OK
    statusBadge = '<span class="badge badge-success">OK</span>';
  }

  return `<tr>
    <td class="font-medium">${esc(f.nome_completo)}</td>
    <td>${formatDate(f.data_admissao)}</td>
    <td class="cell-center">${dias}</td>
    <td>${formatDate(v45.toISOString())}</td>
    <td>${formatDate(v90.toISOString())}</td>
    <td>${statusBadge}</td>
  </tr>`;
}).join('');
