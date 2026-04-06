// =====================================================
// CORREÇÃO: colaboradores.html — trecho do SAVE
// =====================================================
//
// PROBLEMA: O save envia campos que não existem na tabela rh_funcionarios
//   (cidade, estado, unidade, categoria, cbo) e cargo_id como texto em vez de UUID.
//
// SOLUÇÃO: Trocar o save direto por chamada à RPC salvar_colaborador
//   que faz o mapeamento correto (texto do cargo -> UUID, etc.)
//
// COMO APLICAR:
//   1. Abrir colaboradores.html no repo classic-rh
//   2. Encontrar o bloco: document.getElementById('modal-colab-save').addEventListener('click', async () => {
//   3. Substituir TODO o conteúdo do addEventListener por este código:
//

document.getElementById('modal-colab-save').addEventListener('click', async () => {
    const nome = document.getElementById('f-nome').value.trim();
    const cpf = cpfClean(document.getElementById('f-cpf').value);
    const dataAdm = document.getElementById('f-data-admissao').value;

    if (!nome) { toast('Preencha o nome do colaborador', 'err'); return; }
    if (!cpf || cpf.length !== 11) { toast('CPF invalido (11 digitos)', 'err'); return; }
    if (!dataAdm) { toast('Preencha a data de admissao', 'err'); return; }

    const params = {
      p_id: editingId || null,
      p_nome_completo: nome.toUpperCase(),
      p_cpf: cpf,
      p_matricula: document.getElementById('f-matricula').value.trim() || null,
      p_cargo: document.getElementById('f-cargo').value.trim() || null,
      p_setor_id: document.getElementById('f-setor').value || null,
      p_ghe: document.getElementById('f-ghe').value.trim() || null,
      p_cbo: document.getElementById('f-cbo').value.trim() || null,
      p_salario_base: parseFloat(document.getElementById('f-salario').value) || null,
      p_data_admissao: dataAdm,
      p_turno: document.getElementById('f-turno').value,
      p_status: document.getElementById('f-status').value,
      p_unidade: document.getElementById('f-unidade').value,
      p_categoria: document.getElementById('f-categoria').value,
      p_cargo_confianca: document.getElementById('f-cargo-confianca').checked,
      p_jovem_aprendiz: document.getElementById('f-jovem-aprendiz').checked,
    };

    try {
      const res = await API.rpc('salvar_colaborador', params);
      if (res.ok && res.data && res.data.ok) {
        UI.toast(editingId ? 'Colaborador atualizado' : 'Colaborador cadastrado', 'ok');
        API.logEvent(editingId ? 'colab_update' : 'colab_create', { nome: params.p_nome_completo, cpf: params.p_cpf });
        closeModal($modalColab);
        await loadColabs();
      } else {
        const errMsg = (res.data && res.data.error) || res.error || 'Erro ao salvar';
        if (errMsg.includes('CPF ja cadastrado')) {
          toast('CPF ja cadastrado no sistema', 'err');
        } else {
          toast('Erro ao salvar: ' + errMsg, 'err');
        }
      }
    } catch (e) {
      toast('Erro de conexao: ' + e.message, 'err');
    }
  });
