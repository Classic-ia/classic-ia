# Plano de Testes — Migracao de Paginas

## Para cada pagina migrada, validar:

### 1. Autenticacao
- [ ] Pagina redireciona para login se nao autenticado
- [ ] Perfil administrador acessa normalmente
- [ ] Perfil rh acessa normalmente
- [ ] Perfil gestor ve apenas dados do setor (quando aplicavel)
- [ ] Perfil gestor_confianca ve dados ampliados
- [ ] Perfil visualizador ve apenas leitura

### 2. Navegacao
- [ ] Menu lateral renderiza corretamente
- [ ] Item ativo esta destacado
- [ ] Todos os links do menu funcionam
- [ ] Breadcrumb mostra caminho correto
- [ ] Botao voltar funciona

### 3. Dados
- [ ] Dados carregam sem erro no console
- [ ] Nenhum UUID visivel onde deveria ter nome
- [ ] Nenhum campo null/undefined visivel
- [ ] Numeros formatados em pt-BR
- [ ] Datas formatadas em DD/MM/YYYY
- [ ] CPF mascarado corretamente
- [ ] Status com badge colorido

### 4. Erros
- [ ] Erro de API mostra mensagem amigavel (nao JSON cru)
- [ ] Timeout mostra mensagem de retry
- [ ] Pagina sem dados mostra empty state
- [ ] Loading skeleton aparece durante carregamento
- [ ] Refresh (F5) nao quebra a pagina

### 5. Acoes
- [ ] Botoes de acao respeitam perfil (desabilitados se sem permissao)
- [ ] Formularios validam campos obrigatorios
- [ ] Sucesso mostra toast verde
- [ ] Erro mostra toast vermelho
- [ ] Acao irreversivel pede confirmacao

### 6. Responsividade
- [ ] Layout funciona em 1920px (desktop)
- [ ] Layout funciona em 1024px (tablet)
- [ ] Menu colapsa em < 768px
- [ ] Tabelas tem scroll horizontal em mobile

### 7. Observabilidade
- [ ] PAGE_LOAD logado no API.getEvents()
- [ ] Erros de API logados
- [ ] Nenhum erro nao tratado no console
