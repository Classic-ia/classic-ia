-- ============================================================
-- Migration: RLS dos cadastros (Fase 2)
-- Policies:
--   admin                — full CRUD
--   gestor               — SELECT tudo; INSERT/UPDATE; sem DELETE
--   almoxarife           — CRUD produtos/categorias; SELECT veic/motoristas/fornecedores
--   motorista            — SELECT próprio veículo/motorista (limitado); sem escrita aqui
-- ============================================================

-- ============================================================
-- trans_motoristas
-- ============================================================
ALTER TABLE public.trans_motoristas ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_motoristas_select ON public.trans_motoristas;
CREATE POLICY trans_motoristas_select
  ON public.trans_motoristas FOR SELECT TO authenticated
  USING (
    public.trans_current_perfil() IN ('admin','gestor','almoxarife','motorista')
  );

DROP POLICY IF EXISTS trans_motoristas_insert ON public.trans_motoristas;
CREATE POLICY trans_motoristas_insert
  ON public.trans_motoristas FOR INSERT TO authenticated
  WITH CHECK (public.trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_motoristas_update ON public.trans_motoristas;
CREATE POLICY trans_motoristas_update
  ON public.trans_motoristas FOR UPDATE TO authenticated
  USING (public.trans_current_perfil() IN ('admin','gestor'))
  WITH CHECK (public.trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_motoristas_delete ON public.trans_motoristas;
CREATE POLICY trans_motoristas_delete
  ON public.trans_motoristas FOR DELETE TO authenticated
  USING (public.trans_is_admin());

-- ============================================================
-- trans_veiculos
-- ============================================================
ALTER TABLE public.trans_veiculos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_veiculos_select ON public.trans_veiculos;
CREATE POLICY trans_veiculos_select
  ON public.trans_veiculos FOR SELECT TO authenticated
  USING (public.trans_current_perfil() IS NOT NULL);

DROP POLICY IF EXISTS trans_veiculos_insert ON public.trans_veiculos;
CREATE POLICY trans_veiculos_insert
  ON public.trans_veiculos FOR INSERT TO authenticated
  WITH CHECK (public.trans_current_perfil() IN ('admin','gestor'));

DROP POLICY IF EXISTS trans_veiculos_update ON public.trans_veiculos;
CREATE POLICY trans_veiculos_update
  ON public.trans_veiculos FOR UPDATE TO authenticated
  USING (public.trans_current_perfil() IN ('admin','gestor','almoxarife'))
  WITH CHECK (public.trans_current_perfil() IN ('admin','gestor','almoxarife'));

DROP POLICY IF EXISTS trans_veiculos_delete ON public.trans_veiculos;
CREATE POLICY trans_veiculos_delete
  ON public.trans_veiculos FOR DELETE TO authenticated
  USING (public.trans_is_admin());

-- ============================================================
-- trans_fornecedores
-- ============================================================
ALTER TABLE public.trans_fornecedores ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_fornecedores_select ON public.trans_fornecedores;
CREATE POLICY trans_fornecedores_select
  ON public.trans_fornecedores FOR SELECT TO authenticated
  USING (public.trans_current_perfil() IS NOT NULL);

DROP POLICY IF EXISTS trans_fornecedores_insert ON public.trans_fornecedores;
CREATE POLICY trans_fornecedores_insert
  ON public.trans_fornecedores FOR INSERT TO authenticated
  WITH CHECK (public.trans_current_perfil() IN ('admin','gestor','almoxarife'));

DROP POLICY IF EXISTS trans_fornecedores_update ON public.trans_fornecedores;
CREATE POLICY trans_fornecedores_update
  ON public.trans_fornecedores FOR UPDATE TO authenticated
  USING (public.trans_current_perfil() IN ('admin','gestor','almoxarife'))
  WITH CHECK (public.trans_current_perfil() IN ('admin','gestor','almoxarife'));

DROP POLICY IF EXISTS trans_fornecedores_delete ON public.trans_fornecedores;
CREATE POLICY trans_fornecedores_delete
  ON public.trans_fornecedores FOR DELETE TO authenticated
  USING (public.trans_is_admin());

-- ============================================================
-- trans_categorias_produto
-- ============================================================
ALTER TABLE public.trans_categorias_produto ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_categorias_select ON public.trans_categorias_produto;
CREATE POLICY trans_categorias_select
  ON public.trans_categorias_produto FOR SELECT TO authenticated
  USING (public.trans_current_perfil() IS NOT NULL);

DROP POLICY IF EXISTS trans_categorias_insert ON public.trans_categorias_produto;
CREATE POLICY trans_categorias_insert
  ON public.trans_categorias_produto FOR INSERT TO authenticated
  WITH CHECK (public.trans_current_perfil() IN ('admin','almoxarife'));

DROP POLICY IF EXISTS trans_categorias_update ON public.trans_categorias_produto;
CREATE POLICY trans_categorias_update
  ON public.trans_categorias_produto FOR UPDATE TO authenticated
  USING (public.trans_current_perfil() IN ('admin','almoxarife'))
  WITH CHECK (public.trans_current_perfil() IN ('admin','almoxarife'));

DROP POLICY IF EXISTS trans_categorias_delete ON public.trans_categorias_produto;
CREATE POLICY trans_categorias_delete
  ON public.trans_categorias_produto FOR DELETE TO authenticated
  USING (public.trans_is_admin());

-- ============================================================
-- trans_produtos
-- ============================================================
ALTER TABLE public.trans_produtos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_produtos_select ON public.trans_produtos;
CREATE POLICY trans_produtos_select
  ON public.trans_produtos FOR SELECT TO authenticated
  USING (public.trans_current_perfil() IS NOT NULL);

DROP POLICY IF EXISTS trans_produtos_insert ON public.trans_produtos;
CREATE POLICY trans_produtos_insert
  ON public.trans_produtos FOR INSERT TO authenticated
  WITH CHECK (public.trans_current_perfil() IN ('admin','almoxarife'));

DROP POLICY IF EXISTS trans_produtos_update ON public.trans_produtos;
CREATE POLICY trans_produtos_update
  ON public.trans_produtos FOR UPDATE TO authenticated
  USING (public.trans_current_perfil() IN ('admin','almoxarife'))
  WITH CHECK (public.trans_current_perfil() IN ('admin','almoxarife'));

DROP POLICY IF EXISTS trans_produtos_delete ON public.trans_produtos;
CREATE POLICY trans_produtos_delete
  ON public.trans_produtos FOR DELETE TO authenticated
  USING (public.trans_is_admin());

-- ============================================================
-- trans_produtos_ean
-- ============================================================
ALTER TABLE public.trans_produtos_ean ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trans_produtos_ean_select ON public.trans_produtos_ean;
CREATE POLICY trans_produtos_ean_select
  ON public.trans_produtos_ean FOR SELECT TO authenticated
  USING (public.trans_current_perfil() IS NOT NULL);

DROP POLICY IF EXISTS trans_produtos_ean_insert ON public.trans_produtos_ean;
CREATE POLICY trans_produtos_ean_insert
  ON public.trans_produtos_ean FOR INSERT TO authenticated
  WITH CHECK (public.trans_current_perfil() IN ('admin','almoxarife'));

DROP POLICY IF EXISTS trans_produtos_ean_update ON public.trans_produtos_ean;
CREATE POLICY trans_produtos_ean_update
  ON public.trans_produtos_ean FOR UPDATE TO authenticated
  USING (public.trans_current_perfil() IN ('admin','almoxarife'))
  WITH CHECK (public.trans_current_perfil() IN ('admin','almoxarife'));

DROP POLICY IF EXISTS trans_produtos_ean_delete ON public.trans_produtos_ean;
CREATE POLICY trans_produtos_ean_delete
  ON public.trans_produtos_ean FOR DELETE TO authenticated
  USING (public.trans_current_perfil() IN ('admin','almoxarife'));
