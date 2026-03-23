#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════
# Classic CQ — n8n Setup Script
# ══════════════════════════════════════════════════════════
# Uso: cd n8n && bash setup.sh
# ══════════════════════════════════════════════════════════
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo ""
echo "================================================"
echo "  Classic CQ — Configuracao n8n"
echo "================================================"
echo ""

# ── 1. Verificar Docker ──────────────────────────────────
if ! command -v docker &> /dev/null; then
  echo -e "${RED}Docker nao encontrado.${NC}"
  echo "Instale em: https://docs.docker.com/get-docker/"
  exit 1
fi

if ! docker compose version &> /dev/null; then
  echo -e "${RED}Docker Compose v2 nao encontrado.${NC}"
  echo "Atualize o Docker Desktop ou instale o plugin compose."
  exit 1
fi

echo -e "${GREEN}Docker OK${NC}"

# ── 2. Criar .env se nao existe ──────────────────────────
if [ ! -f .env ]; then
  echo ""
  echo -e "${YELLOW}Arquivo .env nao encontrado. Criando a partir do template...${NC}"
  cp .env.example .env

  echo ""
  echo "Preencha as credenciais:"
  echo ""

  # Supabase Service Key
  read -rp "SUPABASE_SERVICE_KEY (service_role do Supabase): " SB_KEY
  if [ -n "$SB_KEY" ]; then
    sed -i "s|SUPABASE_SERVICE_KEY=.*|SUPABASE_SERVICE_KEY=${SB_KEY}|" .env
  fi

  # ATAK Token
  read -rp "ATAK_API_TOKEN (token da API ATAK): " ATAK_TOKEN
  if [ -n "$ATAK_TOKEN" ]; then
    sed -i "s|ATAK_API_TOKEN=.*|ATAK_API_TOKEN=${ATAK_TOKEN}|" .env
  fi

  # n8n password
  read -rp "Senha do painel n8n [admin123]: " N8N_PASS
  N8N_PASS=${N8N_PASS:-admin123}
  sed -i "s|N8N_BASIC_AUTH_PASSWORD=.*|N8N_BASIC_AUTH_PASSWORD=${N8N_PASS}|" .env

  echo ""
  echo -e "${GREEN}.env criado com sucesso${NC}"
else
  echo -e "${GREEN}.env ja existe${NC}"
fi

# ── 3. Copiar workflows ─────────────────────────────────
echo ""
echo "Copiando workflows..."
WORKFLOW_DIR="../integracao_atak_cq"
mkdir -p workflows

for f in "$WORKFLOW_DIR"/n8n_fluxo_*.json; do
  if [ -f "$f" ]; then
    cp "$f" workflows/
    echo "  $(basename "$f")"
  fi
done
echo -e "${GREEN}Workflows copiados para ./workflows/${NC}"

# ── 4. Subir container ──────────────────────────────────
echo ""
echo "Subindo n8n..."
docker compose up -d

echo ""
echo "Aguardando n8n ficar pronto..."
for i in $(seq 1 30); do
  if docker compose exec -T n8n wget -qO- http://localhost:5678/healthz 2>/dev/null | grep -q "ok"; then
    echo -e "${GREEN}n8n esta rodando!${NC}"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo -e "${YELLOW}n8n ainda subindo... verifique com: docker compose logs -f${NC}"
  fi
  sleep 2
done

# ── 5. Importar workflows via API ────────────────────────
echo ""
echo "Importando workflows..."

N8N_USER=$(grep N8N_BASIC_AUTH_USER .env | cut -d= -f2)
N8N_PASS=$(grep N8N_BASIC_AUTH_PASSWORD .env | cut -d= -f2)
N8N_USER=${N8N_USER:-admin}
N8N_BASE="http://localhost:5678"

IMPORTED=0
FAILED=0

for wf in workflows/n8n_fluxo_*.json; do
  if [ ! -f "$wf" ]; then continue; fi
  WF_NAME=$(basename "$wf" .json)

  RESP=$(curl -s -w "\n%{http_code}" -X POST "${N8N_BASE}/api/v1/workflows" \
    -u "${N8N_USER}:${N8N_PASS}" \
    -H "Content-Type: application/json" \
    -d @"$wf" 2>/dev/null || echo "000")

  HTTP_CODE=$(echo "$RESP" | tail -1)

  if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo -e "  ${GREEN}OK${NC} $WF_NAME"
    IMPORTED=$((IMPORTED + 1))
  else
    echo -e "  ${YELLOW}--${NC} $WF_NAME (HTTP $HTTP_CODE — importe manualmente)"
    FAILED=$((FAILED + 1))
  fi
done

echo ""
echo "================================================"
echo -e "  ${GREEN}Setup concluido!${NC}"
echo "================================================"
echo ""
echo "  Painel n8n:  http://localhost:5678"
echo "  Usuario:     ${N8N_USER}"
echo "  Senha:       (definida no .env)"
echo ""
echo "  Workflows importados: ${IMPORTED}"
if [ "$FAILED" -gt 0 ]; then
  echo -e "  ${YELLOW}Falharam: ${FAILED} — importe manualmente no painel${NC}"
fi
echo ""
echo "  Proximos passos:"
echo "    1. Acesse http://localhost:5678"
echo "    2. Ative cada workflow (toggle no canto superior direito)"
echo "    3. Execute 'Sync Cadastros' manualmente primeiro"
echo "    4. Depois ative 'Sync Cargas'"
echo "    5. Verifique os dados no Supabase"
echo ""
