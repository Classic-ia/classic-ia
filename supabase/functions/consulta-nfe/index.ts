// Supabase Edge Function: Consulta NFe na SEFAZ
// Usa certificado digital A1 (.pfx) para autenticação mTLS
//
// Deploy:
//   supabase functions deploy consulta-nfe
//
// Secrets necessários (via supabase secrets set):
//   CERT_PFX_BASE64  — certificado .pfx codificado em base64
//   CERT_PASSWORD     — senha do certificado
//
// Uso:
//   POST /functions/v1/consulta-nfe
//   Body: { "chave_nfe": "43241208849964000110550010000092901234567890" }

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";

// URLs dos Web Services SEFAZ por UF (NFeConsultaProtocolo 4.00)
const SEFAZ_WS: Record<string, string> = {
  // Autorizadores próprios
  "AM": "https://nfe.sefaz.am.gov.br/services2/services/NfeConsulta4",
  "BA": "https://nfe.sefaz.ba.gov.br/webservices/NFeConsultaProtocolo4/NFeConsultaProtocolo4.asmx",
  "CE": "https://nfe.sefaz.ce.gov.br/nfe4/services/NFeConsultaProtocolo4",
  "GO": "https://nfe.sefaz.go.gov.br/nfe/services/NFeConsultaProtocolo4",
  "MG": "https://nfe.fazenda.mg.gov.br/nfe2/services/NFeConsultaProtocolo4",
  "MS": "https://nfe.sefaz.ms.gov.br/ws/NFeConsultaProtocolo4",
  "MT": "https://nfe.sefaz.mt.gov.br/nfews/v2/services/NfeConsulta4",
  "PE": "https://nfe.sefaz.pe.gov.br/nfe-service/services/NFeConsultaProtocolo4",
  "PR": "https://nfe.sefa.pr.gov.br/nfe/NFeConsultaProtocolo4",
  "RS": "https://nfe.sefazrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "SP": "https://nfe.fazenda.sp.gov.br/ws/nfeconsultaprotocolo4.asmx",
  // SVRS (estados que usam Sefaz Virtual RS)
  "AC": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "AL": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "AP": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "DF": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "ES": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "MA": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "PA": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "PB": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "PI": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "RJ": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "RN": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "RO": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "RR": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "SC": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "SE": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
  "TO": "https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
};

// Código UF IBGE → sigla
const UF_CODES: Record<string, string> = {
  "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
  "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
  "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
  "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
  "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
  "52": "GO", "53": "DF",
};

// Status NFe codes
const STATUS_CODES: Record<string, string> = {
  "100": "Autorizada",
  "101": "Cancelada",
  "102": "Inutilizada",
  "110": "Denegada",
  "150": "Autorizada (fora de prazo)",
  "151": "Cancelada (fora de prazo)",
  "155": "Cancelada (substituição)",
  "135": "Evento registrado e vinculado",
  "301": "Denegada - Irregularidade fiscal do emitente",
  "302": "Denegada - Irregularidade fiscal do destinatário",
  "303": "Denegada - Irregularidade fiscal do emitente",
};

function buildSoapEnvelope(chaveNfe: string, tpAmb: string = "1"): string {
  return `<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap12:Body>
    <nfeDadosMsg xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeConsultaProtocolo4">
      <consSitNFe versao="4.00" xmlns="http://www.portalfiscal.inf.br/nfe">
        <tpAmb>${tpAmb}</tpAmb>
        <xServ>CONSULTAR</xServ>
        <chNFe>${chaveNfe}</chNFe>
      </consSitNFe>
    </nfeDadosMsg>
  </soap12:Body>
</soap12:Envelope>`;
}

function parseResponse(xml: string): {
  cStat: string;
  xMotivo: string;
  dhRecbto?: string;
  nProt?: string;
  cSitNFe?: string;
} {
  const extract = (tag: string): string => {
    const match = xml.match(new RegExp(`<${tag}[^>]*>([^<]*)</${tag}>`));
    return match ? match[1] : "";
  };

  const cStat = extract("cStat");
  const xMotivo = extract("xMotivo");
  const dhRecbto = extract("dhRecbto");
  const nProt = extract("nProt");

  return { cStat, xMotivo, dhRecbto, nProt };
}

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const { chave_nfe } = await req.json();

    if (!chave_nfe || chave_nfe.length !== 44) {
      return new Response(
        JSON.stringify({ error: "Chave NFe inválida. Deve ter 44 dígitos." }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Extrair UF da chave (primeiros 2 dígitos)
    const ufCode = chave_nfe.substring(0, 2);
    const uf = UF_CODES[ufCode];
    if (!uf) {
      return new Response(
        JSON.stringify({ error: `Código UF '${ufCode}' não reconhecido.` }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const wsUrl = SEFAZ_WS[uf];
    if (!wsUrl) {
      return new Response(
        JSON.stringify({ error: `Web Service não configurado para UF: ${uf}` }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Carregar certificado A1
    const certBase64 = Deno.env.get("CERT_PFX_BASE64");
    const certPassword = Deno.env.get("CERT_PASSWORD");

    if (!certBase64 || !certPassword) {
      return new Response(
        JSON.stringify({ error: "Certificado digital não configurado. Configure CERT_PFX_BASE64 e CERT_PASSWORD." }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Decodificar certificado PFX
    const certBytes = Uint8Array.from(atob(certBase64), c => c.charCodeAt(0));

    // Montar envelope SOAP
    const soapBody = buildSoapEnvelope(chave_nfe);

    // Criar cliente HTTP com certificado mTLS
    const httpClient = Deno.createHttpClient({
      certChain: undefined,
      privateKey: undefined,
      // Deno suporta certificado via caCerts ou TLS
      // Para mTLS com PFX, precisa extrair cert + key do PFX
      // Nota: Deno ainda não suporta PFX nativamente
      // Alternativa: usar a lib pkcs12 para extrair PEM
    });

    // NOTA: Deno Edge Functions ainda não suportam mTLS com PFX nativamente.
    // Solução alternativa: converter PFX → PEM (cert + key) e usar certChain/privateKey.
    //
    // Para deploy funcional, execute:
    //   openssl pkcs12 -in certificado.pfx -out cert.pem -clcerts -nokeys
    //   openssl pkcs12 -in certificado.pfx -out key.pem -nocerts -nodes
    //
    // Depois salve como secrets:
    //   supabase secrets set CERT_PEM="$(cat cert.pem)"
    //   supabase secrets set CERT_KEY="$(cat key.pem)"

    const certPem = Deno.env.get("CERT_PEM");
    const certKey = Deno.env.get("CERT_KEY");

    if (!certPem || !certKey) {
      // Fallback: retornar instrução de configuração
      return new Response(
        JSON.stringify({
          error: "Certificado PEM não configurado.",
          instrucoes: [
            "1. Converter PFX para PEM:",
            "   openssl pkcs12 -in certificado.pfx -out cert.pem -clcerts -nokeys",
            "   openssl pkcs12 -in certificado.pfx -out key.pem -nocerts -nodes",
            "2. Salvar como secrets do Supabase:",
            "   supabase secrets set CERT_PEM=\"$(cat cert.pem)\"",
            "   supabase secrets set CERT_KEY=\"$(cat key.pem)\"",
          ]
        }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Criar cliente com certificado mTLS
    const client = Deno.createHttpClient({
      certChain: certPem,
      privateKey: certKey,
    });

    // Fazer chamada SOAP
    const response = await fetch(wsUrl, {
      method: "POST",
      client,
      headers: {
        "Content-Type": "application/soap+xml; charset=utf-8",
        "SOAPAction": "http://www.portalfiscal.inf.br/nfe/wsdl/NFeConsultaProtocolo4/nfeConsultaNF",
      },
      body: soapBody,
    });

    client.close();

    if (!response.ok) {
      const errText = await response.text();
      return new Response(
        JSON.stringify({ error: `SEFAZ retornou ${response.status}`, detalhe: errText.substring(0, 500) }),
        { status: 502, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const xmlResponse = await response.text();
    const parsed = parseResponse(xmlResponse);

    const statusLabel = STATUS_CODES[parsed.cStat] || parsed.xMotivo || "Desconhecido";

    return new Response(
      JSON.stringify({
        chave_nfe,
        uf,
        status_codigo: parsed.cStat,
        status: statusLabel,
        motivo: parsed.xMotivo,
        protocolo: parsed.nProt || null,
        data_recebimento: parsed.dhRecbto || null,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (err) {
    return new Response(
      JSON.stringify({ error: "Erro interno: " + (err as Error).message }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});
