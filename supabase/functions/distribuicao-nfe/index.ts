// Supabase Edge Function: Distribuição DFe — Buscar NFes da SEFAZ automaticamente
// Usa certificado digital A1 para baixar NFes recebidas direto da SEFAZ
//
// Deploy:
//   supabase functions deploy distribuicao-nfe
//
// Secrets necessários:
//   CERT_PEM      — certificado em PEM
//   CERT_KEY      — chave privada em PEM
//   CERT_PASSWORD — (opcional, se key é encriptada)
//
// Uso:
//   POST /functions/v1/distribuicao-nfe
//   Body:
//     { "cnpj": "08849964000110", "uf": "PR", "ultimo_nsu": "0" }          — busca por NSU
//     { "cnpj": "08849964000110", "uf": "PR", "chave_nfe": "441234..." }   — busca por chave

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";

// SEFAZ AN (Ambiente Nacional) — serviço de distribuição
const SEFAZ_AN_URL = "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx";

// Código UF IBGE
const UF_TO_CODE: Record<string, string> = {
  "AC":"12","AL":"27","AM":"13","AP":"16","BA":"29","CE":"23","DF":"53",
  "ES":"32","GO":"52","MA":"21","MG":"31","MS":"50","MT":"51","PA":"15",
  "PB":"25","PE":"26","PI":"22","PR":"41","RJ":"33","RN":"24","RO":"11",
  "RR":"14","RS":"43","SC":"42","SE":"28","SP":"35","TO":"17",
};

function buildDistribuicaoEnvelope(cUFAutor: string, cnpj: string, ultNSU?: string, chaveNFe?: string): string {
  let consulta = "";
  if (chaveNFe) {
    consulta = `<consChNFe><chNFe>${chaveNFe}</chNFe></consChNFe>`;
  } else {
    consulta = `<distNSU><ultNSU>${(ultNSU || "0").padStart(15, "0")}</ultNSU></distNSU>`;
  }

  return `<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap12:Body>
    <nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe">
      <nfeDadosMsg>
        <distDFeInt versao="1.01" xmlns="http://www.portalfiscal.inf.br/nfe">
          <tpAmb>1</tpAmb>
          <cUFAutor>${cUFAutor}</cUFAutor>
          <CNPJ>${cnpj}</CNPJ>
          ${consulta}
        </distDFeInt>
      </nfeDadosMsg>
    </nfeDistDFeInteresse>
  </soap12:Body>
</soap12:Envelope>`;
}

interface DocNFe {
  nsu: string;
  schema: string;
  xml: string; // XML decompressed
}

function parseDistribuicaoResponse(xml: string): {
  cStat: string;
  xMotivo: string;
  ultNSU: string;
  maxNSU: string;
  docs: DocNFe[];
} {
  const extract = (tag: string): string => {
    const match = xml.match(new RegExp(`<${tag}[^>]*>([^<]*)</${tag}>`));
    return match ? match[1] : "";
  };

  const cStat = extract("cStat");
  const xMotivo = extract("xMotivo");
  const ultNSU = extract("ultNSU");
  const maxNSU = extract("maxNSU");

  // Extrair documentos (docZip contém XMLs compactados em base64+gzip)
  const docs: DocNFe[] = [];
  const docRegex = /<docZip\s+NSU="(\d+)"\s+schema="([^"]*)">([\s\S]*?)<\/docZip>/g;
  let match;
  while ((match = docRegex.exec(xml)) !== null) {
    docs.push({
      nsu: match[1],
      schema: match[2],
      xml: match[3], // base64 + gzipped — needs decompression on client
    });
  }

  return { cStat, xMotivo, ultNSU, maxNSU, docs };
}

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const body = await req.json();
    const { cnpj, uf, ultimo_nsu, chave_nfe } = body;

    if (!cnpj || !uf) {
      return new Response(
        JSON.stringify({ error: "CNPJ e UF são obrigatórios." }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const cUFAutor = UF_TO_CODE[uf.toUpperCase()];
    if (!cUFAutor) {
      return new Response(
        JSON.stringify({ error: `UF '${uf}' não reconhecida.` }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Certificado PEM
    const certPem = Deno.env.get("CERT_PEM");
    const certKey = Deno.env.get("CERT_KEY");

    if (!certPem || !certKey) {
      return new Response(
        JSON.stringify({
          error: "Certificado digital não configurado.",
          instrucoes: [
            "1. Converter PFX para PEM:",
            "   openssl pkcs12 -in certificado.pfx -out cert.pem -clcerts -nokeys",
            "   openssl pkcs12 -in certificado.pfx -out key.pem -nocerts -nodes",
            "2. Salvar como secrets:",
            "   supabase secrets set CERT_PEM=\"$(cat cert.pem)\"",
            "   supabase secrets set CERT_KEY=\"$(cat key.pem)\"",
          ]
        }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const soapBody = buildDistribuicaoEnvelope(cUFAutor, cnpj.replace(/\D/g, ''), ultimo_nsu, chave_nfe);

    const client = Deno.createHttpClient({
      certChain: certPem,
      privateKey: certKey,
    });

    const response = await fetch(SEFAZ_AN_URL, {
      method: "POST",
      client,
      headers: {
        "Content-Type": "application/soap+xml; charset=utf-8",
        "SOAPAction": "http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe/nfeDistDFeInteresse",
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
    const parsed = parseDistribuicaoResponse(xmlResponse);

    // Decompress docs (base64 → gzip → XML)
    const decompressedDocs = [];
    for (const doc of parsed.docs) {
      try {
        const gzipBytes = Uint8Array.from(atob(doc.xml), c => c.charCodeAt(0));
        const ds = new DecompressionStream("gzip");
        const writer = ds.writable.getWriter();
        writer.write(gzipBytes);
        writer.close();
        const reader = ds.readable.getReader();
        const chunks: Uint8Array[] = [];
        let done = false;
        while (!done) {
          const result = await reader.read();
          if (result.value) chunks.push(result.value);
          done = result.done;
        }
        const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
        const merged = new Uint8Array(totalLength);
        let offset = 0;
        for (const chunk of chunks) {
          merged.set(chunk, offset);
          offset += chunk.length;
        }
        const xmlStr = new TextDecoder().decode(merged);
        decompressedDocs.push({
          nsu: doc.nsu,
          schema: doc.schema,
          xml: xmlStr,
        });
      } catch (e) {
        decompressedDocs.push({
          nsu: doc.nsu,
          schema: doc.schema,
          xml: null,
          error: "Erro ao descompactar: " + (e as Error).message,
        });
      }
    }

    return new Response(
      JSON.stringify({
        status_codigo: parsed.cStat,
        motivo: parsed.xMotivo,
        ultimo_nsu: parsed.ultNSU,
        max_nsu: parsed.maxNSU,
        total_docs: decompressedDocs.length,
        documentos: decompressedDocs,
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
