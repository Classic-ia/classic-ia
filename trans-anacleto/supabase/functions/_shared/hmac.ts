// HMAC-SHA256 helper para validar webhooks Onixsat.
// O secret e configurado em trans_onixsat_config.webhook_secret e deve
// ser compartilhado com o Onixsat. Cada requisicao vem com header
// X-Signature = hex(HMAC-SHA256(body, secret)).
export async function verifyHmac(
  body: string,
  signature: string | null,
  secret: string,
): Promise<boolean> {
  if (!signature || !secret) return false;
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sigBuf = await crypto.subtle.sign("HMAC", key, enc.encode(body));
  const hex = Array.from(new Uint8Array(sigBuf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const received = signature.replace(/^sha256=/, "").toLowerCase();
  return timingSafeEqual(hex, received);
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}
