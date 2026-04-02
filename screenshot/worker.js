/**
 * FLixBD Image Host — deploy with Wrangler from this folder.
 *
 * BOT_TOKEN (same bot as Django → Screenshot settings → telegram_bot_token):
 *   Preferred: wrangler secret put BOT_TOKEN
 *   Or paste below in HARDCODED_BOT_TOKEN (do not commit real tokens to a public repo).
 *
 * Secrets:
 *   wrangler secret put CRYPTO_PHRASE   # same as ScreenshotSettings.crypto_phrase
 *
 * Routes:
 *   GET /              → "FLixBD Image Host"
 *   GET /image/{token}/{filename}  → proxy Telegram file (Content-Disposition filename)
 *   GET /image/{token}             → optional: no filename segment
 *
 * Token payload: encrypts Telegram file_id. Worker calls getFile → file_path → file download.
 */

/** @type {string} Paste token here only if Worker secrets are not set, e.g. "123456789:AAH..." */
const HARDCODED_BOT_TOKEN = "";

export default {
  async fetch(request, env) {
    const BOT_TOKEN = String(env.BOT_TOKEN || HARDCODED_BOT_TOKEN || "").trim();
    const CRYPTO_PHRASE = env.CRYPTO_PHRASE || "FLixBD-image-host-crypto-v1";
    if (!BOT_TOKEN) {
      return new Response("Worker misconfigured: set BOT_TOKEN secret or HARDCODED_BOT_TOKEN", {
        status: 500,
      });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/" || path === "") {
      return new Response("FLixBD Image Host", {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    const prefix = "/image/";
    if (!path.startsWith(prefix)) {
      return new Response("Not found", { status: 404 });
    }

    const rest = path.slice(prefix.length);
    if (!rest) {
      return new Response("Missing token", { status: 400 });
    }

    let enc;
    let downloadName = null;
    const cut = rest.indexOf("/");
    if (cut === -1) {
      enc = rest;
    } else {
      enc = rest.slice(0, cut);
      try {
        downloadName = decodeURIComponent(rest.slice(cut + 1));
      } catch {
        return new Response("Bad filename", { status: 400 });
      }
      if (!downloadName) {
        return new Response("Missing filename", { status: 400 });
      }
    }

    let filePath;
    try {
      const plain = await decryptToPlaintext(enc, CRYPTO_PHRASE);
      const parsed = parseImagePayload(plain);
      const fileId = parsed.filePath;
      if (!downloadName && parsed.downloadName) {
        downloadName = parsed.downloadName;
      }
      const resolved = await resolveFilePathViaGetFile(fileId, BOT_TOKEN);
      if (!resolved) {
        return new Response("File not found", { status: 404 });
      }
      filePath = resolved;
    } catch {
      return new Response("Bad token", { status: 400 });
    }

    if (filePath.includes("..") || filePath.startsWith("/")) {
      return new Response("Bad path", { status: 400 });
    }

    const safePath = filePath.replace(/^\/+/, "");
    const tgUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${safePath}`;
    const tg = await fetch(tgUrl);
    if (!tg.ok) {
      return new Response(await tg.text(), { status: tg.status });
    }

    const ct = tg.headers.get("content-type") || "application/octet-stream";
    const extra = downloadName ? contentDispositionHeader(downloadName) : {};
    return new Response(tg.body, {
      headers: {
        "content-type": ct,
        "cache-control": "public, max-age=300",
        ...extra,
      },
    });
  },
};

function base64UrlToBytes(s) {
  let b64 = s.replace(/-/g, "+").replace(/_/g, "/");
  const pad = (4 - (b64.length % 4)) % 4;
  b64 += "=".repeat(pad);
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function decryptToPlaintext(token, phrase) {
  const bin = base64UrlToBytes(token);
  if (bin.length < 13) throw new Error("short");
  const nonce = bin.slice(0, 12);
  const ciphertext = bin.slice(12);
  const keyRaw = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(phrase));
  const key = await crypto.subtle.importKey("raw", keyRaw, { name: "AES-GCM" }, false, [
    "decrypt",
  ]);
  const pt = await crypto.subtle.decrypt({ name: "AES-GCM", iv: nonce }, key, ciphertext);
  return new TextDecoder().decode(pt);
}

function parseImagePayload(decrypted) {
  if (decrypted.startsWith("{")) {
    const o = JSON.parse(decrypted);
    const p = o.path ?? o.p;
    const name = o.name ?? o.n ?? null;
    if (typeof p !== "string" || !p) throw new Error("bad payload");
    return { filePath: p, downloadName: typeof name === "string" ? name : null };
  }
  return { filePath: decrypted, downloadName: null };
}

/** Decrypted payload must be Telegram file_id; resolve current CDN path via getFile. */
async function resolveFilePathViaGetFile(fileId, botToken) {
  const t = fileId.trim();
  if (!t) return null;

  try {
    const gfUrl = `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(t)}`;
    const gfRes = await fetch(gfUrl);
    let gfData;
    try {
      gfData = await gfRes.json();
    } catch {
      gfData = { ok: false };
    }
    if (gfData.ok && gfData.result && gfData.result.file_path) {
      return String(gfData.result.file_path).replace(/^\/+/, "");
    }
  } catch {
    return null;
  }

  return null;
}

function contentDispositionHeader(filename) {
  if (!filename || filename.length > 200) return {};
  if (/[\r\n\\/]/.test(filename)) return {};
  const star = encodeURIComponent(filename);
  const ascii = filename.replace(/"/g, '\\"');
  return {
    "content-disposition": `inline; filename="${ascii}"; filename*=UTF-8''${star}`,
  };
}
