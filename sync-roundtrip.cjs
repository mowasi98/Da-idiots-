const fs = require("fs");
const vm = require("vm");
const html = fs.readFileSync("C:/Users/shahm/Downloads/index (2).html", "utf8");
const script = html.slice(html.indexOf("<script>") + 8, html.lastIndexOf("</script>"));
const elements = {};
function makeEl(id) {
  return {
    id,
    value: id === "totalPool" ? "60" : "",
    style: {},
    textContent: "",
    innerHTML: "",
    checked: true,
    type: id === "sheetDate" ? "date" : "number",
    tagName: "INPUT",
    className: ""
  };
}
const document = {
  getElementById(id) {
    if (!elements[id]) elements[id] = makeEl(id);
    return elements[id];
  },
  addEventListener() {},
  activeElement: null,
  visibilityState: "visible",
  createElement() { return { style: {}, click() {}, href: "" }; },
  body: { appendChild() {}, removeChild() {} }
};
const localStorage = {
  store: {},
  getItem(k) { return Object.prototype.hasOwnProperty.call(this.store, k) ? this.store[k] : null; },
  setItem(k, v) { this.store[k] = String(v); },
  removeItem(k) { delete this.store[k]; }
};
const context = {
  console, setTimeout, clearTimeout, setInterval, clearInterval,
  WebSocket, crypto, TextEncoder, TextDecoder, Uint8Array, Map, Promise, Date, Math, JSON,
  Number, String, parseFloat, isNaN, URL, Blob, atob, btoa, document, localStorage, window: {},
  alert() {}, confirm() { return false; }, prompt() { return null; }, navigator: {}
};
context.window.crypto = crypto;
context.window.addEventListener = () => {};
vm.createContext(context);
try {
  vm.runInContext(script, context, { filename: "sheet.js" });
  console.log("INIT_OK");
} catch (e) {
  console.log("INIT_FAIL", e.stack);
  process.exit(1);
}

const TOPIC = "s/c4e91a7b2d8065f3/v1";
const SECRET = "kR7mQ2vL9pX4sD8nW3aF6cH1";
const enc = new TextEncoder();
const dec = new TextDecoder();
function varInt(n) { const o = []; do { let d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 128; o.push(d); } while (n > 0); return new Uint8Array(o); }
function mqttStr(s) { const u = enc.encode(s); const out = new Uint8Array(2 + u.length); out[0] = u.length >> 8; out[1] = u.length & 255; out.set(u, 2); return out; }
function mqttPacket(header, chunks) { let size = 0; chunks.forEach(c => size += c.length); const rl = varInt(size); const out = new Uint8Array(1 + rl.length + size); out[0] = header; out.set(rl, 1); let o = 1 + rl.length; chunks.forEach(c => { out.set(c, o); o += c.length; }); return out; }
function connectPacket(id) { return mqttPacket(0x10, [new Uint8Array([0, 4, 77, 81, 84, 84, 4, 2, 0, 60]), mqttStr(id)]); }
function subscribePacket(topic) { return mqttPacket(0x82, [new Uint8Array([0, 1]), mqttStr(topic), new Uint8Array([0])]); }
function publishPacket(topic, text) { return mqttPacket(0x31, [mqttStr(topic), enc.encode(text)]); }
function parseMqtt(u) {
  let i = 1, mult = 1, rem = 0, digit;
  do { digit = u[i++]; rem += (digit & 127) * mult; mult *= 128; } while (digit & 128);
  const body = u.slice(i, i + rem);
  const type = u[0] & 0xf0;
  if (type !== 0x30) return { kind: type };
  const tlen = (body[0] << 8) | body[1];
  const topic = dec.decode(body.slice(2, 2 + tlen));
  let off = 2 + tlen;
  const qos = (u[0] >> 1) & 3;
  if (qos > 0) off += 2;
  return { kind: "pub", topic, payload: dec.decode(body.slice(off)) };
}
async function key() {
  const raw = await crypto.subtle.digest("SHA-256", enc.encode(SECRET));
  return crypto.subtle.importKey("raw", raw, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}
async function encryptText(text) {
  const k = await key();
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const cipher = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv }, k, enc.encode(text)));
  const joined = new Uint8Array(iv.length + cipher.length);
  joined.set(iv, 0); joined.set(cipher, iv.length);
  let s = "";
  for (let i = 0; i < joined.length; i += 0x8000) s += String.fromCharCode.apply(null, joined.subarray(i, i + 0x8000));
  return btoa(s);
}
async function decryptText(b64) {
  const k = await key();
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: bytes.slice(0, 12) }, k, bytes.slice(12));
  return dec.decode(plain);
}

const seen = [];
function listen(url) {
  const ws = new WebSocket(url, ["mqtt"]);
  ws.binaryType = "arraybuffer";
  ws.onopen = () => ws.send(connectPacket("probe" + Math.random().toString(16).slice(2, 8)));
  ws.onmessage = async (ev) => {
    const msg = parseMqtt(new Uint8Array(ev.data));
    if (msg.kind === 0x20) ws.send(subscribePacket(TOPIC));
    else if (msg.kind === "pub" && msg.topic === TOPIC) {
      try {
        const obj = JSON.parse(await decryptText(msg.payload));
        seen.push(obj);
        console.log("HEARD", obj.from, "pool", obj.sheet && obj.sheet.pool, "raise", obj.sheet && obj.sheet.rows && obj.sheet.rows[0].raisePct, "logs", (obj.logs || []).length);
      } catch (e) { console.log("DECRYPT_FAIL", e.message); }
    }
  };
  ws.onerror = () => console.log("WS_ERR", url);
  return ws;
}
const ears = [listen("wss://broker.hivemq.com:8884/mqtt"), listen("wss://broker.emqx.io:8084/mqtt")];

setTimeout(async () => {
  const payload = {
    sheetUpdatedAt: Date.now(),
    logsUpdatedAt: 0,
    passUpdatedAt: 0,
    sheet: {
      date: "2026-11-18",
      pool: "123.45",
      rows: [
        { name: "Wasi", raisePct: 5, withdrew: 1, fee: 0.5 },
        { name: "Rayyan", raisePct: 0, withdrew: 0, fee: 0 },
        { name: "Wali", raisePct: 0, withdrew: 0, fee: 0 }
      ],
      expenses: [{ note: "Peppy", amount: 4 }]
    },
    logs: [],
    password: null,
    from: "external-test"
  };
  const hidden = await encryptText(JSON.stringify(payload));
  for (const url of ["wss://broker.hivemq.com:8884/mqtt", "wss://broker.emqx.io:8084/mqtt"]) {
    const ws = new WebSocket(url, ["mqtt"]);
    ws.binaryType = "arraybuffer";
    ws.onopen = () => ws.send(connectPacket("pub" + Math.random().toString(16).slice(2, 8)));
    ws.onmessage = (ev) => {
      const msg = parseMqtt(new Uint8Array(ev.data));
      if (msg.kind === 0x20) {
        ws.send(publishPacket(TOPIC, hidden));
        setTimeout(() => ws.close(), 300);
      }
    };
  }
  console.log("PUBLISHED_REMOTE");
}, 2000);

let appliedRemote = false;
setTimeout(() => {
  const saved = localStorage.getItem("tiktok_excel_sheet_v1");
  console.log("LOCAL_SHEET", saved);
  console.log("POOL_INPUT", elements.totalPool && elements.totalPool.value);
  appliedRemote = String(elements.totalPool.value) === "123.45";
  elements.raise_0 = elements.raise_0 || makeEl("raise_0");
  elements.withdrew_0 = elements.withdrew_0 || makeEl("withdrew_0");
  elements.fee_0 = elements.fee_0 || makeEl("fee_0");
  elements.raise_0.value = "33";
  elements.withdrew_0.value = "2";
  elements.fee_0.value = "0";
  elements.totalPool.value = "77";
  context.updateCell(0);
  console.log("LOCAL_EDIT_SENT");
}, 6000);

setTimeout(() => {
  const gotRemote = seen.some(o => o.from === "external-test");
  const gotPage = seen.some(o => o.from && o.from !== "external-test" && o.sheet && String(o.sheet.pool) === "77");
  console.log("RESULT remoteHeard", gotRemote, "pageHeard", gotPage, "appliedRemote", appliedRemote, "poolNow", elements.totalPool.value);
  ears.forEach(ws => { try { ws.close(); } catch (e) {} });
  process.exit(gotRemote && appliedRemote && gotPage ? 0 : 1);
}, 11000);
