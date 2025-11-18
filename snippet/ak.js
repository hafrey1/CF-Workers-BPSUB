const FIXED_UUID = '';// stallTCP v1.3 from https://t.me/Enkelte_notif/784
import { connect } from 'cloudflare:sockets';
let 反代IP = '';
let 启用SOCKS5反代 = null;
let 启用SOCKS5全局反代 = false;
let 我的SOCKS5账号 = '';
//////////////////////////////////////////////////////////////////////////stall参数////////////////////////////////////////////////////////////////////////
const MAX_PENDING = 2097152, KEEPALIVE = 15000, STALL_TO = 8000, MAX_STALL = 12, MAX_RECONN = 24;
//////////////////////////////////////////////////////////////////////////主要架构////////////////////////////////////////////////////////////////////////
const buildUUID = (a, i) => Array.from(a.slice(i, i + 16)).map(n => n.toString(16).padStart(2, '0')).join('').replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
const extractAddr = b => {
  const o1 = 18 + b[17] + 1, p = (b[o1] << 8) | b[o1 + 1], t = b[o1 + 2]; let o2 = o1 + 3, h, l;
  switch (t) {
    case 1: l = 4; h = b.slice(o2, o2 + l).join('.'); break;
    case 2: l = b[o2++]; h = new TextDecoder().decode(b.slice(o2, o2 + l)); break;
    case 3: l = 16; h = `[${Array.from({ length: 8 }, (_, i) => ((b[o2 + i * 2] << 8) | b[o2 + i * 2 + 1]).toString(16)).join(':')}]`; break;
    default: throw new Error('Invalid address type.');
  } return { host: h, port: p, payload: b.slice(o2 + l) };
};
class Pool {
  constructor() { this.buf = new ArrayBuffer(16384); this.ptr = 0; this.pool = []; this.max = 8; this.large = false; }
  alloc = s => {
    if (s <= 4096 && s <= 16384 - this.ptr) { const v = new Uint8Array(this.buf, this.ptr, s); this.ptr += s; return v; } const r = this.pool.pop();
    if (r && r.byteLength >= s) return new Uint8Array(r.buffer, 0, s); return new Uint8Array(s);
  };
  free = b => {
    if (b.buffer === this.buf) { this.ptr = Math.max(0, this.ptr - b.length); return; }
    if (this.pool.length < this.max && b.byteLength >= 1024) this.pool.push(b);
  }; enableLarge = () => { this.large = true; }; reset = () => { this.ptr = 0; this.pool.length = 0; this.large = false; };
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    反代IP = 反代IP ? 反代IP : request.cf.colo + '.PrOxYip.CmLiuSsSs.nEt';
    我的SOCKS5账号 = url.searchParams.get('socks5') || url.searchParams.get('http');
    启用SOCKS5全局反代 = url.searchParams.has('globalproxy') || 启用SOCKS5全局反代;
    if (url.pathname.toLowerCase().includes('/socks5=') || (url.pathname.includes('/s5=')) || (url.pathname.includes('/gs5='))) {
      我的SOCKS5账号 = url.pathname.split('5=')[1];
      启用SOCKS5反代 = 'socks5';
      启用SOCKS5全局反代 = url.pathname.includes('/gs5=') ? true : 启用SOCKS5全局反代;
    } else if (url.pathname.toLowerCase().includes('/http=')) {
      我的SOCKS5账号 = url.pathname.split('/http=')[1];
      启用SOCKS5反代 = 'http';
    } else if (url.pathname.toLowerCase().includes('/socks://') || url.pathname.toLowerCase().includes('/socks5://') || url.pathname.toLowerCase().includes('/http://')) {
      启用SOCKS5反代 = (url.pathname.includes('/http://')) ? 'http' : 'socks5';
      我的SOCKS5账号 = url.pathname.split('://')[1].split('#')[0];
      if (我的SOCKS5账号.includes('@')) {
        const lastAtIndex = 我的SOCKS5账号.lastIndexOf('@');
        let userPassword = 我的SOCKS5账号.substring(0, lastAtIndex).replaceAll('%3D', '=');
        const base64Regex = /^(?:[A-Z0-9+/]{4})*(?:[A-Z0-9+/]{2}==|[A-Z0-9+/]{3}=)?$/i;
        if (base64Regex.test(userPassword) && !userPassword.includes(':')) userPassword = atob(userPassword);
        我的SOCKS5账号 = `${userPassword}@${我的SOCKS5账号.substring(lastAtIndex + 1)}`;
      }
      启用SOCKS5全局反代 = true;//开启全局SOCKS5
    }

    if (我的SOCKS5账号) {
      try {
        获取SOCKS5账号(我的SOCKS5账号);
        启用SOCKS5反代 = url.searchParams.get('http') ? 'http' : 启用SOCKS5反代;
      } catch (err) {
        启用SOCKS5反代 = null;
      }
    } else {
      启用SOCKS5反代 = null;
    }

    if (url.searchParams.has('proxyip')) {
      反代IP = url.searchParams.get('proxyip');
      启用SOCKS5反代 = null;
    } else if (url.pathname.toLowerCase().includes('/proxyip=')) {
      反代IP = url.pathname.toLowerCase().split('/proxyip=')[1];
      启用SOCKS5反代 = null;
    } else if (url.pathname.toLowerCase().includes('/proxyip.')) {
      反代IP = `proxyip.${url.pathname.toLowerCase().split("/proxyip.")[1]}`;
      启用SOCKS5反代 = null;
    } else if (url.pathname.toLowerCase().includes('/pyip=')) {
      反代IP = url.pathname.toLowerCase().split('/pyip=')[1];
      启用SOCKS5反代 = null;
    } else if (url.pathname.toLowerCase().includes('/ip=')) {
      反代IP = url.pathname.toLowerCase().split('/ip=')[1];
      启用SOCKS5反代 = null;
    }
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('Hello World!', { status: 200 });
    const { 0: c, 1: s } = new WebSocketPair(); s.accept(); handle(s);
    return new Response(null, { status: 101, webSocket: c });
  }
};
const handle = ws => {
  const pool = new Pool(); let sock, w, r, info, first = true, rxBytes = 0, stalls = 0, reconns = 0;
  let lastAct = Date.now(), conn = false, reading = false; const tmrs = {}, pend = [];
  let pendBytes = 0, score = 1.0, lastChk = Date.now(), lastRx = 0, succ = 0, fail = 0;
  let stats = { tot: 0, cnt: 0, big: 0, win: 0, ts: Date.now() }; let mode = 'direct', avgSz = 0, tputs = [];
  const updateMode = s => {
    stats.tot += s; stats.cnt++; if (s > 8192) stats.big++; avgSz = avgSz * 0.9 + s * 0.1; const now = Date.now();
    if (now - stats.ts > 1000) {
      const rate = stats.win; tputs.push(rate); if (tputs.length > 5) tputs.shift(); stats.win = s; stats.ts = now;
      const avg = tputs.reduce((a, b) => a + b, 0) / tputs.length;
      if (stats.cnt >= 20) {
        if (avg > 20971520 && avgSz > 16384) { if (mode !== 'buffered') { mode = 'buffered'; pool.enableLarge(); } }
        else if (avg < 10485760 || avgSz < 8192) { if (mode !== 'direct') mode = 'direct'; }
        else { if (mode !== 'adaptive') mode = 'adaptive'; }
      }
    } else { stats.win += s; }
  };
  const readLoop = async () => {
    if (reading) return; reading = true; let batch = [], bSz = 0, bTmr = null;
    const flush = () => {
      if (!bSz) return; const m = new Uint8Array(bSz); let p = 0;
      for (const c of batch) { m.set(c, p); p += c.length; }
      if (ws.readyState === 1) ws.send(m);
      batch = []; bSz = 0; if (bTmr) { clearTimeout(bTmr); bTmr = null; }
    };
    try {
      while (true) {
        if (pendBytes > MAX_PENDING) { await new Promise(res => setTimeout(res, 100)); continue; }
        const { done, value: v } = await r.read();
        if (v?.length) {
          rxBytes += v.length; lastAct = Date.now(); stalls = 0; updateMode(v.length); const now = Date.now();
          if (now - lastChk > 5000) {
            const el = now - lastChk, by = rxBytes - lastRx, tp = by / el;
            if (tp > 500) score = Math.min(1.0, score + 0.05);
            else if (tp < 50) score = Math.max(0.1, score - 0.05);
            lastChk = now; lastRx = rxBytes;
          }
          if (mode === 'buffered') {
            if (v.length < 32768) {
              batch.push(v); bSz += v.length;
              if (bSz >= 131072) flush();
              else if (!bTmr) bTmr = setTimeout(flush, avgSz > 16384 ? 5 : 20);
            } else { flush(); if (ws.readyState === 1) ws.send(v); }
          } else if (mode === 'adaptive') {
            if (v.length < 4096) {
              batch.push(v); bSz += v.length;
              if (bSz >= 32768) flush();
              else if (!bTmr) bTmr = setTimeout(flush, 15);
            } else { flush(); if (ws.readyState === 1) ws.send(v); }
          } else { flush(); if (ws.readyState === 1) ws.send(v); }
        } if (done) { flush(); reading = false; reconn(); break; }
      }
    } catch (e) { flush(); if (bTmr) clearTimeout(bTmr); reading = false; fail++; reconn(); }
  };
  const establish = async sp => {
    try {
      sock = await sp; await sock.opened; w = sock.writable.getWriter(); r = sock.readable.getReader(); const bt = pend.splice(0, 10);
      for (const b of bt) { await w.write(b); pendBytes -= b.length; pool.free(b); }
      conn = false; reconns = 0; score = Math.min(1.0, score + 0.15); succ++; lastAct = Date.now(); readLoop();
    } catch (e) { conn = false; fail++; score = Math.max(0.1, score - 0.2); reconn(); }
  };
  const reconn = async () => {
    if (!info || ws.readyState !== 1) { cleanup(); ws.close(1011, 'Invalid.'); return; }
    if (reconns >= MAX_RECONN) { cleanup(); ws.close(1011, 'Max reconnect.'); return; }
    if (score < 0.3 && reconns > 5 && Math.random() > 0.6) { cleanup(); ws.close(1011, 'Poor network.'); return; }
    if (conn) return; reconns++; let d = Math.min(50 * Math.pow(1.5, reconns - 1), 3000);
    d *= (1.5 - score * 0.5); d += (Math.random() - 0.5) * d * 0.2; d = Math.max(50, Math.floor(d));
    try {
      cleanSock();
      if (pendBytes > MAX_PENDING * 2) {
        while (pendBytes > MAX_PENDING && pend.length > 5) { const drop = pend.shift(); pendBytes -= drop.length; pool.free(drop); }
      }
      await new Promise(res => setTimeout(res, d)); conn = true;
      sock = connect({ hostname: info.host, port: info.port }); await sock.opened;
      w = sock.writable.getWriter(); r = sock.readable.getReader(); const bt = pend.splice(0, 10);
      for (const b of bt) { await w.write(b); pendBytes -= b.length; pool.free(b); }
      conn = false; reconns = 0; score = Math.min(1.0, score + 0.15); succ++; stalls = 0; lastAct = Date.now(); readLoop();
    } catch (e) {
      conn = false; fail++; score = Math.max(0.1, score - 0.2);
      if (reconns < MAX_RECONN && ws.readyState === 1) setTimeout(reconn, 500);
      else { cleanup(); ws.close(1011, 'Exhausted.'); }
    }
  };
  const startTmrs = () => {
    tmrs.ka = setInterval(async () => {
      if (!conn && w && Date.now() - lastAct > KEEPALIVE) { try { await w.write(new Uint8Array(0)); lastAct = Date.now(); } catch (e) { reconn(); } }
    }, KEEPALIVE / 3);
    tmrs.hc = setInterval(() => {
      if (!conn && stats.tot > 0 && Date.now() - lastAct > STALL_TO) {
        stalls++;
        if (stalls >= MAX_STALL) {
          if (reconns < MAX_RECONN) { stalls = 0; reconn(); }
          else { cleanup(); ws.close(1011, 'Stall.'); }
        }
      }
    }, STALL_TO / 2);
  };
  const cleanSock = () => { reading = false; try { w?.releaseLock(); r?.releaseLock(); sock?.close(); } catch { } };
  const cleanup = () => {
    Object.values(tmrs).forEach(clearInterval); cleanSock();
    while (pend.length) pool.free(pend.shift());
    pendBytes = 0; stats = { tot: 0, cnt: 0, big: 0, win: 0, ts: Date.now() };
    mode = 'direct'; avgSz = 0; tputs = []; pool.reset();
  };
  ws.addEventListener('message', async e => {
    try {
      if (first) {
        first = false; const b = new Uint8Array(e.data);
        ws.send(new Uint8Array([b[0], 0]));
        if (FIXED_UUID && buildUUID(b, 1) !== FIXED_UUID) throw new Error('Auth failed.');
        const { host, port, payload } = extractAddr(b); if (host.includes(atob('c3BlZWQuY2xvdWRmbGFyZS5jb20='))) throw new Error('Access'); info = { host, port }; conn = true;
        let sp;
        if (启用SOCKS5反代 == 'socks5' && 启用SOCKS5全局反代) {
          sp = await socks5Connect(host, port);
        } else if (启用SOCKS5反代 == 'http' && 启用SOCKS5全局反代) {
          sp = await httpConnect(host, port);
        } else {
          try {
            sp = connect({ hostname: host, port });
            await sp.opened;
          } catch {
            if (启用SOCKS5反代 == 'socks5') {
              sp = await socks5Connect(host, port);
            } else if (启用SOCKS5反代 == 'http') {
              sp = await httpConnect(host, port);
            } else {
              const [反代IP地址, 反代IP端口] = await 解析地址端口(反代IP);
              sp = connect({ hostname: 反代IP地址, port: 反代IP端口 });
            }
          }
        }
        await sp.opened;
        if (payload.length) { const buf = pool.alloc(payload.length); buf.set(payload); pend.push(buf); pendBytes += buf.length; } startTmrs(); establish(sp);
      } else {
        lastAct = Date.now();
        if (conn || !w) { const buf = pool.alloc(e.data.byteLength); buf.set(new Uint8Array(e.data)); pend.push(buf); pendBytes += buf.length; }
        else { await w.write(e.data); }
      }
    } catch (err) { cleanup(); ws.close(1006, 'Error.'); }
  }); ws.addEventListener('close', cleanup); ws.addEventListener('error', cleanup);
};

async function 获取SOCKS5账号(address) {
  const lastAtIndex = address.lastIndexOf("@");
  let [latter, former] = lastAtIndex === -1 ? [address, undefined] : [address.substring(lastAtIndex + 1), address.substring(0, lastAtIndex)];
  let username, password, hostname, port;
  if (former) {
    const formers = former.split(":");
    if (formers.length !== 2) {
      throw new Error('无效的 SOCKS 地址格式：认证部分必须是 "username:password" 的形式');
    }
    [username, password] = formers;
  }
  const latters = latter.split(":");
  if (latters.length > 2 && latter.includes("]:")) {
    port = Number(latter.split("]:")[1].replace(/[^\d]/g, ''));
    hostname = latter.split("]:")[0] + "]";
  } else if (latters.length === 2) {
    port = Number(latters.pop().replace(/[^\d]/g, ''));
    hostname = latters.join(":");
  } else {
    port = 80;
    hostname = latter;
  }

  if (isNaN(port)) {
    throw new Error('无效的 SOCKS 地址格式：端口号必须是数字');
  }
  const regex = /^\[.*\]$/;
  if (hostname.includes(":") && !regex.test(hostname)) {
    throw new Error('无效的 SOCKS 地址格式：IPv6 地址必须用方括号括起来，如 [2001:db8::1]');
  }
  return { username, password, hostname, port };
}
async function 解析地址端口(proxyIP) {
  proxyIP = proxyIP.toLowerCase();
  let 地址 = proxyIP, 端口 = 443;
  if (proxyIP.includes('.tp')) {
    const tpMatch = proxyIP.match(/\.tp(\d+)/);
    if (tpMatch) 端口 = parseInt(tpMatch[1], 10);
    return [地址, 端口];
  }
  if (proxyIP.includes(']:')) {
    const parts = proxyIP.split(']:');
    地址 = parts[0] + ']';
    端口 = parseInt(parts[1], 10) || 端口;
  } else if (proxyIP.includes(':') && !proxyIP.startsWith('[')) {
    const colonIndex = proxyIP.lastIndexOf(':');
    地址 = proxyIP.slice(0, colonIndex);
    端口 = parseInt(proxyIP.slice(colonIndex + 1), 10) || 端口;
  }
  return [地址, 端口];
}
async function httpConnect(addressRemote, portRemote) {
  const { username, password, hostname, port } = await 获取SOCKS5账号(我的SOCKS5账号);
  const sock = await connect({ hostname, port });
  const authHeader = username && password ? `Proxy-Authorization: Basic ${btoa(`${username}:${password}`)}\r\n` : '';
  const connectRequest = `CONNECT ${addressRemote}:${portRemote} HTTP/1.1\r\n` +
    `Host: ${addressRemote}:${portRemote}\r\n` +
    authHeader +
    `User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n` +
    `Proxy-Connection: Keep-Alive\r\n` +
    `Connection: Keep-Alive\r\n\r\n`;
  const writer = sock.writable.getWriter();
  try {
    await writer.write(new TextEncoder().encode(connectRequest));
  } catch (err) {
    throw new Error(`发送HTTP CONNECT请求失败: ${err.message}`);
  } finally {
    writer.releaseLock();
  }
  const reader = sock.readable.getReader();
  let responseBuffer = new Uint8Array(0);
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) throw new Error('HTTP代理连接中断');
      const newBuffer = new Uint8Array(responseBuffer.length + value.length);
      newBuffer.set(responseBuffer);
      newBuffer.set(value, responseBuffer.length);
      responseBuffer = newBuffer;
      const respText = new TextDecoder().decode(responseBuffer);
      if (respText.includes('\r\n\r\n')) {
        const headersEndPos = respText.indexOf('\r\n\r\n') + 4;
        const headers = respText.substring(0, headersEndPos);

        if (!headers.startsWith('HTTP/1.1 200') && !headers.startsWith('HTTP/1.0 200')) {
          throw new Error(`HTTP代理连接失败: ${headers.split('\r\n')[0]}`);
        }
        if (headersEndPos < responseBuffer.length) {
          const remainingData = responseBuffer.slice(headersEndPos);
          const { readable, writable } = new TransformStream();
          new ReadableStream({
            start(controller) {
              controller.enqueue(remainingData);
            }
          }).pipeTo(writable).catch(() => { });
          // @ts-ignore
          sock.readable = readable;
        }
        break;
      }
    }
  } catch (err) {
    throw new Error(`处理HTTP代理响应失败: ${err.message}`);
  } finally {
    reader.releaseLock();
  }
  return sock;
}

async function socks5Connect(targetHost, targetPort) {
  const parsedSocks5Address = await 获取SOCKS5账号(我的SOCKS5账号);
  const { username, password, hostname, port } = parsedSocks5Address;
  const sock = connect({
    hostname: hostname,
    port: port
  });
  await sock.opened;
  const w = sock.writable.getWriter();
  const r = sock.readable.getReader();
  await w.write(new Uint8Array([5, 2, 0, 2]));
  const auth = (await r.read()).value;
  if (auth[1] === 2 && username) {
    const user = new TextEncoder().encode(username);
    const pass = new TextEncoder().encode(password);
    await w.write(new Uint8Array([1, user.length, ...user, pass.length, ...pass]));
    await r.read();
  }
  const domain = new TextEncoder().encode(targetHost);
  await w.write(new Uint8Array([5, 1, 0, 3, domain.length, ...domain,
    targetPort >> 8, targetPort & 0xff
  ]));
  await r.read();
  w.releaseLock();
  r.releaseLock();
  return sock;
}