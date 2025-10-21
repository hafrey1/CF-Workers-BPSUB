const FIXED_UUID = '';// ws + xhttp版本
import { connect } from 'cloudflare:sockets';
let 反代IP = '';
let 启用SOCKS5反代 = null;
let 启用SOCKS5全局反代 = false;
let 我的SOCKS5账号 = '';
export default {
    async fetch(request) {
        const url = new URL(request.url);
        反代IP = 反代IP ? 反代IP : request.cf.colo + '.PrOxYiP.CmLiuSssS.nEt';
        我的SOCKS5账号 = url.searchParams.get('socks5') || url.searchParams.get('http');
        启用SOCKS5全局反代 = url.searchParams.has('globalproxy');
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

        // WebSocket 处理
        if (request.headers.get('Upgrade')?.toLowerCase() === 'websocket') {
            return handleWebSocket(request);
        } else {
            if (request.method === 'POST') {
                // XHTTP 处理
                return handleXhttp(request);
            } else {
                return new Response('Hello World!', { status: 200 });
            }
        }
    }
};

// XHTTP 处理函数
async function handleXhttp(request) {
    const vlessHeader = await readVlessHeader(request.body.getReader());
    if (!vlessHeader) return new Response('Invalid VLESS header', {
        status: 400
    });
    if (vlessHeader.hostname.includes(atob('c3BlZWQuY2xvdWRmbGFyZS5jb20='))) throw new Error('Access');
    // 连接到远程
    let remote = null;
    if (启用SOCKS5反代 == 'socks5' && 启用SOCKS5全局反代) {
        remote = await socks5Connect(vlessHeader.hostname, vlessHeader.port);
    } else if (启用SOCKS5反代 == 'http' && 启用SOCKS5全局反代) {
        remote = await httpConnect(vlessHeader.hostname, vlessHeader.port);
    } else {
        try {
            remote = connect({ hostname: vlessHeader.hostname, port: vlessHeader.port });
            await remote.opened;
        } catch {
            if (启用SOCKS5反代 == 'socks5') {
                remote = await socks5Connect(vlessHeader.hostname, vlessHeader.port);
            } else if (启用SOCKS5反代 == 'http') {
                remote = await httpConnect(vlessHeader.hostname, vlessHeader.port);
            } else {
                const [反代IP地址, 反代IP端口] = await 解析地址端口(反代IP);
                remote = connect({ hostname: 反代IP地址, port: 反代IP端口 });
            }
        }
    }
    await remote.opened;

    if (!remote) return new Response('Connect failed', {
        status: 502
    });

    // 创建双向数据流
    const {
        readable,
        writable
    } = new TransformStream();

    // 上传流：请求 -> 远程
    const uploadPipe = async () => {
        const writer = remote.writable.getWriter();
        if (vlessHeader.data?.length) await writer.write(vlessHeader.data);

        const reader = vlessHeader.reader;
        try {
            while (true) {
                const {
                    value,
                    done
                } = await reader.read();
                if (done) break;
                if (value) await writer.write(value);
            }
        } catch { }
        writer.close();
    };

    // 下载流：远程 -> 响应
    const downloadPipe = new TransformStream({
        start(ctrl) {
            ctrl.enqueue(vlessHeader.resp);
        }
    });

    remote.readable.pipeTo(downloadPipe.writable).catch(() => { });
    uploadPipe().catch(() => { });

    return new Response(downloadPipe.readable, {
        headers: {
            'Content-Type': 'application/grpc',
            'X-Accel-Buffering': 'no',
            'Cache-Control': 'no-store'
        }
    });
}

// WebSocket 处理函数（保持原有逻辑）
async function handleWebSocket(request) {
    const [client, ws] = Object.values(new WebSocketPair());
    ws.accept();

    let remote = null,
        udpWriter = null,
        isDNS = false;

    new ReadableStream({
        start(ctrl) {
            ws.addEventListener('message', e => ctrl.enqueue(e.data));
            ws.addEventListener('close', () => {
                remote?.close();
                ctrl.close();
            });
            ws.addEventListener('error', () => {
                remote?.close();
                ctrl.error();
            });

            const early = request.headers.get('sec-websocket-protocol');
            if (early) {
                try {
                    ctrl.enqueue(Uint8Array.from(atob(early.replace(/-/g, '+').replace(/_/g, '/')),
                        c => c.charCodeAt(0)).buffer);
                } catch { }
            }
        }
    }).pipeTo(new WritableStream({
        async write(data) {
            if (isDNS) return udpWriter?.write(data);
            if (remote) {
                const w = remote.writable.getWriter();
                await w.write(data);
                w.releaseLock();
                return;
            }

            if (data.byteLength < 24) return;

            // UUID验证
            if (FIXED_UUID) {
                const uuidBytes = new Uint8Array(data.slice(1, 17));
                // @ts-ignore
                const expectedUUID = FIXED_UUID.replace(/-/g, '');
                for (let i = 0; i < 16; i++) {
                    if (uuidBytes[i] !== parseInt(expectedUUID.substr(i * 2, 2), 16)) return;
                }
            }

            const view = new DataView(data);
            const version = view.getUint8(0); // 提取版本号
            const optLen = view.getUint8(17);
            const cmd = view.getUint8(18 + optLen);
            if (cmd !== 1 && cmd !== 2) return;

            let pos = 19 + optLen;
            const port = view.getUint16(pos);
            const type = view.getUint8(pos + 2);
            pos += 3;

            let addr = '';
            if (type === 1) {
                addr =
                    `${view.getUint8(pos)}.${view.getUint8(pos + 1)}.${view.getUint8(pos + 2)}.${view.getUint8(pos + 3)}`;
                pos += 4;
            } else if (type === 2) {
                const len = view.getUint8(pos++);
                addr = new TextDecoder().decode(data.slice(pos, pos + len));
                pos += len;
            } else if (type === 3) {
                const ipv6 = [];
                for (let i = 0; i < 8; i++, pos += 2) ipv6.push(view.getUint16(pos).toString(16));
                addr = ipv6.join(':');
            } else return;
            if (addr.includes(atob('c3BlZWQuY2xvdWRmbGFyZS5jb20='))) throw new Error('Access');
            const header = new Uint8Array([version, 0]); // 使用提取的版本号
            const payload = data.slice(pos);

            // UDP DNS
            if (cmd === 2) {
                if (port !== 53) return;
                isDNS = true;
                let sent = false;
                const {
                    readable,
                    writable
                } = new TransformStream({
                    transform(chunk, ctrl) {
                        for (let i = 0; i < chunk.byteLength;) {
                            const len = new DataView(chunk.slice(i, i + 2)).getUint16(0);
                            ctrl.enqueue(chunk.slice(i + 2, i + 2 + len));
                            i += 2 + len;
                        }
                    }
                });

                readable.pipeTo(new WritableStream({
                    async write(query) {
                        try {
                            const resp = await fetch('https://1.1.1.1/dns-query', {
                                method: 'POST',
                                headers: {
                                    'content-type': 'application/dns-message'
                                },
                                body: query
                            });
                            if (ws.readyState === 1) {
                                const result = new Uint8Array(await resp
                                    .arrayBuffer());
                                ws.send(new Uint8Array([...(sent ? [] : header),
                                result.length >> 8, result.length &
                                0xff, ...result
                                ]));
                                sent = true;
                            }
                        } catch { }
                    }
                }));
                udpWriter = writable.getWriter();
                return udpWriter.write(payload);
            }

            // TCP连接
            let sock = null;
            if (启用SOCKS5反代 == 'socks5' && 启用SOCKS5全局反代) {
                sock = await socks5Connect(addr, port);
            } else if (启用SOCKS5反代 == 'http' && 启用SOCKS5全局反代) {
                sock = await httpConnect(addr, port);
            } else {
                try {
                    sock = connect({ hostname: addr, port: port });
                    await sock.opened;
                } catch {
                    if (启用SOCKS5反代 == 'socks5') {
                        sock = await socks5Connect(addr, port);
                    } else if (启用SOCKS5反代 == 'http') {
                        sock = await httpConnect(addr, port);
                    } else {
                        const [反代IP地址, 反代IP端口] = await 解析地址端口(反代IP);
                        sock = connect({ hostname: 反代IP地址, port: 反代IP端口 });
                    }
                }
            }
            await sock.opened;
            if (!sock) return;

            remote = sock;
            const w = sock.writable.getWriter();
            await w.write(payload);
            w.releaseLock();

            let sent = false;
            sock.readable.pipeTo(new WritableStream({
                write(chunk) {
                    if (ws.readyState === 1) {
                        ws.send(sent ? chunk : new Uint8Array([...header, ...
                            new Uint8Array(chunk)
                        ]));
                        sent = true;
                    }
                },
                close: () => ws.readyState === 1 && ws.close(),
                abort: () => ws.readyState === 1 && ws.close()
            })).catch(() => { });
        }
    })).catch(() => { });

    return new Response(null, {
        status: 101,
        webSocket: client
    });
}

// UUID 工具函数
function parseUUID(uuid) {
    return uuid.replaceAll('-', '').match(/.{2}/g).map(x => parseInt(x, 16));
}

// SOCKS5连接
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

// VLESS Header 解析
async function readVlessHeader(reader) {
    let buffer = new Uint8Array(1024);
    let offset = 0;

    while (true) {
        const {
            value,
            done
        } = await reader.read();
        if (done) {
            if (offset === 0) return null;
            break;
        }

        if (offset + value.length > buffer.length) {
            const newBuf = new Uint8Array(offset + value.length);
            newBuf.set(buffer.slice(0, offset));
            buffer = newBuf;
        }
        buffer.set(value, offset);
        offset += value.length;

        // 跳过版本和UUID(17字节)，读取addon长度
        if (offset < 18) continue;

        // UUID验证
        if (FIXED_UUID) {
            const uuidBytes = parseUUID(FIXED_UUID);
            if (!buffer.slice(1, 17).every((b, i) => b === uuidBytes[i])) {
                return null;
            }
        }

        const addonLen = buffer[17];
        if (offset < 18 + addonLen + 1) continue;
        const cmd = buffer[18 + addonLen];
        if (cmd !== 1) return null;

        const portIndex = 18 + addonLen + 1;
        const port = (buffer[portIndex] << 8) | buffer[portIndex + 1];
        const atypeIndex = portIndex + 2;
        const atype = buffer[atypeIndex];
        let hostIndex = atypeIndex + 1,
            hostLen = 0,
            hostname = '';

        switch (atype) {
            case 1: // IPv4
                hostLen = 4;
                if (offset < hostIndex + hostLen) continue;
                hostname = Array.from(buffer.slice(hostIndex, hostIndex + hostLen)).join('.');
                break;
            case 2: // 域名
                hostLen = buffer[hostIndex];
                if (offset < hostIndex + 1 + hostLen) continue;
                hostname = new TextDecoder().decode(buffer.slice(hostIndex + 1, hostIndex + 1 + hostLen));
                hostLen++;
                break;
            case 3: // IPv6
                hostLen = 16;
                if (offset < hostIndex + hostLen) continue;
                const dv = new DataView(buffer.buffer, hostIndex, 16);
                const parts = [];
                for (let i = 0; i < 8; i++) parts.push(dv.getUint16(i * 2).toString(16));
                hostname = parts.join(':');
                break;
            default:
                return null;
        }

        const headerLen = hostIndex + hostLen;
        const data = buffer.slice(headerLen, offset);
        const version = buffer[0]; // 提取版本号
        return {
            hostname,
            port,
            data,
            version,
            resp: new Uint8Array([version, 0]), // 使用实际版本号而不是 buffer[0]
            reader
        };
    }
    return null;
}

async function 获取SOCKS5账号(address) {
    // 使用 "@" 分割地址，分为认证部分和服务器地址部分
    const lastAtIndex = address.lastIndexOf("@");
    let [latter, former] = lastAtIndex === -1 ? [address, undefined] : [address.substring(lastAtIndex + 1), address.substring(0, lastAtIndex)];
    let username, password, hostname, port;

    // 如果存在 former 部分，说明提供了认证信息
    if (former) {
        const formers = former.split(":");
        if (formers.length !== 2) {
            throw new Error('无效的 SOCKS 地址格式：认证部分必须是 "username:password" 的形式');
        }
        [username, password] = formers;
    }

    // 解析服务器地址部分
    const latters = latter.split(":");
    // 检查是否是IPv6地址带端口格式 [xxx]:port
    if (latters.length > 2 && latter.includes("]:")) {
        // IPv6地址带端口格式：[2001:db8::1]:8080
        port = Number(latter.split("]:")[1].replace(/[^\d]/g, ''));
        hostname = latter.split("]:")[0] + "]"; // 正确提取hostname部分
    } else if (latters.length === 2) {
        // IPv4地址带端口或域名带端口
        port = Number(latters.pop().replace(/[^\d]/g, ''));
        hostname = latters.join(":");
    } else {
        port = 80;
        hostname = latter;
    }

    if (isNaN(port)) {
        throw new Error('无效的 SOCKS 地址格式：端口号必须是数字');
    }

    // 处理 IPv6 地址的特殊情况
    // IPv6 地址包含多个冒号，所以必须用方括号括起来，如 [2001:db8::1]
    const regex = /^\[.*\]$/;
    if (hostname.includes(":") && !regex.test(hostname)) {
        throw new Error('无效的 SOCKS 地址格式：IPv6 地址必须用方括号括起来，如 [2001:db8::1]');
    }

    //if (/^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?).){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.test(hostname)) hostname = `${atob('d3d3Lg==')}${hostname}${atob('LmlwLjA5MDIyNy54eXo=')}`;
    // 返回解析后的结果
    return {
        username,  // 用户名，如果没有则为 undefined
        password,  // 密码，如果没有则为 undefined
        hostname,  // 主机名，可以是域名、IPv4 或 IPv6 地址
        port,	 // 端口号，已转换为数字类型
    }
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
    const parsedSocks5Address = await 获取SOCKS5账号(我的SOCKS5账号);
    const { username, password, hostname, port } = parsedSocks5Address;
    const sock = await connect({
        hostname: hostname,
        port: port
    });

    // 构建HTTP CONNECT请求
    let connectRequest = `CONNECT ${addressRemote}:${portRemote} HTTP/1.1\r\n`;
    connectRequest += `Host: ${addressRemote}:${portRemote}\r\n`;

    // 添加代理认证（如果需要）
    if (username && password) {
        const authString = `${username}:${password}`;
        const base64Auth = btoa(authString);
        connectRequest += `Proxy-Authorization: Basic ${base64Auth}\r\n`;
    }

    connectRequest += `User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n`;
    connectRequest += `Proxy-Connection: Keep-Alive\r\n`;
    connectRequest += `Connection: Keep-Alive\r\n`; // 添加标准 Connection 头
    connectRequest += `\r\n`;

    try {
        // 发送连接请求
        const writer = sock.writable.getWriter();
        await writer.write(new TextEncoder().encode(connectRequest));
        writer.releaseLock();
    } catch (err) {
        console.error('发送HTTP CONNECT请求失败:', err);
        throw new Error(`发送HTTP CONNECT请求失败: ${err.message}`);
    }

    // 读取HTTP响应
    const reader = sock.readable.getReader();
    let respText = '';
    let connected = false;
    let responseBuffer = new Uint8Array(0);

    try {
        while (true) {
            const { value, done } = await reader.read();
            if (done) {
                console.error('HTTP代理连接中断');
                throw new Error('HTTP代理连接中断');
            }

            // 合并接收到的数据
            const newBuffer = new Uint8Array(responseBuffer.length + value.length);
            newBuffer.set(responseBuffer);
            newBuffer.set(value, responseBuffer.length);
            responseBuffer = newBuffer;

            // 将收到的数据转换为文本
            respText = new TextDecoder().decode(responseBuffer);

            // 检查是否收到完整的HTTP响应头
            if (respText.includes('\r\n\r\n')) {
                // 分离HTTP头和可能的数据部分
                const headersEndPos = respText.indexOf('\r\n\r\n') + 4;
                const headers = respText.substring(0, headersEndPos);

                // 检查响应状态
                if (headers.startsWith('HTTP/1.1 200') || headers.startsWith('HTTP/1.0 200')) {
                    connected = true;

                    // 如果响应头之后还有数据，我们需要保存这些数据以便后续处理
                    if (headersEndPos < responseBuffer.length) {
                        const remainingData = responseBuffer.slice(headersEndPos);
                        // 创建一个缓冲区来存储这些数据，以便稍后使用
                        const dataStream = new ReadableStream({
                            start(controller) {
                                controller.enqueue(remainingData);
                            }
                        });

                        // 创建一个新的TransformStream来处理额外数据
                        const { readable, writable } = new TransformStream();
                        dataStream.pipeTo(writable).catch(err => console.error('处理剩余数据错误:', err));

                        // 替换原始readable流
                        // @ts-ignore
                        sock.readable = readable;
                    }
                } else {
                    const errorMsg = `HTTP代理连接失败: ${headers.split('\r\n')[0]}`;
                    console.error(errorMsg);
                    throw new Error(errorMsg);
                }
                break;
            }
        }
    } catch (err) {
        reader.releaseLock();
        throw new Error(`处理HTTP代理响应失败: ${err.message}`);
    }

    reader.releaseLock();

    if (!connected) {
        throw new Error('HTTP代理连接失败: 未收到成功响应');
    }

    return sock;
}