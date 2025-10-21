const FIXED_UUID = '';// trojan版本
import { connect } from "cloudflare:sockets";
let 反代IP = '';
let 启用SOCKS5反代 = null;
let 启用SOCKS5全局反代 = false;
let 我的SOCKS5账号 = '';
export default {
    async fetch(request) {
        const url = new URL(request.url);
        反代IP = 反代IP ? 反代IP : request.cf.colo + '.pRoXyIp.CmLiUsSsS.NeT';
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

        if (request.headers.get('Upgrade') === 'websocket') {
            return await 处理WebSocket代理连接(request);
        } else {
            return new Response('Hello World!', { status: 200 });
        }
    }
};

async function 处理WebSocket代理连接(request) {
    const webSocketPair = new WebSocketPair();
    const [client, webSocket] = Object.values(webSocketPair);
    webSocket.accept();
    let address = "";
    const earlyDataHeader = request.headers.get("sec-websocket-protocol") || "";
    const WS可读流 = 创建WS可读流(webSocket, earlyDataHeader);
    let remoteSocketWapper = {
        value: null
    };
    let udpStreamWrite = null;
    WS可读流.pipeTo(new WritableStream({
        async write(chunk, controller) {
            if (udpStreamWrite) {
                return udpStreamWrite(chunk);
            }
            if (remoteSocketWapper.value) {
                const writer = remoteSocketWapper.value.writable.getWriter();
                await writer.write(chunk);
                writer.releaseLock();
                return;
            }
            const {
                hasError,
                message,
                portRemote = 443,
                addressRemote = "",
                rawClientData
            } = await parseTrojanHeader(chunk);
            address = addressRemote;
            if (hasError) {
                throw new Error(message);
                return;
            }
            if (addressRemote.includes(atob('c3BlZWQuY2xvdWRmbGFyZS5jb20='))) throw new Error('Access');
            handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket);
        },
        close() {
        },
        abort(reason) {
        }
    })).catch((err) => {
    });
    return new Response(null, {
        status: 101,
        // @ts-ignore
        webSocket: client
    });
}

async function parseTrojanHeader(buffer) {
    if (buffer.byteLength < 56) {
        return {
            hasError: true,
            message: "invalid data"
        };
    }
    let crLfIndex = 56;
    if (new Uint8Array(buffer.slice(56, 57))[0] !== 0x0d || new Uint8Array(buffer.slice(57, 58))[0] !== 0x0a) {
        return {
            hasError: true,
            message: "invalid header format (missing CR LF)"
        };
    }
    const password = new TextDecoder().decode(buffer.slice(0, crLfIndex));
    if (FIXED_UUID && password !== FIXED_UUID) {
        return {
            hasError: true,
            message: "invalid password"
        };
    }

    const socks5DataBuffer = buffer.slice(crLfIndex + 2);
    if (socks5DataBuffer.byteLength < 6) {
        return {
            hasError: true,
            message: "invalid SOCKS5 request data"
        };
    }

    const view = new DataView(socks5DataBuffer);
    const cmd = view.getUint8(0);
    if (cmd !== 1) {
        return {
            hasError: true,
            message: "unsupported command, only TCP (CONNECT) is allowed"
        };
    }

    const atype = view.getUint8(1);
    // 0x01: IPv4 address
    // 0x03: Domain name
    // 0x04: IPv6 address
    let addressLength = 0;
    let addressIndex = 2;
    let address = "";
    switch (atype) {
        case 1:
            addressLength = 4;
            address = new Uint8Array(
                socks5DataBuffer.slice(addressIndex, addressIndex + addressLength)
            ).join(".");
            break;
        case 3:
            addressLength = new Uint8Array(
                socks5DataBuffer.slice(addressIndex, addressIndex + 1)
            )[0];
            addressIndex += 1;
            address = new TextDecoder().decode(
                socks5DataBuffer.slice(addressIndex, addressIndex + addressLength)
            );
            break;
        case 4:
            addressLength = 16;
            const dataView = new DataView(socks5DataBuffer.slice(addressIndex, addressIndex + addressLength));
            const ipv6 = [];
            for (let i = 0; i < 8; i++) {
                ipv6.push(dataView.getUint16(i * 2).toString(16));
            }
            address = ipv6.join(":");
            break;
        default:
            return {
                hasError: true,
                message: `invalid addressType is ${atype}`
            };
    }

    if (!address) {
        return {
            hasError: true,
            message: `address is empty, addressType is ${atype}`
        };
    }

    const portIndex = addressIndex + addressLength;
    const portBuffer = socks5DataBuffer.slice(portIndex, portIndex + 2);
    const portRemote = new DataView(portBuffer).getUint16(0);
    return {
        hasError: false,
        addressRemote: address,
        portRemote,
        rawClientData: socks5DataBuffer.slice(portIndex + 4)
    };
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket) {
    async function connectAndWrite(address, port) {
        let tcpSocket2;
        if (启用SOCKS5反代 == 'socks5' && 启用SOCKS5全局反代) {
            tcpSocket2 = await socks5Connect(address, port);
        } else if (启用SOCKS5反代 == 'http' && 启用SOCKS5全局反代) {
            tcpSocket2 = await httpConnect(address, port);
        } else {
            try {
                tcpSocket2 = connect({ hostname: address, port });
                await tcpSocket2.opened;
            } catch {
                if (启用SOCKS5反代 == 'socks5') {
                    tcpSocket2 = await socks5Connect(address, port);
                } else if (启用SOCKS5反代 == 'http') {
                    tcpSocket2 = await httpConnect(address, port);
                } else {
                    const [反代IP地址, 反代IP端口] = await 解析地址端口(反代IP);
                    tcpSocket2 = connect({ hostname: 反代IP地址, port: 反代IP端口 });
                }
            }
        }

        remoteSocket.value = tcpSocket2;
        const writer = tcpSocket2.writable.getWriter();
        await writer.write(rawClientData);
        writer.releaseLock();
        return tcpSocket2;
    }
    async function retry() {
        const [反代IP地址, 反代IP端口] = await 解析地址端口(反代IP);
        const tcpSocket2 = await connectAndWrite(反代IP地址, 反代IP端口);
        tcpSocket2.closed.catch((error) => {
        }).finally(() => {
            safeCloseWebSocket(webSocket);
        });
        转发远程数据到WS(tcpSocket2, webSocket, null);
    }
    const tcpSocket = await connectAndWrite(addressRemote, portRemote);
    转发远程数据到WS(tcpSocket, webSocket, retry);
}

function 创建WS可读流(webSocketServer, earlyDataHeader) {
    let readableStreamCancel = false;
    const stream = new ReadableStream({
        start(controller) {
            webSocketServer.addEventListener("message", (event) => {
                if (readableStreamCancel) {
                    return;
                }
                const message = event.data;
                controller.enqueue(message);
            });
            webSocketServer.addEventListener("close", () => {
                safeCloseWebSocket(webSocketServer);
                if (readableStreamCancel) {
                    return;
                }
                controller.close();
            });
            webSocketServer.addEventListener("error", (err) => {
                controller.error(err);
            });
            const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
            if (error) {
                controller.error(error);
            } else if (earlyData) {
                controller.enqueue(earlyData);
            }
        },
        pull(controller) { },
        cancel(reason) {
            if (readableStreamCancel) {
                return;
            }
            readableStreamCancel = true;
            safeCloseWebSocket(webSocketServer);
        }
    });
    return stream;
}

async function 转发远程数据到WS(remoteSocket, webSocket, retry) {
    let hasIncomingData = false;
    await remoteSocket.readable.pipeTo(
        new WritableStream({
            start() { },
            /**
             *
             * @param {Uint8Array} chunk
             * @param {*} controller
             */
            async write(chunk, controller) {
                hasIncomingData = true;
                if (webSocket.readyState !== WS_READY_STATE_OPEN) {
                    controller.error(
                        "webSocket connection is not open"
                    );
                }
                webSocket.send(chunk);
            },
            close() {
            },
            abort(reason) {
            }
        })
    ).catch((error) => {
        safeCloseWebSocket(webSocket);
    });
    if (hasIncomingData === false && retry) {
        retry();
    }
}

function base64ToArrayBuffer(base64Str) {
    if (!base64Str) {
        return { error: null };
    }
    try {
        base64Str = base64Str.replace(/-/g, "+").replace(/_/g, "/");
        const decode = atob(base64Str);
        const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
        return { earlyData: arryBuffer.buffer, error: null };
    } catch (error) {
        return { error };
    }
}

let WS_READY_STATE_OPEN = 1;
let WS_READY_STATE_CLOSING = 2;

function safeCloseWebSocket(socket) {
    try {
        if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
            socket.close();
        }
    } catch (error) {
    }
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
    const { username, password, hostname, port } = await 获取SOCKS5账号(我的SOCKS5账号);
    const sock = await connect({ hostname, port });

    // 构建HTTP CONNECT请求
    const authHeader = username && password ? `Proxy-Authorization: Basic ${btoa(`${username}:${password}`)}\r\n` : '';
    const connectRequest = `CONNECT ${addressRemote}:${portRemote} HTTP/1.1\r\n` +
        `Host: ${addressRemote}:${portRemote}\r\n` +
        authHeader +
        `User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n` +
        `Proxy-Connection: Keep-Alive\r\n` +
        `Connection: Keep-Alive\r\n\r\n`;

    // 发送连接请求
    const writer = sock.writable.getWriter();
    try {
        await writer.write(new TextEncoder().encode(connectRequest));
    } catch (err) {
        throw new Error(`发送HTTP CONNECT请求失败: ${err.message}`);
    } finally {
        writer.releaseLock();
    }

    // 读取并处理HTTP响应
    const reader = sock.readable.getReader();
    let responseBuffer = new Uint8Array(0);

    try {
        while (true) {
            const { value, done } = await reader.read();
            if (done) throw new Error('HTTP代理连接中断');

            // 合并响应数据
            const newBuffer = new Uint8Array(responseBuffer.length + value.length);
            newBuffer.set(responseBuffer);
            newBuffer.set(value, responseBuffer.length);
            responseBuffer = newBuffer;

            const respText = new TextDecoder().decode(responseBuffer);
            
            // 检查是否收到完整的HTTP响应头
            if (respText.includes('\r\n\r\n')) {
                const headersEndPos = respText.indexOf('\r\n\r\n') + 4;
                const headers = respText.substring(0, headersEndPos);

                if (!headers.startsWith('HTTP/1.1 200') && !headers.startsWith('HTTP/1.0 200')) {
                    throw new Error(`HTTP代理连接失败: ${headers.split('\r\n')[0]}`);
                }

                // 处理响应头后的剩余数据
                if (headersEndPos < responseBuffer.length) {
                    const remainingData = responseBuffer.slice(headersEndPos);
                    const { readable, writable } = new TransformStream();
                    new ReadableStream({
                        start(controller) {
                            controller.enqueue(remainingData);
                        }
                    }).pipeTo(writable).catch(() => {});
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