// src/index.js
// VLESS over WebSocket (WSS) implementation for Cloudflare Workers.
// Runtime: Cloudflare Workers (no Node.js APIs used).
// Outbound TCP connections are made with the `cloudflare:sockets` API.

// NOTE: cloudflare:sockets is imported dynamically (inside handleVlessSession)
// instead of at the top of the file. A static top-level import crashes the
// entire Worker on module load if TCP Sockets are unavailable in this
// account/environment, which takes down every route including /health and
// /. A dynamic import means only an actual VLESS connection attempt fails,
// while normal HTTP routes keep working.

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

export default {
  /**
   * @param {Request} request
   * @param {{UUID: string, WS_PATH: string}} env
   */
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const wsPath = normalizePath(env.WS_PATH || '/vless');
      const upgradeHeader = request.headers.get('Upgrade');
      const isWebSocketRequest =
        upgradeHeader && upgradeHeader.toLowerCase() === 'websocket';

      if (url.pathname === '/health') {
        return new Response(JSON.stringify({ status: 'ok' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }

      if (url.pathname === wsPath) {
        if (!isWebSocketRequest) {
          return new Response('Expected WebSocket Upgrade', { status: 426 });
        }
        return await handleWebSocketUpgrade(request, env);
      }

      if (isWebSocketRequest) {
        return new Response('Not Found', { status: 404 });
      }

      return new Response('VLESS Worker is running', {
        status: 200,
        headers: { 'content-type': 'text/plain' },
      });
    } catch (err) {
      return new Response('Internal Error', { status: 500 });
    }
  },
};

function normalizePath(path) {
  if (!path.startsWith('/')) return '/' + path;
  return path;
}

async function handleWebSocketUpgrade(request, env) {
  const uuidStr = (env.UUID || '').trim().toLowerCase();
  if (!isValidUUID(uuidStr)) {
    return new Response('Server not configured', { status: 500 });
  }

  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);

  server.accept();

  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

  handleVlessSession(server, uuidStr, earlyDataHeader).catch(() => {
    safeCloseWebSocket(server);
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader) {
  let cancelled = false;

  return new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (cancelled) return;
        controller.enqueue(event.data);
      });

      webSocketServer.addEventListener('close', () => {
        if (cancelled) return;
        try {
          controller.close();
        } catch (e) {}
      });

      webSocketServer.addEventListener('error', (err) => {
        controller.error(err);
      });

      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel() {
      if (cancelled) return;
      cancelled = true;
      safeCloseWebSocket(webSocketServer);
    },
  });
}

async function handleVlessSession(webSocketServer, uuidStr, earlyDataHeader) {
  let remoteSocket = null;
  let remoteWriter = null;
  let headerParsed = false;
  let timeoutId = null;

  const clearConnTimeout = () => {
    if (timeoutId) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }
  };

  timeoutId = setTimeout(() => {
    if (!headerParsed) {
      safeCloseWebSocket(webSocketServer);
    }
  }, 10_000);

  const readableWebSocketStream = makeReadableWebSocketStream(
    webSocketServer,
    earlyDataHeader
  );

  try {
    await readableWebSocketStream.pipeTo(
      new WritableStream({
        async write(chunk, controller) {
          try {
            if (headerParsed) {
              if (remoteWriter) {
                const data = toUint8Array(chunk);
                await remoteWriter.write(data);
              }
              return;
            }

            const buffer = toArrayBuffer(chunk);
            const result = parseVlessHeader(buffer, uuidStr);

            if (result.hasError) {
              controller.error(result.message);
              return;
            }

            headerParsed = true;
            clearConnTimeout();

            const vlessResponseHeader = new Uint8Array([
              result.vlessVersion,
              0,
            ]);
            const rawClientData = buffer.slice(result.rawDataIndex);

            let tcpSocket;
            try {
              const { connect } = await import('cloudflare:sockets');
              tcpSocket = connect({
                hostname: result.addressRemote,
                port: result.portRemote,
              });
            } catch (connErr) {
              controller.error('Unable to reach destination');
              safeCloseWebSocket(webSocketServer);
              return;
            }

            remoteSocket = tcpSocket;
            remoteWriter = tcpSocket.writable.getWriter();

            if (rawClientData.byteLength > 0) {
              await remoteWriter.write(new Uint8Array(rawClientData));
            }

            pipeRemoteToWebSocket(
              tcpSocket,
              webSocketServer,
              vlessResponseHeader
            ).catch(() => {
              safeCloseWebSocket(webSocketServer);
            });
          } catch (innerErr) {
            controller.error('Session error');
          }
        },
        close() {
          closeRemote(remoteSocket);
        },
        abort() {
          closeRemote(remoteSocket);
        },
      })
    );
  } catch (err) {
    safeCloseWebSocket(webSocketServer);
    closeRemote(remoteSocket);
  } finally {
    clearConnTimeout();
  }
}

async function pipeRemoteToWebSocket(tcpSocket, webSocketServer, vlessResponseHeader) {
  let headerSent = false;
  const reader = tcpSocket.readable.getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      if (webSocketServer.readyState !== WS_READY_STATE_OPEN) {
        break;
      }

      if (!headerSent) {
        const combined = new Uint8Array(
          vlessResponseHeader.byteLength + value.byteLength
        );
        combined.set(vlessResponseHeader, 0);
        combined.set(value, vlessResponseHeader.byteLength);
        webSocketServer.send(combined);
        headerSent = true;
      } else {
        webSocketServer.send(value);
      }
    }
  } catch (err) {
  } finally {
    try {
      reader.releaseLock();
    } catch (e) {}
    safeCloseWebSocket(webSocketServer);
  }
}

function closeRemote(tcpSocket) {
  if (!tcpSocket) return;
  try {
    tcpSocket.close();
  } catch (e) {}
}

function parseVlessHeader(buffer, expectedUuid) {
  if (!buffer || buffer.byteLength < 24) {
    return { hasError: true, message: 'Malformed VLESS header: too short' };
  }

  const view = new DataView(buffer);
  let offset = 0;

  const version = view.getUint8(offset);
  offset += 1;

  const uuidBytes = new Uint8Array(buffer.slice(offset, offset + 16));
  offset += 16;

  const uuid = bytesToUuid(uuidBytes);
  if (uuid !== expectedUuid) {
    return { hasError: true, message: 'Invalid UUID' };
  }

  const optLength = view.getUint8(offset);
  offset += 1 + optLength;

  if (offset + 1 > buffer.byteLength) {
    return { hasError: true, message: 'Malformed VLESS header: truncated' };
  }

  const command = view.getUint8(offset);
  offset += 1;

  if (command !== 1) {
    return {
      hasError: true,
      message: 'Unsupported command (only TCP is supported)',
    };
  }

  if (offset + 2 > buffer.byteLength) {
    return { hasError: true, message: 'Malformed VLESS header: truncated port' };
  }
  const port = view.getUint16(offset);
  offset += 2;

  if (offset + 1 > buffer.byteLength) {
    return { hasError: true, message: 'Malformed VLESS header: truncated address' };
  }
  const addressType = view.getUint8(offset);
  offset += 1;

  let addressValue = '';
  let addressLength = 0;

  if (addressType === 1) {
    addressLength = 4;
    if (offset + addressLength > buffer.byteLength) {
      return { hasError: true, message: 'Malformed VLESS header: truncated IPv4' };
    }
    const ipBytes = new Uint8Array(buffer.slice(offset, offset + addressLength));
    addressValue = ipBytes.join('.');
  } else if (addressType === 2) {
    if (offset + 1 > buffer.byteLength) {
      return { hasError: true, message: 'Malformed VLESS header: truncated domain length' };
    }
    addressLength = view.getUint8(offset);
    offset += 1;
    if (offset + addressLength > buffer.byteLength) {
      return { hasError: true, message: 'Malformed VLESS header: truncated domain' };
    }
    addressValue = new TextDecoder().decode(
      buffer.slice(offset, offset + addressLength)
    );
  } else if (addressType === 3) {
    addressLength = 16;
    if (offset + addressLength > buffer.byteLength) {
      return { hasError: true, message: 'Malformed VLESS header: truncated IPv6' };
    }
    const ipView = new DataView(buffer.slice(offset, offset + addressLength));
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(ipView.getUint16(i * 2).toString(16));
    }
    addressValue = parts.join(':');
  } else {
    return { hasError: true, message: 'Unsupported address type' };
  }

  offset += addressLength;

  if (offset > buffer.byteLength) {
    return { hasError: true, message: 'Malformed VLESS header: overflow' };
  }

  return {
    hasError: false,
    vlessVersion: version,
    addressRemote: addressValue,
    portRemote: port,
    rawDataIndex: offset,
  };
}

function isValidUUID(uuid) {
  const regex =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  return typeof uuid === 'string' && regex.test(uuid);
}

function bytesToUuid(bytes) {
  const hex = Array.from(bytes).map((b) => b.toString(16).padStart(2, '0'));
  return (
    hex.slice(0, 4).join('') +
    '-' +
    hex.slice(4, 6).join('') +
    '-' +
    hex.slice(6, 8).join('') +
    '-' +
    hex.slice(8, 10).join('') +
    '-' +
    hex.slice(10, 16).join('')
  ).toLowerCase();
}

function toArrayBuffer(chunk) {
  if (chunk instanceof ArrayBuffer) return chunk;
  if (ArrayBuffer.isView(chunk)) {
    return chunk.buffer.slice(
      chunk.byteOffset,
      chunk.byteOffset + chunk.byteLength
    );
  }
  return new ArrayBuffer(0);
}

function toUint8Array(chunk) {
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  if (ArrayBuffer.isView(chunk)) return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  return new Uint8Array(0);
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) {
    return { earlyData: null, error: null };
  }
  try {
    const normalized = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decoded = atob(normalized);
    const bytes = new Uint8Array(decoded.length);
    for (let i = 0; i < decoded.length; i++) {
      bytes[i] = decoded.charCodeAt(i);
    }
    return { earlyData: bytes.buffer, error: null };
  } catch (error) {
    return { earlyData: null, error: null };
  }
}

function safeCloseWebSocket(ws) {
  try {
    if (
      ws.readyState === WS_READY_STATE_OPEN ||
      ws.readyState === WS_READY_STATE_CLOSING
    ) {
      ws.close();
    }
  } catch (e) {}
}
