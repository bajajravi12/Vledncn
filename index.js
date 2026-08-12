import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get('Upgrade');

    // 1. WebSocket Connection
    if (upgradeHeader === 'websocket') {
      return await handleVlessWebSocket(request);
    }

    // 2. Browser Request Response
    return new Response('VLESS Worker is Running Successfully!', {
      status: 200,
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }
};

async function handleVlessWebSocket(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);

  server.accept();

  // Path se target IP/Port read karna (e.g. /vless/98.70.26.236-443)
  const url = new URL(request.url);
  const pathParts = url.pathname.split('/').filter(Boolean);
  
  let targetAddress = '1.1.1.1';
  let targetPort = 443;

  if (pathParts.length >= 2) {
    const rawTarget = pathParts[1];
    if (rawTarget.includes('-')) {
      const [ip, port] = rawTarget.split('-');
      targetAddress = ip;
      targetPort = parseInt(port) || 443;
    }
  }

  // Cloudflare TCP Socket Connection
  try {
    const tcpSocket = connect({
      hostname: targetAddress,
      port: targetPort,
    });

    const writer = tcpSocket.writable.getWriter();
    const reader = tcpSocket.readable.getReader();

    server.addEventListener('message', async (event) => {
      await writer.write(event.data);
    });

    (async () => {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        server.send(value);
      }
    })();

  } catch (err) {
    server.close();
  }

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}
