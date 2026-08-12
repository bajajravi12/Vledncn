// Dynamic VLESS Proxy Worker for Cloudflare
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    // 1. WebSocket Handling (Tunnel Connection)
    if (upgradeHeader === 'websocket') {
      return await handleVlessWebSocket(request);
    }

    // 2. Browser Request (Subscription / Info)
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

  // Handle path based dynamic routing (e.g. /aioproxybot/IP-PORT)
  const url = new URL(request.url);
  const pathParts = url.pathname.split('/').filter(Boolean);
  
  let targetAddress = '1.1.1.1';
  let targetPort = 443;

  if (pathParts.length >= 2) {
    const rawTarget = pathParts[1]; // Get '98.70.26.236-443'
    if (rawTarget.includes('-')) {
      const [ip, port] = rawTarget.split('-');
      targetAddress = ip;
      targetPort = parseInt(port) || 443;
    }
  }

  // Connect to target server using TCP Sockets
  try {
    const tcpSocket = connect({
      hostname: targetAddress,
      port: targetPort,
    });

    // Pipe traffic between Client and Target Server
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
