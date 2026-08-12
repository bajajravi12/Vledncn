import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get('Upgrade') === 'websocket') {
      return await handleVlessWebSocket(request);
    }
    return new Response('VLESS Worker Active!', { status: 200 });
  }
};

async function handleVlessWebSocket(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  const url = new URL(request.url);
  const pathParts = url.pathname.split('/').filter(Boolean);

  let targetAddress = '1.1.1.1';
  let targetPort = 443;

  if (pathParts.length >= 2 && pathParts[1].includes('-')) {
    const [ip, port] = pathParts[1].split('-');
    targetAddress = ip;
    targetPort = parseInt(port) || 443;
  }

  try {
    const tcpSocket = connect({ hostname: targetAddress, port: targetPort });
    const writer = tcpSocket.writable.getWriter();
    const reader = tcpSocket.readable.getReader();

    let isFirstChunk = true;

    server.addEventListener('message', async (event) => {
      try {
        let data = new Uint8Array(event.data);

        if (isFirstChunk) {
          isFirstChunk = false;
          // Client ko VLESS handshake response ([0, 0]) bhejo
          server.send(new Uint8Array([0, 0]));

          // First frame me se VLESS header strip karke baki data proxy karo
          if (data.length > 24) {
            const optLen = data[17];
            let headerLen = 19 + optLen;
            const addrType = data[headerLen];

            if (addrType === 1) headerLen += 7;      // IPv4
            else if (addrType === 2) headerLen += 2 + data[headerLen + 1]; // Domain
            else if (addrType === 3) headerLen += 19; // IPv6

            if (data.length > headerLen) {
              data = data.slice(headerLen);
              await writer.write(data);
            }
          }
        } else {
          await writer.write(data);
        }
      } catch (e) {}
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

  return new Response(null, { status: 101, webSocket: client });
}
