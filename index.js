import { connect } from 'cloudflare:sockets';

const defaultUUID = 'a40a972f-b220-4d65-9983-a563f44f9c25';

export default {
  async fetch(request, env, ctx) {
    try {
      const uuid = env.UUID || defaultUUID;
      if (request.headers.get('Upgrade') === 'websocket') {
        return await handleVlessWS(request, uuid);
      }
      return new Response('VLESS Worker Active!', { status: 200 });
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  }
};

async function handleVlessWS(request, uuid) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  let writer = null;
  let isParsed = false;

  server.addEventListener('message', async (event) => {
    try {
      const chunk = new Uint8Array(event.data);

      if (!isParsed) {
        if (chunk.length < 24) return;

        const version = chunk[0];
        server.send(new Uint8Array([version, 0]));

        const optLength = chunk[17];
        let cursor = 18 + optLength;
        const command = chunk[cursor++];

        const port = (chunk[cursor] << 8) | chunk[cursor + 1];
        cursor += 2;

        const addressType = chunk[cursor++];
        let address = '';

        if (addressType === 1) {
          address = chunk.slice(cursor, cursor + 4).join('.');
          cursor += 4;
        } else if (addressType === 2) {
          const domainLen = chunk[cursor++];
          address = new TextDecoder().decode(chunk.slice(cursor, cursor + domainLen));
          cursor += domainLen;
        } else if (addressType === 3) {
          address = Array.from(chunk.slice(cursor, cursor + 16))
            .reduce((s, b, i) => s + (i % 2 === 0 && i > 0 ? ':' : '') + b.toString(16).padStart(2, '0'), '');
          cursor += 16;
        }

        const tcpSocket = connect({ hostname: address || '1.1.1.1', port: port || 443 });
        writer = tcpSocket.writable.getWriter();
        const reader = tcpSocket.readable.getReader();

        isParsed = true;

        const payload = chunk.slice(cursor);
        if (payload.length > 0) {
          await writer.write(payload);
        }

        (async () => {
          try {
            while (true) {
              const { value, done } = await reader.read();
              if (done) break;
              server.send(value);
            }
          } catch (e) {}
        })();

      } else if (writer) {
        await writer.write(chunk);
      }
    } catch (err) {
      server.close();
    }
  });

  return new Response(null, { status: 101, webSocket: client });
}
