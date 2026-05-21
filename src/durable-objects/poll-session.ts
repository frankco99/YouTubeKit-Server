import { DurableObject } from 'cloudflare:workers';
import { PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

/**
 * PollSessionObject - uses DO WebSocket hibernation to stay alive.
 *
 * The client (Watch) opens a WebSocket to /ws, then the DO drives
 * YouTubeService with a fake fetch that routes urlRequests over that
 * same WebSocket. This way the DO is never evicted mid-session.
 *
 * Watch client flow:
 *   1. Open WS to /v1/poll/ws?videoID=xxx
 *   2. Receive messages: { type: "urlRequest", content: ... }
 *   3. Send back:        { id, url, status_code, headers, data(base64) }
 *   4. Receive final:    { type: "result", content: [...streams] }
 */
export class PollSessionObject extends DurableObject {

  private messageListeners = new Map<string, Set<(event: any) => void>>();
  private pendingResponseResolvers = new Map<string, (msg: any) => void>();
  private clientSocket: WebSocket | null = null;

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/ws') {
      return this.handleWebSocket(request);
    }

    return new Response('Not found', { status: 404 });
  }

  // Upgrade to WebSocket — DO stays alive as long as WS is open
  private async handleWebSocket(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const [clientSock, serverSock] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(serverSock);
    this.clientSocket = serverSock;

    // Start YouTubeService — DO stays alive because WebSocket is open
    const fakeSocket = this.buildFakeWebSocket(serverSock);
    const service = new YouTubeService(videoId, fakeSocket);

    this.ctx.waitUntil(
      service.start().catch(err => {
        console.error('[service] error:', err);
        try {
          serverSock.send(JSON.stringify({ type: 'error', message: err.message ?? 'Unknown error' }));
          serverSock.close();
        } catch {}
      })
    );

    return new Response(null, { status: 101, webSocket: clientSock });
  }

  // Called by Cloudflare when DO receives a WS message (hibernation-safe)
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    if (typeof message !== 'string') return;
    let parsed: any;
    try { parsed = JSON.parse(message); } catch { return; }

    console.log(`[ws message] id=${parsed.id}`);
    const resolver = this.pendingResponseResolvers.get(parsed.id);
    if (resolver) {
      this.pendingResponseResolvers.delete(parsed.id);
      resolver(parsed);
    } else {
      console.warn(`[ws message] no resolver for id=${parsed.id}`);
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string) {
    console.log(`[ws close] code=${code} reason=${reason}`);
  }

  async webSocketError(ws: WebSocket, error: unknown) {
    console.error('[ws error]', error);
  }

  private buildFakeWebSocket(serverSock: WebSocket): WebSocket {
    const self = this;

    const fakeSocket = {
      addEventListener(type: string, listener: (event: any) => void) {
        if (!self.messageListeners.has(type)) self.messageListeners.set(type, new Set());
        self.messageListeners.get(type)!.add(listener);
      },

      send(data: string | ArrayBuffer) {
        if (typeof data !== 'string') return;
        let parsed: any;
        try { parsed = JSON.parse(data); } catch { return; }

        console.log(`[send] type=${parsed.type}`);

        if (parsed.type === 'urlRequest') {
          const req = parsed.content as PollRequest;

          // Register resolver before sending to client
          const responsePromise = new Promise<any>((resolve, reject) => {
            const timer = setTimeout(() => {
              self.pendingResponseResolvers.delete(req.id);
              reject(new Error('Client fetch timeout'));
            }, 55_000);

            self.pendingResponseResolvers.set(req.id, data => {
              clearTimeout(timer);
              resolve(data);
            });
          });

          // Send urlRequest to Watch client over WebSocket
          try {
            serverSock.send(JSON.stringify({ type: 'urlRequest', content: req }));
          } catch (e) {
            console.error('[send] failed to send urlRequest:', e);
          }

          // When Watch responds, fire it back to YouTubeService
          responsePromise.then(responseData => {
            self.messageListeners.get('message')?.forEach(l =>
              l({ data: JSON.stringify(responseData) })
            );
          }).catch(err => {
            console.error(`[send] timeout for id=${req.id}:`, err);
          });

        } else if (parsed.type === 'result' || parsed.type === 'error') {
          // Forward final result/error to Watch, then close
          try {
            serverSock.send(data);
            serverSock.close();
          } catch {}
        }
      },

      close(_code?: number, _reason?: string) { /* no-op */ },
    };

    return fakeSocket as unknown as WebSocket;
  }
}
