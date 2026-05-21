import { DurableObject } from 'cloudflare:workers';
import { PollSession, PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

export class PollSessionObject extends DurableObject {

  // In-memory state — lives for the duration of the DO instance
  // (DO instances are kept alive as long as there's activity)
  private phase: 'waiting_for_request' | 'pending_request' | 'waiting_for_response' | 'done' | 'error' = 'waiting_for_request';
  private pendingRequest: PollRequest | null = null;
  private result: any[] | null = null;
  private errorMessage: string | null = null;
  private pendingResponseResolvers = new Map<string, (msg: any) => void>();
  private messageListeners = new Map<string, Set<(event: any) => void>>();

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/start') return this.handleStart(request);
    if (url.pathname === '/next')  return this.handleNext();
    if (url.pathname === '/respond') return this.handleRespond(request);
    return new Response('Not found', { status: 404 });
  }

  private async handleStart(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    this.phase = 'waiting_for_request';

    const fakeSocket = this.buildFakeWebSocket();
    // Watch fetches YouTube directly (preserves Vietnamese IP for geo-restricted content)
    const service = new YouTubeService(videoId, fakeSocket);
    this.ctx.waitUntil(service.start());

    return new Response(null, { status: 204 });
  }

  private async handleNext(): Promise<Response> {
    const deadline = Date.now() + 50_000;

    while (Date.now() < deadline) {
      if (this.phase === 'pending_request' && this.pendingRequest) {
        const request = this.pendingRequest;
        this.pendingRequest = null;
        this.phase = 'waiting_for_response';
        return Response.json({ type: 'urlRequest', content: request });
      }

      if (this.phase === 'done') {
        return Response.json({ type: 'result', content: this.result });
      }

      if (this.phase === 'error') {
        return new Response(
          JSON.stringify({ type: 'error', message: this.errorMessage }),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        );
      }

      await sleep(200);
    }

    return new Response(null, { status: 204 });
  }

  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;

    if (this.phase !== 'waiting_for_response') {
      return new Response('Unexpected respond', { status: 409 });
    }

    this.phase = 'waiting_for_request';
    this.pendingResponseResolvers.get(body.id)?.(body);
    this.pendingResponseResolvers.delete(body.id);

    return new Response(null, { status: 204 });
  }

  private buildFakeWebSocket(): WebSocket {
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

        if (parsed.type === 'urlRequest') {
          const requestId: string = parsed.content.id;

          // Surface to /next
          self.pendingRequest = parsed.content as PollRequest;
          self.phase = 'pending_request';

          // When /respond delivers the response, fire it back to YouTubeService
          const responsePromise = new Promise<any>(resolve => {
            self.pendingResponseResolvers.set(requestId, resolve);
          });

          responsePromise.then(responseData => {
            self.messageListeners.get('message')?.forEach(l =>
              l({ data: JSON.stringify(responseData) })
            );
          });

        } else if (parsed.type === 'result') {
          self.result = parsed.content;
          self.phase = 'done';

        } else if (parsed.type === 'error') {
          self.errorMessage = parsed.message ?? 'Unknown error';
          self.phase = 'error';
        }
      },

      close(_code?: number, _reason?: string) { /* no-op */ },
    };

    return fakeSocket as unknown as WebSocket;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
