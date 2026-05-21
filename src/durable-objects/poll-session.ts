import { DurableObject } from 'cloudflare:workers';
import { PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

/**
 * PollSessionObject - HTTP polling for watchOS
 *
 * Architecture: instead of a fake WebSocket + waitUntil (which causes DO
 * hibernation issues), we run YouTubeService directly inside the /next
 * long-poll request. The DO stays alive because there's an active HTTP
 * request open. All state is in-memory (same instance guaranteed).
 *
 * Flow:
 *   POST /start?videoID=xxx  → starts YouTubeService, returns immediately
 *   GET  /next               → blocks until result or next urlRequest ready
 *   POST /respond            → delivers proxy response back to service
 *   (repeat GET/POST until result)
 */
export class PollSessionObject extends DurableObject {

  private servicePromise: Promise<void> | null = null;
  private messageListeners = new Map<string, Set<(event: any) => void>>();
  private pendingForClient: { type: 'urlRequest'; request: PollRequest } | { type: 'result'; streams: any[] } | { type: 'error'; message: string } | null = null;
  private pendingResponseResolvers = new Map<string, (msg: any) => void>();
  private pendingForClientResolve: ((val: any) => void) | null = null;

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/start')   return this.handleStart(request);
    if (url.pathname === '/next')    return this.handleNext();
    if (url.pathname === '/respond') return this.handleRespond(request);
    return new Response('Not found', { status: 404 });
  }

  // POST /start — build fake socket, kick off service, return immediately
  private async handleStart(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    this.pendingForClient = null;
    this.pendingForClientResolve = null;
    this.pendingResponseResolvers.clear();
    this.messageListeners.clear();

    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);

    // Start service — DO stays alive because /next will have an open request
    this.servicePromise = service.start().catch(err => {
      console.error('[service] error:', err);
      this.notifyClient({ type: 'error', message: err.message ?? 'Unknown error' });
    });

    return new Response(null, { status: 204 });
  }

  // GET /next — wait until service has something for client (urlRequest or result)
  private async handleNext(): Promise<Response> {
    // If already have something queued, return it immediately
    if (this.pendingForClient) {
      return this.buildClientResponse(this.pendingForClient);
    }

    // Otherwise wait up to 50s for service to produce something
    const message = await new Promise<any>((resolve, reject) => {
      this.pendingForClientResolve = resolve;
      setTimeout(() => {
        this.pendingForClientResolve = null;
        reject(new Error('poll timeout'));
      }, 50_000);
    }).catch(() => null);

    if (!message) {
      return new Response(null, { status: 204 }); // timeout, client retries
    }

    return this.buildClientResponse(message);
  }

  // POST /respond — deliver proxy response back to YouTubeService
  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    console.log(`[respond] id=${body.id}`);

    const resolver = this.pendingResponseResolvers.get(body.id);
    if (resolver) {
      this.pendingResponseResolvers.delete(body.id);
      resolver(body);
    } else {
      console.warn(`[respond] no resolver for id=${body.id}`);
    }

    return new Response(null, { status: 204 });
  }

  // Called by fake socket when service has something for client
  private notifyClient(message: any) {
    if (this.pendingForClientResolve) {
      const resolve = this.pendingForClientResolve;
      this.pendingForClientResolve = null;
      this.pendingForClient = null;
      resolve(message);
    } else {
      // /next not currently waiting — queue it
      this.pendingForClient = message;
    }
  }

  private buildClientResponse(message: any): Response {
    if (message.type === 'urlRequest') {
      console.log(`[next] returning urlRequest id=${message.request.id}`);
      return Response.json({ type: 'urlRequest', content: message.request });
    }
    if (message.type === 'result') {
      console.log(`[next] returning result streams=${message.streams?.length ?? 0}`);
      return Response.json({ type: 'result', content: message.streams });
    }
    // error
    console.log(`[next] returning error: ${message.message}`);
    return new Response(
      JSON.stringify({ type: 'error', message: message.message }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }

  // Build a fake WebSocket that routes through this DO's in-memory state
  private buildFakeWebSocket(): WebSocket {
    const self = this;

    const fakeSocket = {
      addEventListener(type: string, listener: (event: any) => void) {
        if (!self.messageListeners.has(type)) self.messageListeners.set(type, new Set());
        self.messageListeners.get(type)!.add(listener);
      },

      // YouTubeService calls this synchronously — we must handle async internally
      send(data: string | ArrayBuffer) {
        if (typeof data !== 'string') return;
        let parsed: any;
        try { parsed = JSON.parse(data); } catch { return; }

        console.log(`[send] type=${parsed.type}`);

        if (parsed.type === 'urlRequest') {
          const req = parsed.content as PollRequest;

          // Tell client to fetch this URL
          // But first register resolver so /respond can deliver result
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

          // Notify /next that there's a urlRequest ready
          self.notifyClient({ type: 'urlRequest', request: req });

          // When /respond delivers the response, fire it back to service listener
          // This runs async — service is awaiting the wsFetch promise
          responsePromise.then(responseData => {
            console.log(`[send] firing response back to service id=${req.id}`);
            self.messageListeners.get('message')?.forEach(l =>
              l({ data: JSON.stringify(responseData) })
            );
          }).catch(err => {
            console.error(`[send] response timeout for id=${req.id}:`, err);
          });

        } else if (parsed.type === 'result') {
          console.log(`[send] result streams=${parsed.content?.length ?? 0}`);
          self.notifyClient({ type: 'result', streams: parsed.content });

        } else if (parsed.type === 'error') {
          console.log(`[send] error: ${parsed.message}`);
          self.notifyClient({ type: 'error', message: parsed.message ?? 'Unknown error' });
        }
      },

      close(_code?: number, _reason?: string) { /* no-op */ },
    };

    return fakeSocket as unknown as WebSocket;
  }
}
