import { DurableObject } from 'cloudflare:workers';
import { PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

export class PollSessionObject extends DurableObject {

  // All state in-memory — DO stays alive via waitUntil + open /next request
  private phase: 'idle' | 'pending_request' | 'waiting_for_response' | 'done' | 'error' = 'idle';
  private pendingRequest: PollRequest | null = null;
  private result: any[] | null = null;
  private errorMessage: string | null = null;
  private messageListeners = new Map<string, Set<(event: any) => void>>();
  private pendingResponseResolvers = new Map<string, (msg: any) => void>();
  private pendingNextResolve: ((msg: any) => void) | null = null;

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/start')   return this.handleStart(request);
    if (url.pathname === '/next')    return this.handleNext();
    if (url.pathname === '/respond') return this.handleRespond(request);
    return new Response('Not found', { status: 404 });
  }

  private async handleStart(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    // Reset state
    this.phase = 'idle';
    this.pendingRequest = null;
    this.result = null;
    this.errorMessage = null;
    this.messageListeners.clear();
    this.pendingResponseResolvers.clear();
    this.pendingNextResolve = null;

    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);

    // waitUntil keeps DO alive while service runs
    this.ctx.waitUntil(
      service.start().catch(err => {
        console.error('[service] error:', err.message);
        this.notifyNext({ type: 'error', message: err.message ?? 'Unknown error' });
      })
    );

    return new Response(null, { status: 204 });
  }

  private async handleNext(): Promise<Response> {
    // Already have something ready
    if (this.phase === 'pending_request' && this.pendingRequest) {
      const req = this.pendingRequest;
      this.pendingRequest = null;
      this.phase = 'waiting_for_response';
      console.log(`[next] returning urlRequest id=${req.id}`);
      return Response.json({ type: 'urlRequest', content: req });
    }
    if (this.phase === 'done') {
      console.log(`[next] returning result streams=${this.result?.length ?? 0}`);
      return Response.json({ type: 'result', content: this.result });
    }
    if (this.phase === 'error') {
      console.log(`[next] returning error: ${this.errorMessage}`);
      return Response.json({ type: 'error', message: this.errorMessage });
    }

    // Wait up to 50s for service to produce something
    const message = await new Promise<any>((resolve, reject) => {
      this.pendingNextResolve = resolve;
      setTimeout(() => {
        this.pendingNextResolve = null;
        reject(new Error('poll timeout'));
      }, 50_000);
    }).catch(() => null);

    if (!message) return new Response(null, { status: 204 });

    if (message.type === 'urlRequest') {
      console.log(`[next] returning urlRequest id=${message.content.id}`);
    } else {
      console.log(`[next] returning ${message.type}`);
    }
    return Response.json(message);
  }

  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    console.log(`[respond] id=${body.id}`);

    const resolver = this.pendingResponseResolvers.get(body.id);
    if (resolver) {
      this.pendingResponseResolvers.delete(body.id);
      this.phase = 'idle';
      resolver(body);
    } else {
      console.warn(`[respond] no resolver for id=${body.id}`);
    }

    return new Response(null, { status: 204 });
  }

  // Notify the waiting /next request
  private notifyNext(message: any) {
    if (this.pendingNextResolve) {
      const resolve = this.pendingNextResolve;
      this.pendingNextResolve = null;
      resolve(message);
    } else {
      // /next not currently waiting — store for next poll
      if (message.type === 'urlRequest') {
        this.phase = 'pending_request';
        this.pendingRequest = message.content;
      } else if (message.type === 'result') {
        this.phase = 'done';
        this.result = message.content;
      } else if (message.type === 'error') {
        this.phase = 'error';
        this.errorMessage = message.message;
      }
    }
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

        console.log(`[send] type=${parsed.type}`);

        if (parsed.type === 'urlRequest') {
          const req = parsed.content as PollRequest;

          // Register resolver BEFORE notifying /next
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

          // Tell /next there's a urlRequest ready
          self.notifyNext({ type: 'urlRequest', content: req });

          // When /respond delivers response, fire to service
          responsePromise.then(responseData => {
            self.messageListeners.get('message')?.forEach(l =>
              l({ data: JSON.stringify(responseData) })
            );
          }).catch(err => {
            console.error(`[send] timeout for id=${req.id}:`, err.message);
          });

        } else if (parsed.type === 'result') {
          self.notifyNext({ type: 'result', content: parsed.content });

        } else if (parsed.type === 'error') {
          self.notifyNext({ type: 'error', message: parsed.message ?? 'Unknown error' });
        }
      },

      close() { /* no-op */ },
    };

    return fakeSocket as unknown as WebSocket;
  }
}
