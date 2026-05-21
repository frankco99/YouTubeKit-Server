import { DurableObject } from 'cloudflare:workers';
import { PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

export class PollSessionObject extends DurableObject {

  private phase: 'idle' | 'pending_request' | 'waiting_for_response' | 'done' | 'error' = 'idle';
  private pendingRequest: PollRequest | null = null;
  private result: any[] | null = null;
  private errorMessage: string | null = null;
  private messageListeners = new Map<string, Set<(event: any) => void>>();
  private pendingResponseResolvers = new Map<string, (msg: any) => void>();
  private pendingNextResolve: ((msg: any) => void) | null = null;
  private sessionActive = false;

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/start')   return this.handleStart(request);
    if (url.pathname === '/next')    return this.handleNext();
    if (url.pathname === '/respond') return this.handleRespond(request);
    return new Response('Not found', { status: 404 });
  }

  async alarm() {
    if (this.sessionActive) {
      await this.ctx.storage.setAlarm(Date.now() + 25_000);
    }
  }

  private async handleStart(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    this.phase = 'idle';
    this.pendingRequest = null;
    this.result = null;
    this.errorMessage = null;
    this.messageListeners.clear();
    this.pendingResponseResolvers.clear();
    this.pendingNextResolve = null;
    this.sessionActive = true;

    await this.ctx.storage.setAlarm(Date.now() + 25_000);

    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);

    this.ctx.waitUntil(
      service.start().catch(err => {
        console.error('[service] error:', err.message);
        this.notifyNext({ type: 'error', message: err.message ?? 'Unknown error' });
      }).finally(() => {
        this.sessionActive = false;
      })
    );

    return new Response(null, { status: 204 });
  }

  private async handleNext(): Promise<Response> {
    // Re-send pending urlRequest if client retrying
    if (this.phase === 'pending_request' && this.pendingRequest) {
      const req = this.pendingRequest;
      this.phase = 'waiting_for_response';
      console.log(`[next] urlRequest id=${req.id}`);
      return Response.json({ type: 'urlRequest', content: req });
    }
    if (this.phase === 'waiting_for_response' && this.pendingRequest) {
      // Client retrying after timeout — resend same request
      console.log(`[next] re-send urlRequest id=${this.pendingRequest.id}`);
      return Response.json({ type: 'urlRequest', content: this.pendingRequest });
    }
    if (this.phase === 'done') {
      console.log(`[next] result streams=${this.result?.length ?? 0}`);
      return Response.json({ type: 'result', content: this.result });
    }
    if (this.phase === 'error') {
      return Response.json({ type: 'error', message: this.errorMessage });
    }

    // Wait for service to produce something
    const message = await new Promise<any>((resolve, reject) => {
      this.pendingNextResolve = resolve;
      setTimeout(() => {
        this.pendingNextResolve = null;
        reject(new Error('poll timeout'));
      }, 50_000);
    }).catch(() => null);

    if (!message) return new Response(null, { status: 204 });
    console.log(`[next] ${message.type} ${message.content?.id ?? ''}`);
    return Response.json(message);
  }

  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    console.log(`[respond] id=${body.id}`);

    const resolver = this.pendingResponseResolvers.get(body.id);
    if (resolver) {
      this.pendingResponseResolvers.delete(body.id);
      this.pendingRequest = null;
      this.phase = 'idle';
      resolver(body);
    } else {
      console.warn(`[respond] no resolver for id=${body.id}`);
    }

    return new Response(null, { status: 204 });
  }

  private notifyNext(message: any) {
    if (this.pendingNextResolve) {
      const resolve = this.pendingNextResolve;
      this.pendingNextResolve = null;
      if (message.type === 'urlRequest') {
        this.phase = 'waiting_for_response';
        this.pendingRequest = message.content;
      }
      resolve(message);
    } else {
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

          self.notifyNext({ type: 'urlRequest', content: req });

          responsePromise.then(responseData => {
            self.messageListeners.get('message')?.forEach(l =>
              l({ data: JSON.stringify(responseData) })
            );
          }).catch(err => {
            console.error(`[send] timeout id=${req.id}:`, err.message);
            self.notifyNext({ type: 'error', message: err.message });
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
