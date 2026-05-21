import { DurableObject } from 'cloudflare:workers';
import { PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

type Phase = 'waiting_for_request' | 'pending_request' | 'waiting_for_response' | 'done' | 'error';

export class PollSessionObject extends DurableObject {

  // Use storage so state survives DO hibernation
  private pendingResponseResolvers = new Map<string, (msg: any) => void>();
  private messageListeners = new Map<string, Set<(event: any) => void>>();

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

    await this.setPhase('waiting_for_request');

    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);

    // Keep DO alive for the duration of the service
    this.ctx.waitUntil(
      service.start().catch(async err => {
        console.error('YouTubeService error:', err);
        await this.ctx.storage.put('error', err.message ?? 'Unknown error');
        await this.setPhase('error');
      })
    );

    return new Response(null, { status: 204 });
  }

  private async handleNext(): Promise<Response> {
    const deadline = Date.now() + 50_000;

    while (Date.now() < deadline) {
      const phase = await this.getPhase();

      if (phase === 'pending_request') {
        const request = await this.ctx.storage.get<PollRequest>('pending_request');
        if (request) {
          await this.ctx.storage.delete('pending_request');
          await this.setPhase('waiting_for_response');
          return Response.json({ type: 'urlRequest', content: request });
        }
      }

      if (phase === 'done') {
        const streams = await this.ctx.storage.get<any[]>('result');
        return Response.json({ type: 'result', content: streams ?? [] });
      }

      if (phase === 'error') {
        const msg = await this.ctx.storage.get<string>('error');
        return new Response(
          JSON.stringify({ type: 'error', message: msg ?? 'Unknown error' }),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        );
      }

      await sleep(300);
    }

    return new Response(null, { status: 204 });
  }

  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    const phase = await this.getPhase();

    if (phase !== 'waiting_for_response') {
      return new Response('Unexpected respond', { status: 409 });
    }

    await this.setPhase('waiting_for_request');

    // Deliver to in-memory resolver if present (same instance)
    const resolver = this.pendingResponseResolvers.get(body.id);
    if (resolver) {
      this.pendingResponseResolvers.delete(body.id);
      resolver(body);
    } else {
      // Store for fake socket to pick up (cross-request delivery)
      await this.ctx.storage.put(`response:${body.id}`, body);
    }

    return new Response(null, { status: 204 });
  }

  // - Storage helpers -

  private async getPhase(): Promise<Phase> {
    return (await this.ctx.storage.get<Phase>('phase')) ?? 'waiting_for_request';
  }

  private async setPhase(phase: Phase): Promise<void> {
    await this.ctx.storage.put('phase', phase);
  }

  // - Fake WebSocket -

  private buildFakeWebSocket(): WebSocket {
    const self = this;

    const fakeSocket = {
      addEventListener(type: string, listener: (event: any) => void) {
        if (!self.messageListeners.has(type)) self.messageListeners.set(type, new Set());
        self.messageListeners.get(type)!.add(listener);
      },

      async send(data: string | ArrayBuffer) {
        if (typeof data !== 'string') return;
        let parsed: any;
        try { parsed = JSON.parse(data); } catch { return; }

        if (parsed.type === 'urlRequest') {
          const request = parsed.content as PollRequest;

          // Save to storage first, then set phase
          await self.ctx.storage.put('pending_request', request);
          await self.setPhase('pending_request');

          // Wait for response — check storage every 300ms
          const responseData = await new Promise<any>((resolve, reject) => {
            const id = request.id;
            const timer = setTimeout(() => reject(new Error('Client fetch timeout')), 55_000);

            // Register in-memory resolver (fast path, same instance)
            self.pendingResponseResolvers.set(id, data => {
              clearTimeout(timer);
              resolve(data);
            });

            // Also poll storage as fallback (different instance)
            const poll = setInterval(async () => {
              const stored = await self.ctx.storage.get<any>(`response:${id}`);
              if (stored) {
                clearInterval(poll);
                clearTimeout(timer);
                await self.ctx.storage.delete(`response:${id}`);
                self.pendingResponseResolvers.delete(id);
                resolve(stored);
              }
            }, 300);
          });

          // Fire back to YouTubeService listener
          self.messageListeners.get('message')?.forEach(l =>
            l({ data: JSON.stringify(responseData) })
          );

        } else if (parsed.type === 'result') {
          await self.ctx.storage.put('result', parsed.content);
          await self.setPhase('done');

        } else if (parsed.type === 'error') {
          await self.ctx.storage.put('error', parsed.message ?? 'Unknown error');
          await self.setPhase('error');
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
