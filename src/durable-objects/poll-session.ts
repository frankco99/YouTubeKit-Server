import { DurableObject } from 'cloudflare:workers';
import { PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

/**
 * PollSessionObject - pure storage-based message queue.
 *
 * NO in-memory state. Everything goes through DO storage.
 * This means any DO instance can handle any request for the same session.
 *
 * Storage keys:
 *   service_started      : "1" once service.start() has been called
 *   outbox               : JSON ServerMessage waiting for /next to pick up
 *   inbox:{id}           : proxy response waiting for service to pick up
 *   done                 : "1" when session complete
 */
export class PollSessionObject extends DurableObject {

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/start')   return this.handleStart(request);
    if (url.pathname === '/next')    return this.handleNext();
    if (url.pathname === '/respond') return this.handleRespond(request);
    return new Response('Not found', { status: 404 });
  }

  // POST /start — kick off YouTubeService in this request's context
  private async handleStart(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    // Clear any old state
    await this.ctx.storage.deleteAll();

    // Build fake socket and run service — service will block on each wsFetch
    // until /respond delivers the response via storage
    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);

    // Run service in background — DO stays alive via waitUntil
    this.ctx.waitUntil(
      service.start().catch(async err => {
        console.error('[service] error:', err.message);
        await this.ctx.storage.put('outbox', JSON.stringify({
          type: 'error', message: err.message ?? 'Unknown error'
        }));
      })
    );

    return new Response(null, { status: 204 });
  }

  // GET /next — poll outbox until message appears (up to 50s)
  private async handleNext(): Promise<Response> {
    const deadline = Date.now() + 50_000;

    while (Date.now() < deadline) {
      const raw = await this.ctx.storage.get<string>('outbox');
      if (raw) {
        const message = JSON.parse(raw);
        console.log(`[next] type=${message.type}`);

        if (message.type === 'urlRequest') {
          // Keep outbox set so if client crashes it can retry
          // Client must call /respond to clear it
          return Response.json(message);
        }

        // result or error — clear and return
        await this.ctx.storage.delete('outbox');
        return Response.json(message);
      }

      await sleep(250);
    }

    return new Response(null, { status: 204 });
  }

  // POST /respond — put response in inbox for service to pick up
  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    console.log(`[respond] id=${body.id}`);

    // Clear outbox (urlRequest has been handled)
    await this.ctx.storage.delete('outbox');
    // Put response in inbox
    await this.ctx.storage.put(`inbox:${body.id}`, JSON.stringify(body));

    return new Response(null, { status: 204 });
  }

  // Fake WebSocket — all communication via DO storage
  private buildFakeWebSocket(): WebSocket {
    const self = this;
    const listeners = new Map<string, Set<(event: any) => void>>();

    const fakeSocket = {
      addEventListener(type: string, listener: (event: any) => void) {
        if (!listeners.has(type)) listeners.set(type, new Set());
        listeners.get(type)!.add(listener);
      },

      // YouTubeService calls send() synchronously — we handle async internally
      send(data: string | ArrayBuffer) {
        if (typeof data !== 'string') return;
        let parsed: any;
        try { parsed = JSON.parse(data); } catch { return; }

        console.log(`[send] type=${parsed.type}`);

        if (parsed.type === 'urlRequest') {
          const req = parsed.content as PollRequest;

          // Write urlRequest to outbox — /next will pick it up
          // Then wait for /respond to put response in inbox
          const work = async () => {
            await self.ctx.storage.put('outbox', JSON.stringify({
              type: 'urlRequest', content: req
            }));

            // Poll inbox until response arrives (up to 55s)
            const deadline = Date.now() + 55_000;
            while (Date.now() < deadline) {
              const raw = await self.ctx.storage.get<string>(`inbox:${req.id}`);
              if (raw) {
                await self.ctx.storage.delete(`inbox:${req.id}`);
                const responseData = JSON.parse(raw);
                // Fire response back to YouTubeService
                listeners.get('message')?.forEach(l =>
                  l({ data: JSON.stringify(responseData) })
                );
                return;
              }
              await sleep(250);
            }
            console.error(`[send] inbox timeout for id=${req.id}`);
          };

          // We can't await here (send is sync), so fire and forget
          // The Promise keeps this DO alive via the event loop
          work().catch(err => console.error('[send] work error:', err));

        } else if (parsed.type === 'result' || parsed.type === 'error') {
          // Write final result/error to outbox for /next to return
          self.ctx.storage.put('outbox', data).catch(err =>
            console.error('[send] storage error:', err)
          );
        }
      },

      close() { /* no-op */ },
    };

    return fakeSocket as unknown as WebSocket;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
