import { DurableObject } from 'cloudflare:workers';
import { PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

/**
 * PollSessionObject
 *
 * Outbox (server → client): stored in DO storage so /next can read it
 * from any instance. Values are small JSON (urlRequest metadata only,
 * no body data).
 *
 * Inbox (client → server): kept in-memory because response bodies can
 * be >128KB (YouTube JS player), which exceeds SQLITE_TOOBIG limit.
 * This works because /respond and the service work() loop always run
 * in the same DO instance (same session UUID → same DO).
 */
export class PollSessionObject extends DurableObject {

  // In-memory inbox: response bodies from Watch → service
  private inbox = new Map<string, (data: any) => void>();

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

    await this.ctx.storage.deleteAll();
    this.inbox.clear();

    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);

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

  private async handleNext(): Promise<Response> {
    const deadline = Date.now() + 50_000;

    while (Date.now() < deadline) {
      const raw = await this.ctx.storage.get<string>('outbox');
      if (raw) {
        const message = JSON.parse(raw);
        console.log(`[next] type=${message.type}`);
        if (message.type !== 'urlRequest') {
          await this.ctx.storage.delete('outbox');
        }
        return Response.json(message);
      }
      await sleep(250);
    }

    return new Response(null, { status: 204 });
  }

  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    console.log(`[respond] id=${body.id}`);

    // Clear outbox
    await this.ctx.storage.delete('outbox');

    // Deliver to in-memory resolver
    const resolve = this.inbox.get(body.id);
    if (resolve) {
      this.inbox.delete(body.id);
      resolve(body);
    } else {
      console.warn(`[respond] no inbox resolver for id=${body.id}`);
    }

    return new Response(null, { status: 204 });
  }

  private buildFakeWebSocket(): WebSocket {
    const self = this;
    const listeners = new Map<string, Set<(event: any) => void>>();

    const fakeSocket = {
      addEventListener(type: string, listener: (event: any) => void) {
        if (!listeners.has(type)) listeners.set(type, new Set());
        listeners.get(type)!.add(listener);
      },

      send(data: string | ArrayBuffer) {
        if (typeof data !== 'string') return;
        let parsed: any;
        try { parsed = JSON.parse(data); } catch { return; }

        console.log(`[send] type=${parsed.type}`);

        if (parsed.type === 'urlRequest') {
          const req = parsed.content as PollRequest;

          const work = async () => {
            // Only store small metadata in outbox (no body data)
            await self.ctx.storage.put('outbox', JSON.stringify({
              type: 'urlRequest', content: req
            }));

            // Wait for /respond to deliver response via in-memory inbox
            const responseData = await new Promise<any>((resolve, reject) => {
              const timer = setTimeout(() => {
                self.inbox.delete(req.id);
                reject(new Error('Client fetch timeout'));
              }, 55_000);

              self.inbox.set(req.id, data => {
                clearTimeout(timer);
                resolve(data);
              });
            });

            // Fire response back to YouTubeService
            listeners.get('message')?.forEach(l =>
              l({ data: JSON.stringify(responseData) })
            );
          };

          work().catch(err => console.error('[send] work error:', err));

        } else if (parsed.type === 'result' || parsed.type === 'error') {
          // Store final result — small enough for storage
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
