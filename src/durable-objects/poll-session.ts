import { DurableObject } from 'cloudflare:workers';
import { PollSession, PollSessionState, PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

/**
 * PollSessionObject Durable Object
 * One instance per Apple Watch polling session, keyed by sessionId.
 */
export class PollSessionObject extends DurableObject {

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/start') {
      return this.handleStart(request);
    } else if (url.pathname === '/next') {
      return this.handleNext();
    } else if (url.pathname === '/respond') {
      return this.handleRespond(request);
    }

    return new Response('Not found', { status: 404 });
  }

  private async handleStart(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    // Initialize session state
    const session: PollSession = {
      sessionId: this.ctx.id.toString(),
      videoId,
      state: { phase: 'waiting_for_request' },
      createdAt: Date.now(),
    };
    await this.ctx.storage.put('session', session);

    // Start YouTubeService with fake WebSocket backed by this DO
    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);
    this.ctx.waitUntil(service.start());

    return Response.json({ session_id: this.ctx.id.toString() });
  }

  private async handleNext(): Promise<Response> {
    const deadline = Date.now() + 25_000;

    while (Date.now() < deadline) {
      const session = await this.ctx.storage.get<PollSession>('session');
      if (!session) return new Response('Session not found', { status: 404 });

      const state = session.state;

      if (state.phase === 'pending_request') {
        const message = { type: 'urlRequest', content: state.request };
        session.state = { phase: 'waiting_for_response', requestId: state.request.id };
        await this.ctx.storage.put('session', session);
        return Response.json(message);
      }

      if (state.phase === 'done') {
        return Response.json({ type: 'result', content: state.streams });
      }

      if (state.phase === 'error') {
        return new Response(JSON.stringify({ type: 'error', message: state.message }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      await sleep(200);
    }

    return new Response(null, { status: 204 });
  }

  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    const session = await this.ctx.storage.get<PollSession>('session');
    if (!session) return new Response('Session not found', { status: 404 });

    if (session.state.phase !== 'waiting_for_response') {
      return new Response('Unexpected respond', { status: 409 });
    }

    this.pendingResponseResolvers.get(body.id)?.(body);
    this.pendingResponseResolvers.delete(body.id);

    session.state = { phase: 'waiting_for_request' };
    await this.ctx.storage.put('session', session);

    return new Response(null, { status: 204 });
  }

  // - Fake WebSocket -

  private pendingResponseResolvers = new Map<string, (msg: any) => void>();

  private buildFakeWebSocket(): WebSocket {
    const self = this;
    const listeners = new Map<string, Set<(event: any) => void>>();

    const fakeSocket = {
      addEventListener(type: string, listener: (event: any) => void) {
        if (!listeners.has(type)) listeners.set(type, new Set());
        listeners.get(type)!.add(listener);
      },

      async send(data: string | ArrayBuffer) {
        if (typeof data !== 'string') return;

        let parsed: any;
        try { parsed = JSON.parse(data); } catch { return; }

        const session = await self.ctx.storage.get<PollSession>('session');
        if (!session) return;

        if (parsed.type === 'urlRequest') {
          session.state = { phase: 'pending_request', request: parsed.content as PollRequest };
          await self.ctx.storage.put('session', session);

          const requestId: string = parsed.content.id;
          const responsePromise = new Promise<any>(resolve => {
            self.pendingResponseResolvers.set(requestId, resolve);
          });

          responsePromise.then(responseData => {
            listeners.get('message')?.forEach(l => l({ data: JSON.stringify(responseData) }));
          });

        } else if (parsed.type === 'result') {
          session.state = { phase: 'done', streams: parsed.content };
          await self.ctx.storage.put('session', session);
        } else if (parsed.type === 'error') {
          session.state = { phase: 'error', message: parsed.message ?? 'Unknown error' };
          await self.ctx.storage.put('session', session);
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
