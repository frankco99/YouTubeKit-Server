import { DurableObject } from 'cloudflare:workers';
import { PollSession, PollSessionState, PollRequest } from '../youtube/models/poll';
import { YouTubeService } from '../youtube/service';

const SESSION_TTL_MS = 5 * 60 * 1000; // 5 minutes

/**
 * PollSession Durable Object
 *
 * One instance per HTTP polling session (i.e. one Apple Watch request).
 * Holds the full session state and drives YouTubeService exactly like
 * the WebSocket path does — but instead of a real WebSocket it uses a
 * fake one backed by this Durable Object's storage.
 *
 * Client flow:
 *   POST /v1/poll/start?videoID=xxx   → { session_id }
 *   GET  /v1/poll/next?session_id=x   → ServerMessage JSON  (blocks up to 25s)
 *   POST /v1/poll/respond?session_id=x → client sends proxy response
 */
export class PollSessionObject extends DurableObject {

  // Called by the Worker router to handle requests addressed to this DO
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

  // POST /start — initialize session and kick off YouTubeService
  private async handleStart(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const videoId = url.searchParams.get('videoID');
    if (!videoId) return new Response('Missing videoID', { status: 400 });

    const sessionId = crypto.randomUUID();

    const session: PollSession = {
      sessionId,
      videoId,
      state: { phase: 'waiting_for_request' },
      createdAt: Date.now(),
    };
    await this.ctx.storage.put('session', session);

    // Start YouTubeService with a fake WebSocket that routes through this DO
    const fakeSocket = this.buildFakeWebSocket();
    const service = new YouTubeService(videoId, fakeSocket);
    // Run in background — don't await
    this.ctx.waitUntil(service.start());

    return Response.json({ session_id: sessionId });
  }

  // GET /next — long-poll until state has a message for the client (up to 25s)
  private async handleNext(): Promise<Response> {
    const deadline = Date.now() + 25_000;

    while (Date.now() < deadline) {
      const session = await this.ctx.storage.get<PollSession>('session');
      if (!session) return new Response('Session not found', { status: 404 });

      const state = session.state;

      if (state.phase === 'pending_request') {
        // Server wants client to execute an HTTP request
        const message = {
          type: 'urlRequest',
          content: state.request,
        };
        // Transition to waiting_for_response
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

      // Still processing — wait 200ms and retry
      await sleep(200);
    }

    // Timeout — tell client to retry immediately (204 = nothing yet)
    return new Response(null, { status: 204 });
  }

  // POST /respond — client sends back the HTTP response it proxied
  private async handleRespond(request: Request): Promise<Response> {
    const body = await request.json() as any;
    const session = await this.ctx.storage.get<PollSession>('session');
    if (!session) return new Response('Session not found', { status: 404 });

    // Deliver the response to the waiting inflightFetches resolver
    const state = session.state;
    if (state.phase !== 'waiting_for_response') {
      return new Response('Unexpected respond', { status: 409 });
    }

    // Notify the fake WebSocket listener with the response
    this.pendingResponseResolvers.get(body.id)?.(body);
    this.pendingResponseResolvers.delete(body.id);

    // Transition back to waiting for next request from service
    session.state = { phase: 'waiting_for_request' };
    await this.ctx.storage.put('session', session);

    return new Response(null, { status: 204 });
  }

  // - Fake WebSocket -
  //
  // YouTubeService expects a WebSocket. We give it a fake one that:
  //   • send()     → stores an outgoing message so /next can return it
  //   • message    → we fire when /respond delivers a proxied response
  //   • close()    → no-op (session expires via TTL)

  private pendingResponseResolvers = new Map<string, (msg: any) => void>();

  private buildFakeWebSocket(): WebSocket {
    const self = this;

    // We build a minimal duck-typed object that satisfies what YouTubeService needs.
    const listeners = new Map<string, Set<(event: any) => void>>();

    const fakeSocket = {
      addEventListener(type: string, listener: (event: any) => void) {
        if (!listeners.has(type)) listeners.set(type, new Set());
        listeners.get(type)!.add(listener);
      },

      async send(data: string | ArrayBuffer) {
        const session = await self.ctx.storage.get<PollSession>('session');
        if (!session) return;

        // YouTubeService sends two kinds of messages:
        //   1. JSON ServerMessage<RemoteURLRequest> — forward to client via /next
        //   2. JSON ServerMessage<result>           — forward to client via /next
        let parsed: any;
        if (typeof data === 'string') {
          try { parsed = JSON.parse(data); } catch { return; }
        } else {
          // Binary (chunked) — shouldn't happen server→client, but handle gracefully
          return;
        }

        if (parsed.type === 'urlRequest') {
          // Service wants client to fetch something — surface via /next
          session.state = { phase: 'pending_request', request: parsed.content as PollRequest };
          await self.ctx.storage.put('session', session);

          // Also register a resolver so when /respond comes in we can notify service
          const requestId: string = parsed.content.id;
          const responsePromise = new Promise<any>(resolve => {
            self.pendingResponseResolvers.set(requestId, resolve);
          });

          // Deliver response back to YouTubeService via fake message event
          responsePromise.then(responseData => {
            const messageListeners = listeners.get('message');
            if (messageListeners) {
              const event = { data: JSON.stringify(responseData) };
              messageListeners.forEach(l => l(event));
            }
          });

        } else if (parsed.type === 'result' || parsed.type === 'error') {
          if (parsed.type === 'result') {
            session.state = { phase: 'done', streams: parsed.content };
          } else {
            session.state = { phase: 'error', message: parsed.message ?? 'Unknown error' };
          }
          await self.ctx.storage.put('session', session);
        }
      },

      close(_code?: number, _reason?: string) {
        // no-op — session TTL handles cleanup
      },
    };

    return fakeSocket as unknown as WebSocket;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
