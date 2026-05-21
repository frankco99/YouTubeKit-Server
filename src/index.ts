import { YouTubeService } from './youtube/service';
export { AppRateLimiter } from './durable-objects/app-rate-limiter';
export { PollSessionObject } from './durable-objects/poll-session';
import { RateLimitDecision } from './durable-objects/app-rate-limiter';

const APP_ID_HEADER = 'X-AppID-v1';
const APP_ID_MAX_LENGTH = 128;

export default {
   async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
      const url = new URL(request.url);

      const userAgent = request.headers.get('User-Agent') ?? 'unknown';
      console.log(`User Agent: ${userAgent}`);

      const appID = normalizeAppID(request.headers.get(APP_ID_HEADER));
      console.log(`App ID: ${appID}`);

      // --- WebSocket: GET /v1 (iOS / macOS / tvOS) ---
      if (url.pathname === '/v1' && request.headers.get('Upgrade') === 'websocket') {
         try {
            const decision = await checkRateLimit(appID, env);
            if (!decision.allowed) return buildRateLimitResponse(decision);
         } catch (error) {
            return new Response('Rate limiter unavailable', { status: 503 });
         }

         const videoID = url.searchParams.get('videoID');
         if (!videoID) return new Response('Missing videoID', { status: 400 });

         const [clientSock, serverSock] = Object.values(new WebSocketPair());
         serverSock.accept();
         new YouTubeService(videoID, serverSock).start();
         return new Response(null, { status: 101, webSocket: clientSock });
      }

      // --- HTTP Polling: POST /v1/poll/start (watchOS) ---
      if (url.pathname === '/v1/poll/start' && request.method === 'POST') {
         try {
            const decision = await checkRateLimit(appID, env);
            if (!decision.allowed) return buildRateLimitResponse(decision);
         } catch (error) {
            return new Response('Rate limiter unavailable', { status: 503 });
         }

         const videoID = url.searchParams.get('videoID');
         if (!videoID) return new Response('Missing videoID', { status: 400 });

         // Use a UUID as the DO name so we control the session_id format
         const sessionId = crypto.randomUUID();
         const doStub = getPollSession(env, sessionId);

         const doUrl = `https://do/start?videoID=${encodeURIComponent(videoID)}`;
         const resp = await doStub.fetch(new Request(doUrl, { method: 'POST' }));
         if (!resp.ok) return resp;

         // Return session_id to client (the UUID we generated)
         return Response.json({ session_id: sessionId });
      }

      // --- HTTP Polling: GET /v1/poll/next (watchOS) ---
      if (url.pathname === '/v1/poll/next' && request.method === 'GET') {
         const sessionId = url.searchParams.get('session_id');
         if (!sessionId) return new Response('Missing session_id', { status: 400 });

         const doStub = getPollSession(env, sessionId);
         return doStub.fetch(new Request('https://do/next', { method: 'GET' }));
      }

      // --- HTTP Polling: POST /v1/poll/respond (watchOS) ---
      if (url.pathname === '/v1/poll/respond' && request.method === 'POST') {
         const sessionId = url.searchParams.get('session_id');
         if (!sessionId) return new Response('Missing session_id', { status: 400 });

         const doStub = getPollSession(env, sessionId);
         return doStub.fetch(new Request('https://do/respond', {
            method: 'POST',
            body: request.body,
            headers: { 'Content-Type': 'application/json' },
         }));
      }

      return new Response('Not found', { status: 404 });
   },
} satisfies ExportedHandler<Env>;

function getPollSession(env: Env, sessionId: string) {
   const objectID = (env as any).POLL_SESSION.idFromName(sessionId);
   return (env as any).POLL_SESSION.get(objectID);
}

function normalizeAppID(rawAppID: string | null): string {
   const trimmed = rawAppID?.trim();
   if (!trimmed) return 'unknown';
   return trimmed.slice(0, APP_ID_MAX_LENGTH);
}

async function checkRateLimit(appID: string, env: Env): Promise<RateLimitDecision> {
   const objectID = env.APP_RATE_LIMITER.idFromName(appID);
   const rate_limiter = env.APP_RATE_LIMITER.get(objectID);
   return await rate_limiter.admit({ cost: 1, nowMs: Date.now() });
}

function buildRateLimitResponse(decision: RateLimitDecision): Response {
   return new Response('Too many requests', {
      status: 429,
      headers: {
         'Retry-After': decision.retryAfterSeconds.toString(),
         'X-RateLimit-Limit-Day': decision.limitDaily.toString(),
         'X-RateLimit-Limit-Week': decision.limitWeekly.toString(),
         'X-RateLimit-Remaining-Day': decision.remainingDaily.toString(),
         'X-RateLimit-Remaining-Week': decision.remainingWeekly.toString(),
      },
   });
}
