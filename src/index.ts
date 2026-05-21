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

      // --- WebSocket: GET /v1?videoID=... (iOS / macOS / tvOS) ---
      if (url.pathname === '/v1' && request.headers.get('Upgrade') === 'websocket') {
         try {
            const decision = await checkRateLimit(appID, env);
            if (!decision.allowed) {
               console.warn('Rate limit rejected request', JSON.stringify({ appID }));
               return buildRateLimitResponse(decision);
            }
         } catch (error) {
            console.error('Rate limiter unavailable:', error);
            return new Response('Rate limiter unavailable', { status: 503 });
         }

         const videoID = url.searchParams.get('videoID');
         if (!videoID) return new Response('Missing videoID', { status: 400 });

         const [clientSock, serverSock] = Object.values(new WebSocketPair());
         serverSock.accept();

         const youtubeService = new YouTubeService(videoID, serverSock);
         youtubeService.start();

         return new Response(null, { status: 101, webSocket: clientSock });
      }

      // --- HTTP Polling: POST /v1/poll/start?videoID=... (watchOS) ---
      if (url.pathname === '/v1/poll/start' && request.method === 'POST') {
         try {
            const decision = await checkRateLimit(appID, env);
            if (!decision.allowed) {
               return buildRateLimitResponse(decision);
            }
         } catch (error) {
            return new Response('Rate limiter unavailable', { status: 503 });
         }

         const videoID = url.searchParams.get('videoID');
         if (!videoID) return new Response('Missing videoID', { status: 400 });

         const sessionId = crypto.randomUUID();
         const objectID = (env as any).POLL_SESSION.idFromName(sessionId);
         const pollSession = (env as any).POLL_SESSION.get(objectID);

         // Forward to the DO with videoID in the URL
         const doUrl = new URL(request.url);
         doUrl.pathname = '/start';
         doUrl.searchParams.set('videoID', videoID);
         doUrl.searchParams.set('sessionId', sessionId);
         return pollSession.fetch(new Request(doUrl.toString(), { method: 'POST' }));
      }

      // --- HTTP Polling: GET /v1/poll/next?session_id=... (watchOS) ---
      if (url.pathname === '/v1/poll/next' && request.method === 'GET') {
         const sessionId = url.searchParams.get('session_id');
         if (!sessionId) return new Response('Missing session_id', { status: 400 });

         const objectID = (env as any).POLL_SESSION.idFromName(sessionId);
         const pollSession = (env as any).POLL_SESSION.get(objectID);

         const doUrl = new URL(request.url);
         doUrl.pathname = '/next';
         return pollSession.fetch(new Request(doUrl.toString(), { method: 'GET' }));
      }

      // --- HTTP Polling: POST /v1/poll/respond?session_id=... (watchOS) ---
      if (url.pathname === '/v1/poll/respond' && request.method === 'POST') {
         const sessionId = url.searchParams.get('session_id');
         if (!sessionId) return new Response('Missing session_id', { status: 400 });

         const objectID = (env as any).POLL_SESSION.idFromName(sessionId);
         const pollSession = (env as any).POLL_SESSION.get(objectID);

         const doUrl = new URL(request.url);
         doUrl.pathname = '/respond';
         return pollSession.fetch(new Request(doUrl.toString(), {
            method: 'POST',
            body: request.body,
            headers: request.headers,
         }));
      }

      return new Response('Not found', { status: 404 });
   },
} satisfies ExportedHandler<Env>;

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
