// HTTP Polling session types (used by watchOS client)

export type PollSessionState =
  | { phase: 'waiting_for_request' }
  | { phase: 'pending_request'; request: PollRequest }
  | { phase: 'waiting_for_response'; requestId: string }
  | { phase: 'done'; streams: any[] }
  | { phase: 'error'; message: string };

export interface PollRequest {
  id: string;
  url: string;
  method: string;
  headers: Record<string, string>;
  body?: string; // base64
  allow_redirects: boolean;
  apply_cookies_on_redirect: boolean;
  save_intermediate_responses: boolean;
  max_message_chunk_size?: number;
}

export interface PollSession {
  sessionId: string;
  videoId: string;
  state: PollSessionState;
  createdAt: number;
}
