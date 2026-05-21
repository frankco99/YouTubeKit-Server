import { Innertube, Platform } from 'youtubei.js';
import { ServerMessage, ServerMessageType, RemoteURLResponse, RemoteURLRequest, RemoteStream } from './models/websocket';
import { AvailableInnertubeClient } from './models/internal';
import { fileExtensionFromMimeType } from './file_extension';
import { evaluateJavaScript } from './js-evaluator';

export class YouTubeService {
   private static readonly PLAYER_ID_OVERRIDE: string | undefined = undefined;

   readonly videoID: string;
   private websocket: WebSocket;
   private inflightFetches = new Map<string, (msg: RemoteURLResponse) => void>();

   // State for handling incoming chunked messages, keyed by packetId (stringified)
   private chunkBuffers = new Map<string, { buffer: ArrayBuffer[]; expectedTotal: number; receivedCount: number }>();
   // Fixed 12-byte header: packetId (4), chunkIndex (4), totalChunks (4)
   private static readonly CHUNK_HEADER_SIZE = 12;

   // If provided, use this fetch instead of routing through client (used by HTTP polling)
   private directFetch: typeof fetch | undefined;

   constructor(videoID: string, websocket: WebSocket, directFetch?: typeof fetch) {
      this.videoID = videoID;
      this.websocket = websocket;
      this.directFetch = directFetch;
   }

   private send(data: ServerMessage<any>) {
      this.websocket.send(JSON.stringify(data));
   }

   async start() {
      // If directFetch is provided (HTTP polling path), use it directly —
      // no need to round-trip through the client device for each YouTube request.
      const wsFetch: typeof fetch = this.directFetch ?? (async (input, init = {}) => {
         const req = new Request(input, init);
         const id = crypto.randomUUID();

         const body = req.body ? await req.arrayBuffer() : undefined;

         const payload: RemoteURLRequest = {
            id,
            url: req.url,
            method: req.method,
            headers: Object.fromEntries(req.headers.entries()),
            body: body ? btoa(String.fromCharCode(...new Uint8Array(body))) : undefined,
            allow_redirects: true,
            apply_cookies_on_redirect: true,
            save_intermediate_responses: false,
            max_message_chunk_size: 900 * 1024,
         };

         this.send({ type: ServerMessageType.urlRequest, content: payload });

         const responsePromise = new Promise<RemoteURLResponse>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('Client fetch timeout')), 55_000);
            this.inflightFetches.set(id, msg => {
               clearTimeout(timer);
               resolve(msg);
            });
         });

         const resMsg = await responsePromise;
         const binary = Uint8Array.from(atob(resMsg.data), c => c.charCodeAt(0));
         return new Response(binary, { status: resMsg.status_code, headers: resMsg.headers });
      });

      // Listen for messages from client (only needed for WebSocket path)
      this.websocket.addEventListener('message', async event => {
         try {
            const data: any = event.data;

            if (data instanceof ArrayBuffer) {
               if (data.byteLength >= YouTubeService.CHUNK_HEADER_SIZE) {
                  await this.handleChunk(data);
               } else {
                  this.processCompleteMessage(new TextDecoder().decode(data));
               }
            } else if (data instanceof Blob) {
               const arrayBuffer = await data.arrayBuffer();
               if (arrayBuffer.byteLength >= YouTubeService.CHUNK_HEADER_SIZE) {
                  await this.handleChunk(arrayBuffer);
               } else {
                  this.processCompleteMessage(new TextDecoder().decode(arrayBuffer));
               }
            } else if (typeof data === 'string') {
               this.processCompleteMessage(data);
            }
         } catch (error: any) {
            console.error('Error processing message from client:', error);
         }
      });

      try {
         Platform.shim.eval = async (data, env) => {
            return await evaluateJavaScript(data, env);
         };

         const innertube = await Innertube.create({
            fetch: wsFetch,
            ...(YouTubeService.PLAYER_ID_OVERRIDE ? { player_id: YouTubeService.PLAYER_ID_OVERRIDE } : {}),
         });
         const streams = await this.getStreams(innertube);

         this.send({ type: ServerMessageType.result, content: streams });
      } catch (error: any) {
         console.error(error);
         this.websocket.send(JSON.stringify({ type: 'error', message: error.message }));
         this.websocket.close(1008, 'Failed to get video info');
      } finally {
         this.websocket.close();
      }
   }

   private async handleChunk(chunkData: ArrayBuffer): Promise<void> {
      if (chunkData.byteLength < YouTubeService.CHUNK_HEADER_SIZE) {
         console.error(`Chunk too small for header (size: ${chunkData.byteLength}, needed: ${YouTubeService.CHUNK_HEADER_SIZE})`);
         return;
      }
      const headerView = new DataView(chunkData, 0, YouTubeService.CHUNK_HEADER_SIZE);
      let packetId: number | undefined;

      try {
         packetId = headerView.getUint32(0, false);
         const packetIdStr = packetId.toString();
         const chunkIndex = headerView.getUint32(4, false);
         const totalChunks = headerView.getUint32(8, false);
         const payload = chunkData.slice(YouTubeService.CHUNK_HEADER_SIZE);

         let state = this.chunkBuffers.get(packetIdStr);

         if (chunkIndex === 0) {
            state = { buffer: new Array(totalChunks), expectedTotal: totalChunks, receivedCount: 0 };
            this.chunkBuffers.set(packetIdStr, state);
         } else if (!state) {
            return;
         }

         if (totalChunks !== state.expectedTotal) {
            this.chunkBuffers.delete(packetIdStr);
            return;
         }

         if (chunkIndex >= state.expectedTotal || state.buffer[chunkIndex]) return;

         state.buffer[chunkIndex] = payload;
         state.receivedCount++;

         if (state.receivedCount === state.expectedTotal) {
            await this.reassembleAndProcess(packetIdStr);
         }
      } catch (error: any) {
         console.error('Error handling chunk:', error);
         if (packetId !== undefined) this.chunkBuffers.delete(packetId.toString());
      }
   }

   private async reassembleAndProcess(packetIdStr: string): Promise<void> {
      const state = this.chunkBuffers.get(packetIdStr);
      if (!state) return;

      try {
         for (let i = 0; i < state.expectedTotal; i++) {
            if (!state.buffer[i]) throw new Error(`Missing chunk ${i + 1}`);
         }
         const completeBuffer = await this.reassembleChunks(state.buffer);
         this.processCompleteMessage(new TextDecoder().decode(completeBuffer));
      } catch (e) {
         console.error(`Failed to reassemble Packet ID ${packetIdStr}:`, e);
      } finally {
         this.chunkBuffers.delete(packetIdStr);
      }
   }

   private async reassembleChunks(buffer: ArrayBuffer[]): Promise<ArrayBuffer> {
      const totalSize = buffer.reduce((sum, chunk) => sum + chunk.byteLength, 0);
      const reassembled = new Uint8Array(totalSize);
      let offset = 0;
      for (const chunk of buffer) {
         reassembled.set(new Uint8Array(chunk), offset);
         offset += chunk.byteLength;
      }
      return reassembled.buffer;
   }

   private processCompleteMessage(messageData: string): void {
      try {
         const parsed = JSON.parse(messageData) as RemoteURLResponse;
         const callback = this.inflightFetches.get(parsed.id);
         if (callback) {
            this.inflightFetches.delete(parsed.id);
            callback(parsed);
         }
      } catch (error: any) {
         console.error('Bad message format:', error);
      }
   }

   private async getStreams(innertube: Innertube): Promise<RemoteStream[]> {
      const clients: AvailableInnertubeClient[] = ['ANDROID_VR', 'WEB'];
      const fallbackClient: AvailableInnertubeClient = 'WEB_EMBEDDED';

      let allStreams: RemoteStream[] = [];

      for (const client of clients) {
         try {
            const streams = await this.getStreamsForClient(innertube, client);
            allStreams = allStreams.concat(streams);
         } catch (error) {
            console.error(`Failed to get streams for client ${client}:`, error);
         }
      }

      if (allStreams.length === 0) {
         try {
            return await this.getStreamsForClient(innertube, fallbackClient);
         } catch (error) {
            console.error(`Failed to get streams for fallback client ${fallbackClient}:`, error);
         }
      }

      return allStreams;
   }

   private async getStreamsForClient(innertube: Innertube, client: AvailableInnertubeClient): Promise<RemoteStream[]> {
      const info = await innertube.getInfo(this.videoID, { client });
      const f = info.streaming_data || { formats: [], adaptive_formats: [] };
      const formats = [...(f.formats ?? []), ...(f.adaptive_formats ?? [])];

      const BATCH_SIZE = 5;
      const streamsOrNull: (RemoteStream | null)[] = [];

      for (let i = 0; i < formats.length; i += BATCH_SIZE) {
         const batch = formats.slice(i, i + BATCH_SIZE);
         const batchResults = await Promise.all(
            batch.map(async format => {
               let deciphered: string | undefined;
               try {
                  deciphered = await format.decipher(innertube.session.player);
               } catch (error) {
                  deciphered = undefined;
               }

               const streamUrl = deciphered ?? ((format as any).deciphered_url as string | undefined);
               if (!streamUrl) return null;
               if (format.is_dubbed) return null;

               let mimeType = format.mime_type;
               let videoCodec: string | undefined;
               let audioCodec: string | undefined;

               if (mimeType?.includes('codecs=')) {
                  const codecString = mimeType.split('codecs=')[1]?.replace(/"/g, '') || '';
                  const codecs = codecString.split(',').map(c => c.trim()).filter(c => c.length > 0);

                  if (format.has_video && format.has_audio && codecs.length >= 2) {
                     videoCodec = codecs[0];
                     audioCodec = codecs[1];
                  } else if (format.has_video && codecs.length >= 1) {
                     videoCodec = codecs[0];
                  } else if (format.has_audio && codecs.length >= 1) {
                     audioCodec = codecs[0];
                  }
                  mimeType = mimeType.split(';')[0];
               }

               return {
                  url: streamUrl,
                  itag: format.itag,
                  ext: fileExtensionFromMimeType(mimeType),
                  video_codec: videoCodec,
                  audio_codec: audioCodec,
                  average_bitrate: format.bitrate || undefined,
                  audio_bitrate: format.has_audio ? format.bitrate : undefined,
                  video_bitrate: format.has_video ? format.bitrate : undefined,
                  filesize: format.content_length ? Number(format.content_length) : undefined,
               } as RemoteStream;
            })
         );
         streamsOrNull.push(...batchResults);
      }

      return streamsOrNull.filter((s): s is RemoteStream => s !== null);
   }
}
