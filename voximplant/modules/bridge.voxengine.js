// Shared call<->backend bridge for both inbound.voxengine.js and
// outbound.voxengine.js. Paste this ABOVE the rest of the scenario code in
// the Voximplant scenario editor (VoxEngine scenarios are single files, no
// real module system across scenarios) -- kept in its own file here only for
// readability/version control.
//
// !!! CONFIGURE BEFORE DEPLOYING !!!
var BACKEND_WS_URL = "wss://YOUR-RAILWAY-DOMAIN/ws/telephony"; // no trailing slash
var BACKEND_WS_TOKEN = "YOUR-TELEPHONY-WS-TOKEN"; // must match TELEPHONY_WS_TOKEN in the backend's .env

require(Modules.WebSocket);

/**
 * Bridges one call's audio to/from our backend over a single WebSocket.
 *
 * Both directions are plain VoxEngine media routes, set up once at OPEN:
 *  - call.sendMediaTo(ws) -- caller's voice -> our backend. Confirmed
 *    working since the first version of this bridge.
 *  - ws.sendMediaTo(call) -- the backend's synthesized bot voice -> the
 *    caller. The backend synthesizes with Cartesia itself (the same code
 *    path the browser transport uses -- see backend/session_base.py's
 *    pump_tts_audio/_emit_tts_audio) and pushes raw mu-law bytes as binary
 *    WebSocket frames; once this route is attached, VoxEngine forwards them
 *    into the call the same way it already forwards the caller's audio out
 *    to us in the other direction.
 *
 * An earlier version of this file instead handed reply TEXT to Voximplant's
 * own native Cartesia.createRealtimeTTSPlayer connector and let it
 * synthesize and play the audio itself. That connector never played more
 * than the first ~300ms of any utterance on a real call
 * (PlayerEvents.PlaybackBuffering then permanent silence -- confirmed across
 * many calls, independent of API key/model/voice/message shape, and ruled
 * out as a Cartesia-account issue by replaying the exact same request
 * pattern directly against Cartesia's own realtime WS, which played the
 * full utterance correctly). Four earlier attempts at this same raw-audio
 * approach reached the WebSocket "confirmed live" (WebSocketEvents.MESSAGE
 * fired) but never produced audible sound -- most likely because they never
 * actually called ws.sendMediaTo(call): there's no other VoxEngine API to
 * get JS-visible WebSocket bytes into a live call's audio stream, and
 * WebSocketEvents.MESSAGE firing on its own doesn't do that.
 *
 * @param {Call} call - an answered inbound call, or a connected outbound call.
 * @param {string} [phoneNumber] - the dialed number, for outbound calls only
 *   (see outbound.voxengine.js). Forwarded to the backend so a finished
 *   call's transcript can be matched back to the client it was placed for --
 *   inbound calls have no such number and omit this argument entirely.
 * @param {string} [clientName] - the dialed client's name, for outbound
 *   calls only, when known (see outbound.voxengine.js/run_campaign.py).
 *   Forwarded so the backend can personalize the greeting/system prompt.
 */
function createTelephonyBridge(call, phoneNumber, clientName) {
  var callId = "c" + Date.now() + "-" + Math.floor(Math.random() * 1e9);
  var phoneParam = phoneNumber ? "&phone_number=" + encodeURIComponent(phoneNumber) : "";
  var nameParam = clientName ? "&client_name=" + encodeURIComponent(clientName) : "";
  var url = BACKEND_WS_URL + "?token=" + encodeURIComponent(BACKEND_WS_TOKEN) + "&call_id=" + callId + phoneParam + nameParam;
  var ws = VoxEngine.createWebSocket(url);

  // VoxEngine's own Logger.write output is only visible in the live web
  // debugger / a session log that needs a browser login to fetch -- neither
  // reachable from here. Piggyback diagnostics over this same WebSocket
  // instead, so they land in the backend's own logs (Railway), which are
  // already reachable via the API.
  var reportDiag = function (payload) {
    try {
      ws.send(JSON.stringify(Object.assign({ type: "diag" }, payload)));
    } catch (err) {
      Logger.write("reportDiag failed: " + err);
    }
  };

  function attachOutboundMedia() {
    try {
      // NOT ULAW: VoxEngine's own session log shows this direction's
      // SendMedia command silently drops an `encoding` option that the
      // other (call->ws) direction honors -- confirmed live (2026-08-28)
      // that error-free, connection-checked binary ULAW frames sent from
      // the backend on this route still produced total silence on the
      // caller's end. Sending PCM8 (Voximplant's name for 16-bit linear
      // PCM at 8kHz) instead, matching their documented default for this
      // parameter, in case the ULAW option genuinely wasn't taking effect.
      ws.sendMediaTo(call, { encoding: WebSocketAudioEncoding.PCM8 });
      reportDiag({ stage: "outbound_media_attached" });
    } catch (err) {
      reportDiag({ stage: "outbound_media_attach_error", error: String(err) });
    }
  }

  ws.addEventListener(WebSocketEvents.OPEN, function () {
    call.sendMediaTo(ws, { encoding: WebSocketAudioEncoding.ULAW });
    attachOutboundMedia();
  });

  ws.addEventListener(WebSocketEvents.MESSAGE, function (e) {
    var data;
    try {
      data = JSON.parse(e.text);
    } catch (err) {
      // Not a JSON control message -- either a binary bot-audio frame
      // (already routed into the call by attachOutboundMedia()'s
      // ws.sendMediaTo(call) above, nothing to do with it here) or
      // something malformed. Either way, nothing actionable at this layer.
      return;
    }

    if (data.type === "tts_cancel") {
      // Barge-in: stop whatever's already buffered on the call leg right
      // away, then immediately re-arm the route so the next turn's audio
      // still gets through. Unlike the browser path's estimated-duration
      // mute, here NOT stopping playback means the bot's voice keeps going
      // out over the caller.
      try {
        ws.stopMediaTo(call);
      } catch (err) {
        reportDiag({ stage: "stop_media_to_error", error: String(err) });
      }
      attachOutboundMedia();
    }
  });

  ws.addEventListener(WebSocketEvents.CLOSE, function () {
    Logger.write("Telephony bridge WebSocket closed");
  });
  ws.addEventListener(WebSocketEvents.ERROR, function (e) {
    Logger.write("Telephony bridge WebSocket error: " + JSON.stringify(e));
  });

  var cleanup = function () {
    try { ws.close(); } catch (e) {}
    VoxEngine.terminate();
  };
  call.addEventListener(CallEvents.Disconnected, cleanup);
  call.addEventListener(CallEvents.Failed, cleanup);

  return ws;
}
