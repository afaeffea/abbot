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
 *    pump_tts_audio/_emit_tts_audio) and sends it as
 *    Voximplant.WebSocketFormat events: a StartEvent naming the codec, then
 *    one JSON media event per 20ms chunk carrying base64 mu-law.
 *
 * The two directions are NOT symmetric, which is the trap this file spent
 * five attempts in: VoxEngine streams the caller's audio out to us as raw
 * binary frames, but only accepts audio coming back as those JSON events
 * (https://voximplant.com/docs/guides/media-streams/format, schema at
 * https://github.com/voximplant/protobuf/blob/main/websockets.proto --
 * MediaEvent.chunk and .timestamp are required, and omitting them is enough
 * for the whole stream to be ignored). Anything else is dropped in
 * silence: no error, no close, no warning, no sound. MEDIA_STARTED below
 * is the only positive confirmation that the stream was accepted.
 *
 * An earlier version of this file instead handed reply TEXT to Voximplant's
 * own native Cartesia.createRealtimeTTSPlayer connector and let it
 * synthesize and play the audio itself. That connector never played more
 * than the first ~300ms of any utterance on a real call
 * (PlayerEvents.PlaybackBuffering then permanent silence -- confirmed across
 * many calls, independent of API key/model/voice/message shape, and ruled
 * out as a Cartesia-account issue by replaying the exact same request
 * pattern directly against Cartesia's own realtime WS, which played the
 * full utterance correctly). Earlier attempts at the raw-audio approach
 * reached the WebSocket "confirmed live" but never produced audible sound;
 * they were all sending a format this route does not accept (see above).
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
      // No `encoding` here on purpose: for the ws->call direction the codec
      // comes from the backend's StartEvent.mediaFormat, not from this
      // option (which is why VoxEngine's session log never showed an
      // encoding on this leg's SendMedia command however we set it).
      ws.sendMediaTo(call);
      reportDiag({ stage: "outbound_media_attached" });
    } catch (err) {
      reportDiag({ stage: "outbound_media_attach_error", error: String(err) });
    }
  }

  ws.addEventListener(WebSocketEvents.OPEN, function () {
    call.sendMediaTo(ws, { encoding: WebSocketAudioEncoding.ULAW });
    attachOutboundMedia();
  });

  // Fires only if the backend's StartEvent was well-formed, which makes this
  // the definitive check on the outbound stream: every previous silent call
  // sent audio VoxEngine never accepted, and there was no signal anywhere
  // that told us so.
  ws.addEventListener(WebSocketEvents.MEDIA_STARTED, function () {
    reportDiag({ stage: "outbound_media_started" });
  });
  ws.addEventListener(WebSocketEvents.MEDIA_ENDED, function () {
    reportDiag({ stage: "outbound_media_ended" });
  });

  // Whether a frame we sent reaches this JS handler at all is the single most
  // diagnostic fact available about the silent ws->call direction: if it does,
  // the native ws.sendMediaTo(call) route did NOT consume it (routes intercept
  // media before it ever becomes a MESSAGE event), so that format is being
  // rejected. Capped at a handful of reports because media arrives ~50x/sec.
  var unroutedReports = 0;
  var reportUnrouted = function (payload) {
    if (unroutedReports >= 6) return;
    unroutedReports++;
    reportDiag(payload);
  };

  ws.addEventListener(WebSocketEvents.MESSAGE, function (e) {
    var data;
    try {
      data = JSON.parse(e.text);
    } catch (err) {
      reportUnrouted({
        stage: "unrouted_nonjson_message",
        textType: typeof e.text,
        length: e.text ? String(e.text).length : 0
      });
      return;
    }

    if (data.type !== "tts_cancel") {
      reportUnrouted({ stage: "unrouted_json_message", event: data.event || data.type || "?" });
    }

    if (data.type === "tts_cancel") {
      // Barge-in. The backend deliberately runs several seconds ahead of
      // real time, so by now VoxEngine is holding most of the cancelled
      // reply in its playback buffer -- dropping it here is what actually
      // stops the bot talking over the caller. clearMediaBuffer (not
      // stopMediaTo) because the media route itself should stay attached:
      // tearing it down and re-arming it would end the stream the backend's
      // StartEvent opened.
      try {
        ws.clearMediaBuffer();
      } catch (err) {
        reportDiag({ stage: "clear_media_buffer_error", error: String(err) });
      }
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
