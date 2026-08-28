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
require(Modules.Cartesia);

/**
 * Bridges one call's audio to/from our backend.
 *
 * Inbound direction (caller's voice -> our backend) goes over the WebSocket
 * via call.sendMediaTo(ws) -- confirmed working on real calls.
 *
 * Outbound direction (bot's voice -> caller) does NOT go back over the
 * WebSocket. Four different attempts at pushing synthesized audio through
 * ws.sendMediaTo(call) / VoxEngine.sendMediaBetween(call, ws) -- raw binary,
 * two JSON envelope shapes, and fixed 160-byte framing matching Voximplant's
 * own inbound chunk size -- all reached this WebSocket confirmed-live
 * (WebSocketEvents.MESSAGE fired every time) but never produced audible sound
 * on a real call. Instead, the backend sends reply TEXT over this same
 * WebSocket as {"type":"tts_text",...} control messages, and THIS scenario
 * synthesizes it with Voximplant's own native Cartesia connector and plays it
 * into the call via player.sendMediaTo(call) -- a native media unit sending
 * TO a call, the same direction primitive as call.sendMediaTo(ws) above,
 * which is the one direction that's ever actually worked here.
 *
 * A SECOND WebSocket (recordingWs) also gets a copy of the player's audio,
 * purely so the backend can include the bot's voice in the call recording --
 * it never sees that audio otherwise, since playback happens entirely on
 * Voximplant's side now. This has to be a separate connection: Voximplant's
 * JSON media envelope carries no field telling caller-audio and bot-audio
 * apart, so mixing both into the main socket would feed the bot's own voice
 * into the backend's speech detection as if the caller had said it.
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
function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return String(value);
  }
}

function createTelephonyBridge(call, phoneNumber, clientName) {
  var callId = "c" + Date.now() + "-" + Math.floor(Math.random() * 1e9);
  var phoneParam = phoneNumber ? "&phone_number=" + encodeURIComponent(phoneNumber) : "";
  var nameParam = clientName ? "&client_name=" + encodeURIComponent(clientName) : "";
  var url = BACKEND_WS_URL + "?token=" + encodeURIComponent(BACKEND_WS_TOKEN) + "&call_id=" + callId + phoneParam + nameParam;
  var ws = VoxEngine.createWebSocket(url);

  var recordingUrl = BACKEND_WS_URL.replace("/ws/telephony", "/ws/telephony_recording")
    + "?token=" + encodeURIComponent(BACKEND_WS_TOKEN) + "&call_id=" + callId + phoneParam + nameParam;
  var recordingWs = VoxEngine.createWebSocket(recordingUrl);
  var recordingReady = false;
  var player = null;
  var currentContextId = null;

  function attachRecording() {
    if (player && recordingReady) {
      try {
        // Confirmed live: without an explicit encoding here, the backend's
        // pump_recording_audio (which assumes mu-law, matching what
        // call.sendMediaTo(ws, {encoding: ULAW}) sends on the main leg) was
        // fed some other default format and decoded it as mu-law anyway --
        // heard as loud white noise in the resulting recording. Matching the
        // main leg's explicit encoding fixes the mismatch.
        player.sendMediaTo(recordingWs, { encoding: WebSocketAudioEncoding.ULAW });
      } catch (err) {
        Logger.write("player.sendMediaTo(recordingWs) error: " + err);
      }
    }
  }

  recordingWs.addEventListener(WebSocketEvents.OPEN, function () {
    recordingReady = true;
    attachRecording();
  });
  recordingWs.addEventListener(WebSocketEvents.ERROR, function (e) {
    Logger.write("Recording WebSocket error: " + JSON.stringify(e));
  });

  ws.addEventListener(WebSocketEvents.OPEN, function () {
    call.sendMediaTo(ws, { encoding: WebSocketAudioEncoding.ULAW });
  });

  ws.addEventListener(WebSocketEvents.MESSAGE, function (e) {
    var data;
    try {
      data = JSON.parse(e.text);
    } catch (err) {
      return; // not JSON -- not one of our control messages (shouldn't happen on this leg)
    }

    if (data.type === "tts_text") {
      currentContextId = data.context_id;
      // Diagnostic: VoxEngine's own Logger.write output is only visible in
      // the live web debugger / a session log that needs a browser login to
      // fetch -- neither reachable from here. Piggyback diagnostics over
      // this same WebSocket instead, so they land in the backend's own logs
      // (Railway), which are already reachable via the API.
      var reportDiag = function (payload) {
        try {
          ws.send(JSON.stringify(Object.assign({ type: "diag" }, payload)));
        } catch (err) {
          Logger.write("reportDiag failed: " + err);
        }
      };
      var request = {
        model_id: data.model_id,
        transcript: data.text,
        voice: { mode: "id", id: data.voice_id },
        language: data.language,
        context_id: data.context_id,
        continue: data.continue
        // Confirmed live (2026-08-28): a raw Cartesia realtime WS request
        // needs an explicit output_format (see backend/cartesia_realtime.py,
        // the diag script) -- but Voximplant's Cartesia.createRealtimeTTSPlayer
        // wrapper sets that itself and REJECTS the field if present:
        // PlayerEvents.Error fired with "The request should not contain the
        // \"output_format\" field" as soon as it was added. Do not add it here.
      };
      try {
        if (!player) {
          // Bring-your-own-key: if a CARTESIA_API_KEY secret is set (Control
          // Panel -> Secrets), billing/voice access goes through OUR Cartesia
          // account instead of Voximplant's shared default. getSecretValue
          // returns undefined if the secret doesn't exist -- Cartesia falls
          // back to Voximplant's own default access in that case, so this is
          // safe to leave in even before the secret is configured.
          var hasApiKeySecret = !!VoxEngine.getSecretValue("CARTESIA_API_KEY");
          var playerParams = {
            apiKey: VoxEngine.getSecretValue("CARTESIA_API_KEY"),
            generationRequestParameters: request
          };
          reportDiag({ stage: "creating_player", request: request, hasApiKeySecret: hasApiKeySecret });
          player = Cartesia.createRealtimeTTSPlayer(data.text, playerParams);
          player.sendMediaTo(call);
          attachRecording(); // no-ops until recordingWs is open too; its OPEN handler retries this
          // Diagnostic, left in deliberately: if these never fire, this dumps
          // the real ones VoxEngine has so the fix is a one-line rename
          // instead of another blind guess.
          reportDiag({ stage: "player_events_available", events: Object.keys(PlayerEvents || {}) });
          var notifyPlaybackFinished = function () {
            try {
              ws.send(JSON.stringify({ type: "playback_finished", context_id: currentContextId }));
            } catch (err) {
              Logger.write("Failed to notify playback_finished: " + err);
            }
          };
          // Confirmed live: PlaybackFinished never fired ONCE across an
          // entire multi-turn call -- it's most likely "the whole player
          // stopped" (once per call), not "this turn's audio finished
          // playing". Without a per-turn signal, the backend's bot_speaking
          // never reset after the greeting, so the ENTIRE rest of the call
          // got evaluated against the stricter barge-in thresholds instead
          // of normal listening ones -- that's what read as "~20s delay
          // before it reacts to anything" and "can't interrupt it". Trying
          // AudioChunksPlaybackFinished instead (also in the PlayerEvents
          // list above), documented elsewhere as firing per finished chunk
          // of buffered audio. Keeping both listeners active (cheap,
          // harmless if PlaybackFinished really is call-level only).
          player.addEventListener(PlayerEvents.PlaybackFinished, notifyPlaybackFinished);
          player.addEventListener(PlayerEvents.AudioChunksPlaybackFinished, notifyPlaybackFinished);
          // Diagnostic: PlayerEvents.Error was listed as available but never
          // had a listener attached -- if Cartesia's realtime API is
          // rejecting the request (bad key, quota, param mismatch), it would
          // fire silently right here with nothing logging it.
          player.addEventListener(PlayerEvents.Error, function (e) {
            reportDiag({ stage: "player_error", error: String(e), errorJson: safeStringify(e) });
          });
          player.addEventListener(PlayerEvents.Started, function () {
            reportDiag({ stage: "player_started" });
          });
        } else {
          reportDiag({ stage: "generation_request", request: request });
          player.generationRequest(request);
        }
      } catch (err) {
        reportDiag({ stage: "tts_exception", error: String(err) });
        Logger.write("Cartesia TTS error: " + err);
      }
    } else if (data.type === "tts_cancel") {
      // Barge-in: stop whatever the player has buffered right away -- unlike
      // the browser path's estimated-duration mute, here NOT stopping
      // playback means the bot's voice keeps going out over the caller.
      //
      // cancelContextRequest() used to be called here too, but confirmed
      // live on a real call: since lastRequest is always the turn's closing
      // {transcript: " ", continue: false} object (it's overwritten on every
      // tts_text message, and the closing one is always sent last),
      // cancelContextRequest(lastRequest) just resends that exact stale
      // payload through the same underlying SendRequestToPlayer command --
      // it's not a distinct "stop" primitive, just a redundant resend, and
      // once directly caused a "Player.PlaybackFinished ; error = Invalid
      // transcript" from Cartesia rejecting the re-sent blank transcript.
      // clearBuffer() alone is what actually stops playback (confirmed by
      // every barge-in in that same call log audibly cutting the bot off).
      if (player) {
        try {
          player.clearBuffer();
        } catch (err) {
          Logger.write("clearBuffer error: " + err);
        }
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
    try { recordingWs.close(); } catch (e) {}
    VoxEngine.terminate();
  };
  call.addEventListener(CallEvents.Disconnected, cleanup);
  call.addEventListener(CallEvents.Failed, cleanup);

  return ws;
}
