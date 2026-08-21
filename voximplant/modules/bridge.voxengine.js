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
 */
function createTelephonyBridge(call) {
  var callId = "c" + Date.now() + "-" + Math.floor(Math.random() * 1e9);
  var url = BACKEND_WS_URL + "?token=" + encodeURIComponent(BACKEND_WS_TOKEN) + "&call_id=" + callId;
  var ws = VoxEngine.createWebSocket(url);

  var recordingUrl = BACKEND_WS_URL.replace("/ws/telephony", "/ws/telephony_recording")
    + "?token=" + encodeURIComponent(BACKEND_WS_TOKEN) + "&call_id=" + callId;
  var recordingWs = VoxEngine.createWebSocket(recordingUrl);
  var recordingReady = false;
  var player = null;
  var currentContextId = null;

  function attachRecording() {
    if (player && recordingReady) {
      try {
        player.sendMediaTo(recordingWs);
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
      var request = {
        model_id: data.model_id,
        transcript: data.text,
        voice: { mode: "id", id: data.voice_id },
        language: data.language,
        context_id: data.context_id,
        continue: data.continue
      };
      try {
        if (!player) {
          // Bring-your-own-key: if a CARTESIA_API_KEY secret is set (Control
          // Panel -> Secrets), billing/voice access goes through OUR Cartesia
          // account instead of Voximplant's shared default. getSecretValue
          // returns undefined if the secret doesn't exist -- Cartesia falls
          // back to Voximplant's own default access in that case, so this is
          // safe to leave in even before the secret is configured.
          var playerParams = {
            apiKey: VoxEngine.getSecretValue("CARTESIA_API_KEY"),
            generationRequestParameters: request
          };
          player = Cartesia.createRealtimeTTSPlayer(data.text, playerParams);
          player.sendMediaTo(call);
          attachRecording(); // no-ops until recordingWs is open too; its OPEN handler retries this
          // Diagnostic, left in deliberately: if PlaybackFinished never fires
          // (wrong event name), this dumps the real ones VoxEngine has so the
          // fix is a one-line rename instead of another blind guess.
          Logger.write("Cartesia PlayerEvents available: " + JSON.stringify(Object.keys(PlayerEvents || {})));
          player.addEventListener(PlayerEvents.PlaybackFinished, function () {
            try {
              ws.send(JSON.stringify({ type: "playback_finished", context_id: currentContextId }));
            } catch (err) {
              Logger.write("Failed to notify playback_finished: " + err);
            }
          });
        } else {
          player.generationRequest(request);
        }
      } catch (err) {
        Logger.write("Cartesia TTS error: " + err);
      }
    } else if (data.type === "tts_cancel") {
      // Barge-in: stop whatever the player is generating/has buffered right
      // away. Both calls are defensive (cheap if redundant) -- unlike the
      // browser path's estimated-duration mute, here NOT stopping playback
      // means the bot's voice keeps going out over the caller.
      if (player) {
        try {
          // Confirmed live: passing context_id here throws "Parameter
          // 'request' should be an object" (Player.Error) -- clearBuffer()
          // below is what actually stops playback anyway, so just drop the
          // argument rather than guess at an object shape with no docs to
          // check against.
          player.cancelContextRequest();
        } catch (err) {
          Logger.write("cancelContextRequest error: " + err);
        }
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
