// Shared call<->backend bridge for both inbound.voxengine.js and
// outbound.voxengine.js. Paste this ABOVE the rest of the scenario code in
// the Voximplant scenario editor (VoxEngine scenarios are single files, no
// real module system across scenarios) -- kept in its own file here only for
// readability/version control.
//
// !!! CONFIGURE BEFORE DEPLOYING !!!
// I could not find/verify Voximplant's actual Secrets-accessor JS API (their
// docs site didn't return real content to automated fetching -- see the
// implementation plan for details), so rather than guess a function name
// that might not exist and fail silently, these are plain constants you fill
// in by hand. If the scenario editor's Secrets panel turns out to offer a
// real accessor (check there first), prefer that over hardcoding the token
// in the pasted script -- update these two lines to call it instead.
var BACKEND_WS_URL = "wss://YOUR-RAILWAY-DOMAIN/ws/telephony"; // no trailing slash
var BACKEND_WS_TOKEN = "YOUR-TELEPHONY-WS-TOKEN"; // must match TELEPHONY_WS_TOKEN in the backend's .env

require(Modules.WebSocket);

/**
 * Bridges one call's audio to/from our backend over a WebSocket.
 * @param {Call} call - an answered inbound call, or a connected outbound call.
 */
function createTelephonyBridge(call) {
  var url = BACKEND_WS_URL + "?token=" + encodeURIComponent(BACKEND_WS_TOKEN);
  var ws = VoxEngine.createWebSocket(url);

  ws.addEventListener(WebSocketEvents.OPEN, function () {
    // PRIMARY approach: sendMediaBetween is documented (in Voximplant's newer
    // Voice AI orchestration docs, for their Ultravox connector) as bridging
    // "call media and control signals bidirectionally" -- but I never found a
    // verbatim example of it used with a *raw* createWebSocket() object
    // rather than one of Voximplant's own connector-client objects. This is
    // the one part of this integration I could not verify end-to-end myself
    // (no phone, no Voximplant account access). Test a real call and watch
    // this scenario's Logger output / the backend's own logs:
    //   - If the bot's voice is audible in the call: it worked, done.
    //   - If the caller's speech reaches the backend (check backend logs for
    //     STT transcripts) but the bot is silent on the call: sendMediaBetween
    //     isn't routing the reverse direction. Comment out the line below and
    //     uncomment the two-line FALLBACK further down instead.
    VoxEngine.sendMediaBetween(call, ws);

    // FALLBACK (uncomment these two lines and comment out sendMediaBetween
    // above, if the bot ends up silent on the call):
    // call.sendMediaTo(ws, { encoding: WebSocketAudioEncoding.ULAW });
    // ws.sendMediaTo(call, { encoding: WebSocketAudioEncoding.ULAW });
  });

  ws.addEventListener(WebSocketEvents.CLOSE, function () {
    Logger.write("Telephony bridge WebSocket closed");
  });
  ws.addEventListener(WebSocketEvents.ERROR, function (e) {
    Logger.write("Telephony bridge WebSocket error: " + JSON.stringify(e));
  });

  var cleanup = function () {
    try {
      ws.close();
    } catch (e) {}
    VoxEngine.terminate();
  };
  call.addEventListener(CallEvents.Disconnected, cleanup);
  call.addEventListener(CallEvents.Failed, cleanup);

  return ws;
}
