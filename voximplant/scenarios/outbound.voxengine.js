// Outbound call scenario. In the Voximplant Control Panel scenario editor,
// paste bridge.voxengine.js's contents FIRST, then this file's contents
// below it, into one scenario (VoxEngine scenarios are single files).
// Attach this scenario to a routing rule with NO number pattern -- it's only
// ever started via the Management API (backend/voximplant_client.py's
// StartScenarios call, triggered by POST /api/telephony/call), never by an
// inbound number match.

// Must match VOXIMPLANT_CALLER_ID in the backend's .env (the rented number).
var CALLER_ID = "+70000000000";

VoxEngine.addEventListener(AppEvents.Started, function () {
  var customData = {};
  try {
    customData = JSON.parse(VoxEngine.customData() || "{}");
  } catch (e) {
    Logger.write("Bad script_custom_data: " + VoxEngine.customData());
  }

  var destination = customData.destination;
  if (!destination) {
    Logger.write("No destination in script_custom_data, terminating");
    VoxEngine.terminate();
    return;
  }

  var call = VoxEngine.callPSTN(destination, CALLER_ID);
  call.addEventListener(CallEvents.Connected, function () {
    createTelephonyBridge(call);
  });
  call.addEventListener(CallEvents.Failed, function (e) {
    Logger.write("Outbound call to " + destination + " failed: " + JSON.stringify(e));
    VoxEngine.terminate();
  });
});
