// Inbound call scenario. In the Voximplant Control Panel scenario editor,
// paste bridge.voxengine.js's contents FIRST, then this file's contents
// below it, into one scenario (VoxEngine scenarios are single files).
// Attach this scenario to the routing rule bound to your rented number.

VoxEngine.addEventListener(AppEvents.CallAlerting, function (e) {
  var call = e.call;
  call.answer();
  call.addEventListener(CallEvents.Connected, function () {
    createTelephonyBridge(call);
  });
});
