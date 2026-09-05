from __future__ import annotations

import asyncio
import base64
import json
import os
import re

from fastapi import WebSocket
from starlette.websockets import WebSocketState

import llm_client
import session_base
import speech
import sheets_logger
import telephony_audio
from session_base import BaseSession, GREETING_SENTINEL
from telephony_vad import ServerVAD

# G.711 μ-law is always 8kHz for PSTN — this is a telephony-transport constant,
# not a Cartesia one, so it doesn't belong in cartesia_realtime.py alongside
# STT_SAMPLE_RATE.
TELEPHONY_SAMPLE_RATE = 8000

# 20ms of 8kHz G.711 μ-law (1 byte/sample) -- the chunk size Voximplant's
# media-stream docs recommend ("it is recommended that it be a multiple of
# 20 ms"), and the framing VoxEngine itself uses in the other direction.
TELEPHONY_FRAME_SAMPLES = 160
TELEPHONY_FRAME_BYTES = TELEPHONY_FRAME_SAMPLES

# Voximplant's ws->call media route speaks Voximplant.WebSocketFormat: JSON
# text frames with base64 payloads, NOT raw binary. Every previous attempt
# here sent raw binary (and one sent JSON without the required chunk/
# timestamp fields), which VoxEngine silently ignores -- the bytes leave us
# with no error, the socket stays open, and the caller hears nothing. That
# is the whole "confirmed sent, still silent" mystery. Normative schema:
# https://github.com/voximplant/protobuf/blob/main/websockets.proto
# (MediaEvent.timestamp and MediaEvent.chunk are `required`), documented at
# https://voximplant.com/docs/guides/media-streams/format
#
# A stream must open with a StartEvent naming the codec; the codec then
# cannot change, and MediaEvent payloads must match it exactly. The
# scenario's {encoding: ...} option on sendMediaTo does NOT set the format
# for this direction -- StartEvent does, which is why VoxEngine's session
# log always showed no encoding on the ws->call SendMedia command.
OUTBOUND_STREAM_ENCODING = "ULAW"

# VoxEngine buffers what we send and does the real-time playback itself
# ("it is not strictly necessary to adhere to real-time timing... You may
# send all audio chunks at once"), so the pacer no longer exists to make
# playback smooth -- only to respect the one hard limit on that buffer:
# "the maximum size of the WebSocket buffer is 10 seconds. If this limit is
# reached, any excess audio chunks are discarded." Cartesia generates far
# faster than real time and a long reply easily exceeds 10s of audio, so
# sending it all at once would silently truncate it. Kept low rather than
# just "safely under 10s" because this buffer is also the interruption
# penalty: on barge-in everything already handed over is dropped by
# clearMediaBuffer, but audio the caller is *currently* hearing can't be
# un-heard, so a deep buffer makes the bot slower to actually shut up.
# 1.5s is plenty to never starve playback and short enough to feel abrupt.
OUTBOUND_LOOKAHEAD_S = 1.5

# Fallback unmute timing: there's no client on this transport to report real
# playback completion the way the browser's "bot_playback_done" does (see
# BaseSession.pump_tts_audio's "done" handling, which only means Cartesia
# finished *generating* audio, not that Voximplant finished *playing* it into
# the call) — estimate playback duration from character count instead.
# CHARS_PER_SECOND is calibrated from one real call's greeting (~110
# characters, ~8.2s of audio). PLAYBACK_ESTIMATE_PAD_S covers
# buffering/network latency before audio actually reaches the caller —
# erring toward unmuting a bit late is far less disruptive than unmuting
# while the bot is still actually talking.
CHARS_PER_SECOND = 13.0
PLAYBACK_ESTIMATE_PAD_S = 1.5

# The browser transport's barge-in thresholds (settings_store.DEFAULT_VAD)
# are tuned for a mic signal that's already gone through getUserMedia's
# built-in echo cancellation, so the bot's own played-back audio is stripped
# out before VAD ever sees it. PSTN calls have no such AEC -- raw line audio
# reaches us as-is, including the far end's line/hybrid echo of our own
# greeting bouncing back. Confirmed live: at those thresholds the bot
# barge-in-cancelled its own greeting ~3s after playback started, well
# before any real caller speech was possible. Only barge-in needs raising --
# normal (not-bot-speaking) speech detection isn't exposed to this echo path.
#
# The false trigger above happened at ENERGY_THRESHOLD=0.035 -- line echo is
# heavily attenuated relative to direct near-end speech, so the much higher
# 0.15 here is the primary defense against it, not the duration. Confirmed
# live (2026-08): at 700ms, a real short reply ("алё") never registered at
# all -- the word itself often doesn't last 700ms of continuous above-
# threshold audio. Lowered to 400ms, still well above the browser
# transport's own 250ms barge-in default, to keep an extra safety margin
# against echo.
#
# 0.15 turned out to be past the other edge: measured against real speech run
# through this exact μ-law + resample path, it stops firing entirely at around
# -22 dBFS average speech level, which is an ordinary (not even quiet) PSTN
# level -- only near-clipping audio crosses it. So barge-in was effectively
# switched off, and with the unmute bug that used to accompany it (see
# _on_outbound_queue_drained) that left the caller unable to be heard at all.
# 0.08 is the compromise: still >2x the browser default that echo defeated,
# but reachable by a normally-loud caller.
TELEPHONY_BARGE_IN_ENERGY_THRESHOLD = 0.08
TELEPHONY_BARGE_IN_SPEECH_START_MS = 400

# The ORDINARY (not-bot-speaking) speech threshold needs its own telephony
# value for the same reason barge-in did: the browser's 0.02 assumes
# getUserMedia audio, which arrives normalised. Raw PSTN mu-law does not.
# Measured from one real call's own VAD samples, the caller's speech sat at
# 0.0006-0.028 with most of it between 0.005 and 0.016 -- so at 0.02 only
# the single loudest peak of a whole sentence crossed, VAD saw a ~150ms
# blip instead of an utterance, and forwarded ~170ms of audio to STT.
# Ink-Whisper returns nothing usable from that little (measured: 150-400ms
# bursts come back as bare "Продолжение", 3/3), so the caller's first reply
# was routinely lost. Asked to repeat, people speak up -- which is exactly
# why it "only worked the second time".
#
# 0.008 puts the bar under the bulk of that measured speech while staying
# ~8x above the line's own noise floor (0.0002 idle, up to ~0.001). It sits
# below the loudest echo tail seen after the bot stops (0.0141), so an echo
# can still trip it -- that costs one "не расслышал" at worst, which is a
# far cheaper failure than not hearing the caller at all.
TELEPHONY_ENERGY_THRESHOLD = 0.008

# How long the caller has to stay quiet before we decide they're done and
# start answering. Every millisecond here is dead air the caller is
# listening to, on top of the LLM's own thinking time, so it was set to
# 500ms on the reasoning that this "comfortably outlasts the pauses inside
# a normal sentence".
#
# It does not. Reported live (2026-08-31): the bot kept cutting in while
# the caller was still mid-sentence. An ordinary pause between phrases --
# or a moment's thinking -- runs past 500ms easily, and each one ended the
# utterance, sent a fragment to STT, and got nothing back. 800ms covers
# those pauses at the cost of 300ms more latency per turn, which is a much
# cheaper price than talking over the person you called.
TELEPHONY_SILENCE_HANGOVER_MS = 800

# Slack between "the last frame of this turn left the pacer" and treating the
# bot as done talking -- see _unmute_after_drain.
PLAYBACK_DRAIN_PAD_S = 0.4

# Same idea for the goodbye before hanging up, but a touch longer: cutting
# the line off mid-syllable reads as a dropped call, not a finished one.
HANGUP_PAD_S = 0.8


# How many empty transcripts in a row before the bot admits it didn't catch
# anything, and how many times per call it may do so -- see
# TelephonySession._on_empty_transcript.
#
# Was 2, on the reasoning that a single stray VAD trigger on line noise is
# common and answering it is worse than ignoring it. Confirmed live
# (2026-08-30) that the cost of waiting for a second miss is higher: the
# caller spoke, STT returned nothing, the bot stayed silent, and to the
# caller that is indistinguishable from "it can't hear me" -- which is
# exactly how it was reported. MAX_REPROMPTS_PER_CALL still caps a noisy
# line at three, so the downside of reacting to noise is bounded and the
# bot can never loop on it.
EMPTY_TRANSCRIPTS_BEFORE_REPROMPT = 1
MAX_REPROMPTS_PER_CALL = 3
# Deliberately genderless. This is a FixedLine -- it bypasses the LLM, so the
# persona prompt cannot regender it, and the previous wording ("не расслышал")
# stayed masculine after the operator switched to a female persona/voice. It
# was also the line callers heard most, so it read as "she always speaks in
# the masculine". Keep any future wording here free of gendered forms.
REPROMPT_LINE = "Простите, плохо слышно. Повторите, пожалуйста?"

# Said when the LLM request itself fails -- see _on_turn_failed. Deliberately
# vague about the cause and inviting the caller to repeat: by the time this
# plays the retry may well succeed.
TURN_FAILED_LINE = "Извините, у меня тут связь подвисла. Повторите, пожалуйста?"

# Hard ceiling on one outbound call. A real qualifying conversation takes
# well under a minute; anything running far past that is not converting.
# The case that forces this: a call to a lead whose phone answered with its
# own AI secretary, which asked our bot polite questions indefinitely --
# 13 turns and 184 seconds of billed line with no possible outcome
# (confirmed live 2026-08-28). Voicemail and IVR menus fail the same way.
MAX_CALL_DURATION_S = 150

# How long the line may sit silent -- bot not talking, caller not talking --
# before we hang up. This is the model-independent half of ending a call.
# The fast path is qualify_lead firing, but how reliably a model emits a tool
# call mid-conversation varies enormously and is not something a campaign
# should depend on: replaying one real call's history four times, measured
# gemini-3.7-flash at 4/4, gemini-3.6-flash at 2/4, gemini-3.5-flash-lite at
# 0/4 (and sharpening the prompt made 3.6 WORSE, 1/4 -- it isn't a wording
# problem). When the tool doesn't fire, the bot says its goodbye and both
# sides then sit there saying nothing, billed, until MAX_CALL_DURATION_S.
# 20 seconds is far longer than any natural pause in a live sales call.
IDLE_HANGUP_S = 20.0
IDLE_HANGUP_LINE = "Похоже, связь прервалась. Всего доброго!"
TIME_LIMIT_LINE = (
    "Не буду больше занимать время. Всю информацию передам администратору, "
    "он свяжется с вами. Всего доброго!"
)
# Backstop if that goodbye never finishes playing (nothing left to drain,
# TTS wedged): hang up anyway rather than keep the line open.
GOODBYE_GRACE_S = 15.0

QUALIFY_FALLBACK_PROMPT = (
    "Ниже расшифровка телефонного разговора: оператор звонил клиенту, чтобы пригласить его "
    "на мероприятие. Определи по расшифровке, чем закончился разговор, и вызови функцию "
    "qualify_lead.\n\n"
    "ready_to_buy: \"yes\" — клиент согласился прийти или попросил забронировать; \"no\" — "
    "отказался, не интересно, попросил больше не звонить; \"unclear\" — не дал ответа, "
    "бросил трубку, обещал подумать или разговор оборвался.\n"
    "summary: одно-два предложения — что предложили, что ответил клиент, сколько билетов, "
    "если называли."
)

# Live A/B testing (2026-08-30, across Gemini and multiple OpenAI models)
# found no model that reliably calls qualify_lead on a vague/non-committal
# ending ("ну ладно, посмотрим") -- the conversation just continues, or the
# model says a goodbye-sounding line without the actual tool call the
# hangup logic depends on. A real qualifying exchange resolves in well
# under this many turns; treat exceeding it as "the model isn't going to
# close this out on its own" and force a verdict from the transcript so
# far -- see TelephonySession._on_llm_turn_complete.
FORCE_QUALIFY_AFTER_TURNS = 6

# Second trigger for the same backstop: the bot said goodbye in words but
# never called qualify_lead, so nothing scheduled the hangup and the line
# stayed open on a paid PSTN minute. Seen on a real call 2026-09-05 -- six
# turns, a complete "Спасибо, что заинтересовались, до свидания!", zero tool
# calls all call, and the turn counter one short of firing.
#
# The plan for this backstop explicitly rejected a farewell heuristic on the
# reasoning that the turn counter covers both failure modes. It does not: a
# conversation that reaches its natural end in five turns never reaches the
# threshold. Anchored at the end of the reply so a farewell mentioned
# mid-sentence is not one.
_FAREWELL_RE = re.compile(
    r"(до свидания|всего доброго|всего хорошего|до встречи|хорошего дня|"
    r"хорошего вечера|доброго вечера|удачи)[\s.!?…»)\"]*$",
    re.IGNORECASE)
FORCE_QUALIFY_GOODBYE_LINE = (
    "Хорошо, спасибо за ответ. Всю информацию передам администратору, "
    "он свяжется с вами. Всего доброго!"
)


def _extract_media_payload(data: dict | None) -> bytes | None:
    """Pull raw μ-law bytes out of a Voximplant JSON media-event message
    ({"event":"media","media":{"payload":"<base64>"}})."""
    if not isinstance(data, dict) or data.get("event") != "media":
        return None
    payload_b64 = (data.get("media") or {}).get("payload")
    if not payload_b64:
        return None
    return base64.b64decode(payload_b64)


class TelephonySession(BaseSession):
    """Phone call (via Voximplant): inbound audio (caller -> us) is μ-law@8kHz
    bridged over a WebSocket Voximplant's VoxEngine scenario opens to us,
    decoded and fed into Cartesia STT the same way CallSession feeds its mic
    audio -- server-side VAD instead of mic-worklet.js, since there's no
    browser here.

    Outbound audio (bot -> caller) is synthesized locally with Cartesia, the
    same as CallSession/the browser transport (see BaseSession.pump_tts_audio
    and this class's _emit_tts_audio override below), then resampled to
    8kHz/μ-law and sent to VoxEngine as Voximplant.WebSocketFormat events --
    a StartEvent naming the codec, then one media event per 20ms frame (see
    OUTBOUND_STREAM_ENCODING and _pace_outbound_audio).
    bridge.voxengine.js routes that stream into the call via
    ws.sendMediaTo(call), the mirror image of call.sendMediaTo(ws, ...)
    which already carries the caller's audio to us.

    Note the asymmetry: VoxEngine sends us the caller's audio as plain
    binary frames, but will only accept audio in the other direction as
    these JSON events. Assuming the socket was symmetric is what made every
    earlier attempt here fail identically -- frames sent with no error, no
    close, no warning, and no sound.

    An earlier version of this class instead handed reply text to VoxEngine's
    own native Cartesia connector (Cartesia.createRealtimeTTSPlayer) and let
    it synthesize/play the audio itself -- that connector never played more
    than the first ~300ms of any utterance on a real call, independent of API
    key/model/voice/message shape. See bridge.voxengine.js's docstring for
    the full story."""

    # Deliberately NOT the same thing as bot_speaking. bot_speaking means "a
    # turn is in flight" and is set before the LLM call even starts; this means
    # "the caller is hearing bot audio right now". Gating the caller's
    # microphone on the former muted them for the whole ~1.7s the model spends
    # thinking -- speech arriving in that window was thrown away, and the only
    # way through was the anti-echo barge-in bar (0.08 RMS / 400ms), set that
    # high precisely to reject echo that does not exist while nothing is
    # playing. That was the "он меня не слышит" report.
    #
    # bot_speaking is a property purely to enforce the invariant
    # "_bot_audio_live implies bot_speaking" in one place. BaseSession clears
    # bot_speaking on paths telephony does not override (the LLM-failure path
    # and the fixed-line TTS-failure path, session_base.py); if one of those
    # ran after audio had started, a plain attribute would strand
    # _bot_audio_live at True and mute the caller for the rest of the call.
    @property
    def bot_speaking(self) -> bool:
        return self._bot_speaking

    @bot_speaking.setter
    def bot_speaking(self, value: bool) -> None:
        self._bot_speaking = value
        if not value:
            self._bot_audio_live = False

    def __init__(
        self,
        ws: WebSocket,
        call_id: str | None = None,
        phone_number: str | None = None,
        client_name: str | None = None,
    ):
        # Runs BaseSession.__init__, whose `self.bot_speaking = False` goes
        # through the setter above and creates both backing attributes.
        super().__init__(ws)
        self.call_id = call_id
        self.recorder.phone_number = phone_number
        self.recorder.client_name = client_name
        if client_name:
            self.history[0]["content"] += (
                f"\n\nИмя собеседника: {client_name}. Обратись по имени естественно, "
                f"не более 1-2 раз за разговор."
            )
        self.stt = None
        # Outbound campaign calls only ever qualify a lead -- the prompt
        # explicitly forbids confirm_booking on this scenario. Offering it
        # anyway isn't free: measured on the real prompt, a smaller/faster
        # model that reliably calls qualify_lead when it's the only tool
        # answers in plain speech instead when confirm_booking is also on the
        # table, silently losing the verdict the whole campaign exists to
        # collect. It also costs ~75 input tokens on every turn of every call.
        # Revisit if inbound booking calls come back.
        self.tools = [t for t in session_base.TOOLS if t["function"]["name"] == "qualify_lead"]
        vad_settings = dict(self.settings["vad"])
        vad_settings["energy_threshold"] = TELEPHONY_ENERGY_THRESHOLD
        vad_settings["barge_in_energy_threshold"] = TELEPHONY_BARGE_IN_ENERGY_THRESHOLD
        vad_settings["barge_in_speech_start_ms"] = TELEPHONY_BARGE_IN_SPEECH_START_MS
        vad_settings["silence_hangover_ms"] = TELEPHONY_SILENCE_HANGOVER_MS
        self.vad = ServerVAD(vad_settings, sample_rate=speech.STT_SAMPLE_RATE)
        # One per call, not per chunk: it carries filter state across chunk
        # boundaries (see Downsampler24kTo8k).
        self._downsampler = telephony_audio.Downsampler24kTo8k()
        self._inbound_chunks_received = 0
        self._outbound_chunks_sent = 0
        self._turn_char_count = 0
        self._unmute_task: asyncio.Task | None = None
        # Set once Cartesia has generated a turn's last audio chunk, cleared
        # when the pacer has drained that audio onto the wire -- together
        # they're this transport's stand-in for the browser's real
        # end-of-playback signal. See _on_outbound_queue_drained.
        self._generation_done_context: str | None = None
        # Event-loop clock reading for when the audio handed to VoxEngine so
        # far will have finished playing -- maintained by the pacer, read by
        # whichever of the two drain paths fires (see _on_tts_generation_done).
        self._playback_ends_at = 0.0
        # Set to the context_id of the turn that called qualify_lead; the
        # call hangs up once that turn's audio has played out.
        self._hangup_after_context: str | None = None
        # Set by the call-duration guard: hang up after whatever turn is
        # speaking next, whichever context it happens to get.
        self._hangup_after_any_turn = False
        self._reprompts_sent = 0
        # Turns since the call started (or since qualify_lead last reset
        # this) without qualify_lead firing -- see FORCE_QUALIFY_AFTER_TURNS
        # and _on_llm_turn_complete. Complements _last_caller_activity below:
        # that one catches dead air (nobody talking), this one catches a
        # conversation that stays active on both sides but never resolves.
        self._llm_turns_without_qualify = 0
        self._force_qualify_task: asyncio.Task | None = None
        # Last moment the caller was audibly doing something. Seeded in run()
        # so the idle watchdog measures from the start of the call, not from
        # the epoch. See _enforce_idle_timeout.
        self._last_caller_activity = 0.0
        # Cartesia synthesizes far faster than real time (confirmed live:
        # 2026-08-28, an ~8s greeting arrived in full within ~900ms of the
        # first chunk) -- pushing that straight to send_bytes floods the
        # call's real-time RTP leg with frames all at once instead of one
        # 20ms frame per 20ms of wall time, which VoxEngine's native
        # ws.sendMediaTo(call) route can't play back cleanly (heard live as
        # garbled crackle/rustle, not speech). This queue + _pace_outbound_audio
        # decouple synthesis speed from playback speed.
        self._outbound_queue: asyncio.Queue[bytes] = asyncio.Queue()

    async def run(self):
        self.voice_id = await speech.resolve_voice_id(self.settings["voice_id"])
        # Concurrently, not one after the other: on Yandex each is a separate
        # gRPC connection, and serialising them put their setup end to end in
        # front of the greeting.
        self.stt, self.tts = await asyncio.gather(speech.open_stt(), speech.open_tts())

        # Must precede any media event on this socket, so send it before the
        # greeting turn can start producing audio.
        await self._send_stream_start()
        self._last_caller_activity = asyncio.get_event_loop().time()

        tasks = [
            asyncio.create_task(self.pump_phone_audio()),
            asyncio.create_task(self.pump_stt_transcripts()),
            asyncio.create_task(self.pump_tts_audio()),
            asyncio.create_task(self._pace_outbound_audio()),
            asyncio.create_task(self._enforce_call_time_limit()),
            asyncio.create_task(self._enforce_idle_timeout()),
        ]
        # Bot speaks first, like answering a phone call -- same fixed,
        # operator-edited greeting as the browser transport (settings_store.py).
        self.launch_bot_turn(GREETING_SENTINEL)

        try:
            # The call ends as soon as any one pump exits (e.g. Voximplant
            # hangs up); gather() would hang forever waiting on the other
            # open Cartesia connections.
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc:
                    raise exc
        finally:
            if self._unmute_task and not self._unmute_task.done():
                self._unmute_task.cancel()
            if self._force_qualify_task and not self._force_qualify_task.done():
                self._force_qualify_task.cancel()
            for task in tasks:
                task.cancel()
            try:
                await self._qualify_from_transcript_if_missing()
            except Exception as exc:
                print(f"[telephony:{self.recorder.call_id}] fallback qualification failed: {exc}")
            for ws in (self.stt, self.tts):
                try:
                    await ws.close()
                except Exception:
                    pass
            try:
                await self.recorder.finalize()
            except Exception:
                pass
            try:
                await sheets_logger.append_call_row(self.recorder.sheet_summary())
            except Exception:
                pass

    def _transcript_lines(self) -> list[str]:
        lines = []
        for turn in self.recorder.turns:
            if turn.get("user_text"):
                lines.append(f"Клиент: {turn['user_text']}")
            if turn.get("bot_text"):
                lines.append(f"Оператор: {turn['bot_text']}")
        return lines

    async def _derive_qualification_from_transcript(self) -> dict | None:
        """One forced-tool_choice request against the transcript so far --
        the model cannot answer in prose this time. Shared by the teardown
        fallback and the mid-call forced-qualify backstop (see
        _on_llm_turn_complete) so the two paths can't drift apart."""
        if not any(turn.get("user_text") for turn in self.recorder.turns):
            return None  # nobody said anything back -- nothing to qualify
        response = await llm_client.get_client().chat.completions.create(
            model=llm_client.model_name(),
            messages=[
                {"role": "system", "content": QUALIFY_FALLBACK_PROMPT},
                {"role": "user", "content": "\n".join(self._transcript_lines())},
            ],
            tools=self.tools,
            tool_choice={"type": "function", "function": {"name": "qualify_lead"}},
            **llm_client.completion_kwargs(reply_tokens=400, reasoning_effort=None),
        )
        calls = response.choices[0].message.tool_calls or []
        if not calls:
            return None
        return json.loads(calls[0].function.arguments or "{}")

    async def _qualify_from_transcript_if_missing(self) -> None:
        """Last line of defence for the one number the campaign is actually
        for. In-call tool calling is not guaranteed: the caller can hang up
        mid-sentence, and models differ wildly at it -- measured on one real
        call's own history, gemini-3.7-flash emitted the tool call 4 times
        out of 4 while gemini-3.5-flash-lite did so 0 times out of 4, once
        even reading the words "типа вызов функции" out loud to the caller
        instead. A verdict lost that way is unrecoverable, since call
        history here lives only in memory.

        So if the call ended without one, derive it from the transcript.
        Runs during teardown, after the sockets are gone, so it costs the
        caller nothing -- see _on_llm_turn_complete for the mid-call
        counterpart that can still end the call gracefully."""
        if self.recorder.outcome or not self.recorder.turns:
            return
        args = await self._derive_qualification_from_transcript()
        if args is None:
            return
        self.recorder.record_tool_call("", "qualify_lead", args)
        print(f"[telephony:{self.recorder.call_id}] qualified from transcript after the call: "
              f"{args.get('ready_to_buy')}")

    async def send_json(self, payload: dict) -> None:
        """No UI is listening on this transport, but every event still gets
        logged -- silently discarding "error" events here would be exactly
        the kind of swallowed failure this whole codebase deliberately never
        does."""
        print(f"[telephony:{self.recorder.call_id}] {payload}")

    async def _send_control(self, payload: dict) -> None:
        if self.ws.client_state == WebSocketState.CONNECTED:
            await self.ws.send_json(payload)

    async def _send_tts_text(self, text: str, context_id: str, keep_open: bool) -> None:
        """BaseSession's hook synthesizes text via self.tts same as the
        browser transport. On top of that, telephony has no client to report
        real playback completion (see CHARS_PER_SECOND above), so arm the
        character-count backstop here, at the same point the old
        VoxEngine-control-message version used to."""
        await super()._send_tts_text(text, context_id, keep_open)
        self._turn_char_count += len(text)
        if not keep_open:
            self._schedule_unmute_fallback(context_id, self._turn_char_count)
            self._turn_char_count = 0

    def _schedule_unmute_fallback(self, context_id: str, char_count: int) -> None:
        if self._unmute_task and not self._unmute_task.done():
            self._unmute_task.cancel()
        estimated_s = char_count / CHARS_PER_SECOND + PLAYBACK_ESTIMATE_PAD_S
        print(f"[telephony:{self.recorder.call_id}] unmute backstop armed: {estimated_s:.1f}s "
              f"for {char_count} chars")
        self._unmute_task = asyncio.create_task(self._unmute_after_estimate(context_id, estimated_s))

    async def _unmute_after_estimate(self, context_id: str, estimated_s: float) -> None:
        await asyncio.sleep(estimated_s)
        # Stale if a barge-in or a newer turn already moved active_context_id
        # on -- this fallback only fires for a turn that finished naturally,
        # uninterrupted.
        if context_id == self.active_context_id:
            print(f"[telephony:{self.recorder.call_id}] unmuting via char-count backstop "
                  f"(the outbound queue never drained -- pacer stalled?)")
            self._unmute(context_id)

    def _unmute(self, context_id: str) -> None:
        if context_id != self.active_context_id or not self.bot_speaking:
            return
        self.bot_speaking = False  # clears _bot_audio_live too, see the property
        self.vad.set_bot_speaking(False)
        # Worth a permanent line in the log: until this fires, every caller
        # chunk is gated out of VAD/STT, so any "the bot can't hear me"
        # report is answered by whether this appeared and when. Its absence
        # was the whole difficulty in diagnosing one such report
        # (2026-08-30).
        print(f"[telephony:{self.recorder.call_id}] unmuted (bot_speaking=False) for context {context_id}")

    async def _on_tts_generation_done(self, context_id: str) -> None:
        """Cartesia has generated the last chunk of this turn, so whatever is
        still sitting in _outbound_queue is now the entire rest of the turn --
        which makes the queue draining a real end-of-playback signal rather
        than an estimate. Normally _pace_outbound_audio notices the drain.

        On a short reply the pacer can empty the queue BEFORE this arrives,
        though, and then there's no further frame to notice anything on -- so
        check here too, or that turn silently falls back to the character-count
        estimate and stays muted for far too long (seen live 2026-08-28 on a
        72-character reply: 7s of mute for ~2s of speech)."""
        self._generation_done_context = context_id
        if self._outbound_queue.empty():
            self._on_outbound_queue_drained(self._playback_ends_at)

    async def _on_turn_failed(self, context_id: str, tts_started: bool) -> None:
        """A failed LLM request used to end as dead air: the turn aborted,
        the caller heard nothing, and the line stayed open (and billed) until
        they gave up. At campaign concurrency this is not an edge case --
        the router allows 10 requests/minute and a single call spends 3-5 of
        them, so parallel calls collide by design.

        Say something human instead. Skipped if audio for this turn had
        already started, since then the caller heard a sentence and a stray
        apology on top of it would be more confusing than the gap."""
        if tts_started:
            return
        print(f"[telephony:{self.recorder.call_id}] turn failed -- apologising to the caller")
        self.launch_bot_turn(session_base.FixedLine(TURN_FAILED_LINE))

    async def _on_empty_transcript(self, consecutive: int) -> None:
        """The caller said something and Cartesia gave us back nothing --
        short utterances over a narrowband phone line are the usual cause.
        Staying silent is the worst possible response: to the caller the bot
        has simply stopped answering, and they keep repeating themselves into
        the void (observed live: three attempts ignored across ~20 seconds).

        Ask them to repeat, like a person would -- but never over someone
        who is mid-sentence. VAD ends an utterance after a
        TELEPHONY_SILENCE_HANGOVER_MS pause, which a caller can easily
        exceed while still thinking; the fragment before that pause often
        transcribes to nothing, and the old check (bot_speaking only) then
        let the bot talk straight over them. Reported live as "it keeps
        saying it didn't catch me while I'm still talking". If VAD says the
        caller is speaking right now, they are answering already -- stay
        quiet and let the next utterance come in.

        Capped so a persistently noisy line can't turn into a loop of the
        bot asking again and again."""
        if consecutive < EMPTY_TRANSCRIPTS_BEFORE_REPROMPT or self.bot_speaking:
            return
        if self.vad.speaking:
            print(f"[telephony:{self.recorder.call_id}] empty transcript, but the caller is "
                  f"still speaking -- not interrupting")
            return
        if consecutive % EMPTY_TRANSCRIPTS_BEFORE_REPROMPT != 0:
            return
        if self._reprompts_sent >= MAX_REPROMPTS_PER_CALL:
            return
        self._reprompts_sent += 1
        print(f"[telephony:{self.recorder.call_id}] {consecutive} empty transcripts in a row "
              f"-- asking the caller to repeat ({self._reprompts_sent}/{MAX_REPROMPTS_PER_CALL})")
        self.launch_bot_turn(session_base.FixedLine(REPROMPT_LINE))

    async def _on_tool_call(self, context_id: str, name: str, args: dict) -> None:
        """qualify_lead is by definition the last thing that happens on an
        outbound call: the verdict is recorded and there is nothing left to
        ask. Previously the bot said its goodbye and then just sat on the
        line waiting for a caller who had already hung up mentally, burning
        per-minute PSTN charges on every single lead. Mark the call to end
        as soon as that goodbye has finished playing."""
        if name == "qualify_lead":
            self._hangup_after_context = context_id

    async def _on_llm_turn_complete(self, context_id: str) -> None:
        """Backstop for the gap live A/B testing found in every model tried
        (2026-08-30): none reliably calls qualify_lead on a vague, non-
        committal ending -- the conversation just continues, or the model
        says a goodbye-sounding line without the tool call _on_tool_call
        actually depends on to schedule a hangup. FORCE_QUALIFY_AFTER_TURNS
        real LLM turns without a verdict (never counts the greeting or a
        reprompt -- see where this hook is called) is treated as "this
        conversation isn't going to close itself" and forces one from the
        transcript so far, deliberately coarse rather than trying to guess
        intent from the caller's exact wording (that guessing is the thing
        that already failed across every model tested)."""
        if self.recorder.outcome is not None:
            return  # already qualified live -- nothing to backstop
        self._llm_turns_without_qualify += 1
        last_reply = self.recorder.turns[-1].get("bot_text", "") if self.recorder.turns else ""
        said_goodbye = bool(_FAREWELL_RE.search((last_reply or "").strip()))
        if not said_goodbye and self._llm_turns_without_qualify < FORCE_QUALIFY_AFTER_TURNS:
            return
        if self._force_qualify_task and not self._force_qualify_task.done():
            return  # already forcing one
        if said_goodbye:
            # Set synchronously, exactly as the real qualify_lead path does in
            # _on_tool_call, and NOT left to the background task below: that
            # task first derives a verdict from the transcript, which is
            # another LLM round-trip. A short goodbye can finish playing
            # before it returns, and _on_outbound_queue_drained would then
            # read the flag as still unset and keep the line open -- the very
            # bug this is here to fix.
            self._hangup_after_context = context_id
        reason = ("the bot said goodbye without calling qualify_lead" if said_goodbye
                  else f"{self._llm_turns_without_qualify} LLM turns without qualify_lead")
        print(f"[telephony:{self.recorder.call_id}] {reason} -- forcing a verdict and hanging up")
        self._force_qualify_task = asyncio.create_task(
            self._force_qualify_and_hangup(speak_goodbye=not said_goodbye))

    async def _force_qualify_and_hangup(self, speak_goodbye: bool = True) -> None:
        """Runs as a detached task, not awaited inline from
        _on_llm_turn_complete: that hook fires as the last line of the
        bot_turn() call still holding turn_lock for this very turn, and
        launch_bot_turn() below would cancel self.bot_turn_task -- i.e. the
        task currently running this code -- which is a reentrancy bug.
        Detaching (same pattern as _enforce_call_time_limit) avoids that."""
        try:
            args = await self._derive_qualification_from_transcript()
        except Exception as exc:
            print(f"[telephony:{self.recorder.call_id}] forced mid-call qualification failed: {exc}")
            return
        if args is None or self.recorder.outcome is not None or self._hangup_after_any_turn:
            # Nothing derivable, the live call qualified itself meanwhile, or
            # _enforce_idle_timeout already started ending the call while
            # this (slower) derivation was in flight -- don't also try to
            # speak into a call that's already wrapping up.
            return
        self.recorder.record_tool_call("", "qualify_lead", args)
        print(f"[telephony:{self.recorder.call_id}] forced mid-call qualification after "
              f"{self._llm_turns_without_qualify} turns: {args.get('ready_to_buy')}")
        self._hangup_after_any_turn = True
        if speak_goodbye:
            self.launch_bot_turn(session_base.FixedLine(FORCE_QUALIFY_GOODBYE_LINE))
        else:
            # Triggered BY a goodbye the bot just said -- saying another one
            # would have the caller hear two farewells in a row. The line ends
            # once the reply already playing finishes; see _on_tts_generation_done
            # and the _hangup_after_any_turn check in _unmute_after_drain.
            print(f"[telephony:{self.recorder.call_id}] verdict recorded; hanging up after the "
                  f"goodbye already playing")

    def _on_outbound_queue_drained(self, playback_ends_at: float) -> None:
        """Called when the pacer has handed VoxEngine the last frame of a turn
        whose generation already finished -- i.e. the whole remaining reply is
        now sitting in VoxEngine's playback buffer, and playback_ends_at (an
        event-loop clock reading) is when it will finish coming out of it.

        This replaces the character-count estimate as the primary unmute.
        That estimate silently muted whole calls whenever a reply ran long: a
        380-character greeting estimated 30.8s of playback, so on an
        18-second call bot_speaking never cleared and every single caller
        chunk was gated out -- which is exactly why the bot appeared not to
        hear the caller at all (confirmed from production logs, 2026-08-28).
        The estimate stays on only as a backstop for a stalled pacer."""
        context_id = self._generation_done_context
        if context_id is None:
            return
        self._generation_done_context = None
        if self._unmute_task and not self._unmute_task.done():
            self._unmute_task.cancel()
        if context_id == self._hangup_after_context or self._hangup_after_any_turn:
            self._unmute_task = asyncio.create_task(self._hangup_after_playback(playback_ends_at))
        else:
            self._unmute_task = asyncio.create_task(self._unmute_after_drain(context_id, playback_ends_at))

    async def _enforce_idle_timeout(self) -> None:
        """Ends a call that has gone quiet in both directions -- see
        IDLE_HANGUP_S. Deliberately does not consult the model: this is the
        path that has to work when the model declined to call qualify_lead.

        Time spent with the bot talking, or with the caller talking, is not
        idle: the caller is listening, or being listened to."""
        while True:
            await asyncio.sleep(2.0)
            if self.bot_speaking or self._hangup_after_any_turn:
                self._last_caller_activity = asyncio.get_event_loop().time()
                continue
            idle_for = asyncio.get_event_loop().time() - self._last_caller_activity
            if idle_for < IDLE_HANGUP_S:
                continue
            print(f"[telephony:{self.recorder.call_id}] line idle for {idle_for:.0f}s "
                  f"-- hanging up")
            self._hangup_after_any_turn = True
            self.launch_bot_turn(session_base.FixedLine(IDLE_HANGUP_LINE))
            await asyncio.sleep(GOODBYE_GRACE_S)
            await self._send_control({"type": "hangup"})
            return

    async def _enforce_call_time_limit(self) -> None:
        """Wraps the call up once it has run past MAX_CALL_DURATION_S -- see
        that constant for the failure this exists for. Says a normal-sounding
        goodbye first rather than cutting the line dead, since the other end
        may well be a person who simply talks a lot."""
        await asyncio.sleep(MAX_CALL_DURATION_S)
        print(f"[telephony:{self.recorder.call_id}] call hit the {MAX_CALL_DURATION_S}s limit "
              f"-- wrapping up")
        self._hangup_after_any_turn = True
        self.launch_bot_turn(session_base.FixedLine(TIME_LIMIT_LINE))
        await asyncio.sleep(GOODBYE_GRACE_S)
        print(f"[telephony:{self.recorder.call_id}] hanging up on the time limit")
        await self._send_control({"type": "hangup"})

    async def _hangup_after_playback(self, playback_ends_at: float) -> None:
        delay = playback_ends_at - asyncio.get_event_loop().time() + HANGUP_PAD_S
        if delay > 0:
            await asyncio.sleep(delay)
        print(f"[telephony:{self.recorder.call_id}] qualified -- hanging up")
        await self._send_control({"type": "hangup"})

    async def _unmute_after_drain(self, context_id: str, playback_ends_at: float) -> None:
        # The frames are on the wire well before the caller hears them (the
        # pacer deliberately runs OUTBOUND_LOOKAHEAD_S ahead), so wait out the
        # buffered audio, plus a pad: unmuting right on the last frame would
        # let the tail of the bot's own voice echo back in as caller speech.
        delay = playback_ends_at - asyncio.get_event_loop().time() + PLAYBACK_DRAIN_PAD_S
        if delay > 0:
            await asyncio.sleep(delay)
        self._unmute(context_id)

    async def _emit_tts_audio(self, pcm: bytes) -> None:
        """Override of BaseSession's hook: low-pass and resample the
        synthesized PCM16@24kHz chunk down to 8kHz, encode to G.711 μ-law,
        and slice it into 20ms frames for the pacer to hand to VoxEngine as
        Voximplant.WebSocketFormat media events (see
        OUTBOUND_STREAM_ENCODING).

        The filtering is not optional politeness -- decimating 3:1 without it
        folds every sibilant back into the speech band at full amplitude. See
        Downsampler24kTo8k.

        Only enqueued here, not sent: Cartesia generates far faster than
        real time, and VoxEngine's playback buffer tops out at 10 seconds
        of audio with the excess silently discarded, so _pace_outbound_audio
        is what meters it out."""
        pcm_8k = self._downsampler.process(pcm)
        ulaw = telephony_audio.pcm16_to_ulaw(pcm_8k)
        # First audio of the turn: only now does the caller actually start
        # hearing the bot, so only now do the anti-echo thresholds and the
        # microphone gate make sense. Set slightly early (the frames still have
        # to reach VoxEngine), which is the safe direction -- it can only make
        # us more conservative about echo, never less.
        self._bot_audio_live = True
        self._outbound_chunks_sent += 1
        if self._outbound_chunks_sent == 1:
            print(f"[telephony:{self.recorder.call_id}] first outbound TTS audio ready to send "
                  f"({len(ulaw)} bytes ulaw, sliced into {TELEPHONY_FRAME_BYTES}-byte frames)")
        for offset in range(0, len(ulaw), TELEPHONY_FRAME_BYTES):
            frame = ulaw[offset:offset + TELEPHONY_FRAME_BYTES]
            await self._outbound_queue.put(frame)

    async def _send_stream_start(self) -> None:
        """Open the media stream. Required first: "The first event in the
        media stream should be a StartEvent... If the StartEvent is correctly
        formed, a WebSocketEvents.MEDIA_STARTED event is triggered in the
        scenario" -- bridge.voxengine.js reports that back to us as a diag,
        so its absence in the logs means this frame was malformed."""
        await self._send_control({
            "event": "start",
            "sequenceNumber": 0,
            "start": {"mediaFormat": {"encoding": OUTBOUND_STREAM_ENCODING, "sampleRate": TELEPHONY_SAMPLE_RATE}},
        })

    async def _pace_outbound_audio(self) -> None:
        """Drains self._outbound_queue to VoxEngine as media events, keeping
        roughly OUTBOUND_LOOKAHEAD_S of audio buffered ahead of real time --
        enough that VoxEngine always has something to play, far enough from
        its 10-second buffer ceiling that nothing gets discarded.

        next_send tracks when the frame being sent will actually be *heard*,
        which is what makes it (not wall-clock "now") the right basis for
        deciding when the bot has stopped talking. It's reset rather than
        left to drift after an idle gap longer than the lookahead -- between
        turns, or while the caller is talking -- so a long pause doesn't
        leave a backlog that would dump all at once when audio resumes."""
        frame_interval = TELEPHONY_FRAME_SAMPLES / TELEPHONY_SAMPLE_RATE
        next_send: float | None = None
        frames_sent = 0
        frames_dropped = 0
        # Both counters are per-stream and must be monotonic across the whole
        # call: chunk is the RTP sequence number, timestamp the running sample
        # count ("the sum of the samples in the preceding audio chunks").
        chunk = 0
        timestamp = 0
        while True:
            frame = await self._outbound_queue.get()
            now = asyncio.get_event_loop().time()
            if next_send is None or now - next_send > frame_interval + OUTBOUND_LOOKAHEAD_S:
                next_send = now - OUTBOUND_LOOKAHEAD_S
            elif next_send > now:
                await asyncio.sleep(next_send - now)
            if self.ws.client_state == WebSocketState.CONNECTED:
                try:
                    await self._send_control({
                        "event": "media",
                        "sequenceNumber": chunk,
                        "media": {
                            "timestamp": timestamp,
                            "chunk": chunk,
                            "payload": base64.b64encode(frame).decode("ascii"),
                        },
                    })
                except Exception as exc:
                    # Silent WS-send failures here would be indistinguishable
                    # from "nothing to send" -- this is exactly the gap that
                    # let a real call go completely silent with no error
                    # anywhere in the logs (2026-08-28, session 5007264702).
                    print(f"[telephony:{self.recorder.call_id}] _pace_outbound_audio: "
                          f"send failed after {frames_sent} frames sent: {exc!r}")
                    raise
                chunk += 1
                timestamp += len(frame)
                frames_sent += 1
                if frames_sent == 1 or frames_sent % 50 == 0:
                    print(f"[telephony:{self.recorder.call_id}] {frames_sent} outbound audio "
                          f"frames sent to Voximplant so far")
            else:
                frames_dropped += 1
                if frames_dropped == 1 or frames_dropped % 50 == 0:
                    print(f"[telephony:{self.recorder.call_id}] _pace_outbound_audio: dropped "
                          f"{frames_dropped} frames so far -- ws.client_state is "
                          f"{self.ws.client_state!r}, not CONNECTED")
            next_send += frame_interval
            # next_send is where this frame sits on the *sending* schedule,
            # which deliberately runs OUTBOUND_LOOKAHEAD_S ahead of what the
            # caller is hearing -- so the audio handed over so far finishes
            # playing that much later. Forgetting to add it back made
            # bot_speaking clear seconds before the bot actually stopped
            # talking, and in that window the caller's speech wasn't treated
            # as a barge-in at all: no cancel, no buffer clear, the bot just
            # talked over them. That was the "can't interrupt it" bug.
            self._playback_ends_at = next_send + OUTBOUND_LOOKAHEAD_S
            if self._outbound_queue.empty():
                self._on_outbound_queue_drained(self._playback_ends_at)

    def _clear_outbound_queue(self) -> None:
        """Barge-in: drop whatever's still queued from the interrupted turn
        so it doesn't keep draining out after the caller's already been
        told (via tts_cancel/ws.clearMediaBuffer) that playback stopped."""
        while not self._outbound_queue.empty():
            try:
                self._outbound_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def pump_phone_audio(self):
        while True:
            message = await self.ws.receive()
            if message["type"] == "websocket.disconnect":
                return

            audio_bytes = message.get("bytes")
            if audio_bytes is None:
                text = message.get("text")
                try:
                    data = json.loads(text) if text is not None else None
                except ValueError:
                    data = None

                if isinstance(data, dict) and data.get("type") == "diag":
                    # Piggybacked from bridge.voxengine.js -- VoxEngine's own
                    # Logger.write output isn't reachable from here (needs a
                    # browser-session login to fetch), so the scenario reports
                    # its own media-routing lifecycle over this same
                    # WebSocket instead.
                    print(f"[telephony:{self.recorder.call_id}] [voxengine diag] {data}")
                    continue

                audio_bytes = _extract_media_payload(data)
                if audio_bytes is None:
                    # Logged in full: confirmed live these are "start"
                    # ({"event":"start","start":{"mediaFormat":{"encoding":
                    # "ULAW","sampleRate":8000}}}) and "stop" ({"event":"stop",
                    # "stop":{"mediaInfo":{"bytesSent":N,"duration":N}}}) --
                    # neither carries a streamSid or anything else actionable,
                    # so there's nothing to do here beyond this log line.
                    print(f"[telephony:{self.recorder.call_id}] non-media event from Voximplant: {text!r}")
                    continue

            self._inbound_chunks_received += 1
            if self._inbound_chunks_received == 1:
                print(f"[telephony:{self.recorder.call_id}] first inbound audio chunk received "
                      f"({len(audio_bytes)} bytes) -- Voximplant->backend audio direction is alive")
            elif self._inbound_chunks_received % 50 == 0:
                print(f"[telephony:{self.recorder.call_id}] {self._inbound_chunks_received} inbound "
                      f"audio chunks received so far")

            # Edge case (accepted, not fixed -- the browser transport has the
            # exact same gap in mic-worklet.js's equivalent state machine):
            # ServerVAD only evaluates barge_in at the silence->speech
            # transition. If the caller is *already* mid-utterance in the
            # narrow window before audio starts, that speech never gets flagged
            # as a barge-in and gets silently dropped until it ends and a fresh
            # speech_start fires. Confirmed via a live test forcing this exact
            # race; interrupting anywhere else mid-turn (the realistic case)
            # works correctly -- also confirmed live.
            self.vad.set_bot_speaking(self._bot_audio_live)
            pcm_8k = telephony_audio.ulaw_to_pcm16(audio_bytes)
            pcm_16k = telephony_audio.resample_linear(pcm_8k, TELEPHONY_SAMPLE_RATE, speech.STT_SAMPLE_RATE)
            event = self.vad.process(pcm_16k)
            if event.speech_start:
                # Counts as activity even if STT later returns nothing for it:
                # someone audibly trying to talk is the opposite of an idle
                # line, and cutting them off would be the worst possible read.
                self._last_caller_activity = asyncio.get_event_loop().time()
                print(f"[telephony:{self.recorder.call_id}] VAD: speech_start (barge_in={event.barge_in})")
            if event.speech_end:
                print(f"[telephony:{self.recorder.call_id}] VAD: speech_end")

            # Note this is speech_start + "a turn is in flight", not
            # event.barge_in (which is the strictly narrower "started talking
            # over audio that is already playing"). The extra case is the
            # caller talking while the model is still thinking: nothing is
            # playing, so it is not a barge-in, but the turn being computed is
            # now answering a stale prompt and has to go the same way. Without
            # this the bot would start speaking over a caller who is already
            # mid-sentence, then get cancelled a second later anyway when their
            # transcript arrived.
            if event.speech_start and self.bot_speaking:
                # Cancel the in-flight turn and tell VoxEngine to stop playback
                # right away (the media route keeps forwarding whatever's
                # already buffered on the call leg unless we explicitly tell it
                # to stop). Mirrors CallSession's "interrupt" control message.
                stale_context_id = self.active_context_id
                self.active_context_id = None  # any late chunk/done for the old turn is now stale
                if self.bot_turn_task and not self.bot_turn_task.done():
                    self.bot_turn_task.cancel()
                if self._unmute_task and not self._unmute_task.done():
                    self._unmute_task.cancel()
                self.bot_speaking = False  # clears _bot_audio_live too, see the property
                self.vad.set_bot_speaking(False)
                self._clear_outbound_queue()
                # The cancelled turn's characters never got their closing
                # (keep_open=False) flush, which is the only place this
                # counter is normally reset -- leaving them to be added to
                # the NEXT turn's count, inflating every later playback
                # estimate monotonically once a call has been interrupted.
                self._turn_char_count = 0
                self._generation_done_context = None
                if stale_context_id is not None:
                    await self._send_control({"type": "tts_cancel", "context_id": stale_context_id})

            # Gated on audio actually playing, not on a turn being in flight:
            # while the model is thinking there is nothing for the caller to
            # talk over, so there is no reason to be deaf to them.
            if not self._bot_audio_live:
                for chunk in event.forward_chunks:
                    await self.stt.send_audio(chunk)
                    self.recorder.record_mic_chunk(chunk)

                if event.speech_end:
                    # Marked here, not on flush_done in pump_stt_transcripts:
                    # this is the moment the caller was actually done talking
                    # (VAD hangover already elapsed) -- marking it later, after
                    # the STT finalize round-trip, would exclude that round-trip
                    # from the logged latency numbers.
                    self.recorder.mark_speech_end()
                    await self.stt.end_utterance()
