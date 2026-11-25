# -*- coding: utf-8 -*-
"""古村巡游字幕后端的行为测试。

覆盖任务说明中的关键不变量：
* 跳段隔离：跳过节目的字幕只作备选，不按时间相近补进后面的节目；
* 校订作用域：只推进对应语言/段落，共享字幕的另一语言独立核定不被连带；
* 播出事实不可变、播放位置单调，迟报不能把已越过的字幕推回屏幕；
* fetched/queued 未 shown 的内容可安全替换；
* 译文不齐时按渠道保障约定走 brief/delay/notice；
* 回执幂等、lost_reply 可安全重试，无反馈远端不算播出也不阻塞其他屏；
* 三种查询视图（导播看板 / 审校回看 / 播放点位置与交接）。
"""

import contextlib
import io
import json
import unittest
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread

from backend import (
    BackendError, ControlRoom, NoCounterpartyExample, ReceiptConflict,
    ReceiptGatewayClient, build_server, publish_current_cue, safe_reference,
)
from fixture_gateway import FixtureGateway

FIXTURES = str(Path(__file__).resolve().parent / "fixtures")


def fixture_room(replay=True):
    room = ControlRoom.from_fixtures(FIXTURES)
    if replay:
        room.replay_events(FIXTURES)
    return room


# --------------------------------------------------------------------------- #
# 合成台账，用于边界规则
# --------------------------------------------------------------------------- #
def base_registers(channels=None, captions=None, script=None, extra=None):
    data = {
        "script": script if script is not None else [
            {"cue": "alpha", "languages": ["zh"], "expected_minute": 7},
            {"cue": "beta", "languages": ["zh"], "expected_minute": 8},
        ],
        "channels": channels or {"live": {"lang": "zh", "can_hold": False}},
        "caption_versions": captions or [],
    }
    if extra:
        data.update(extra)
    return data


class FixtureScenarioTest(unittest.TestCase):
    """用仓库自带 6 条合成消息验证完整业务场景。"""

    def setUp(self):
        self.room = fixture_room()

    def test_actual_cue_and_skip_recorded(self):
        self.assertEqual(self.room.current_cue(), "courtyard-talk")
        self.assertEqual(self.room.cues["interlude"]["status"], "skipped")
        self.assertEqual(self.room.cues["courtyard-talk"]["anchor_ms"], 420000)
        self.assertEqual(self.room.board()["skipped_cues"], ["interlude"])

    def test_skipped_caption_stays_standby_and_not_reattached(self):
        rec = self.room.captions[("interlude", "local", 2, "")]
        self.assertEqual(rec["state"], "queued")
        # courtyard 的 local head 来自显式 cue 匹配的 rev2，与 interlude 无关。
        head = self.room.approved_head("courtyard-talk", "local")
        self.assertEqual(head["rev"], 2)
        self.assertEqual(head["text"], "西院开唱")
        self.assertIsNone(self.room.approved_head("interlude", "zh"))
        resolution = self.room.resolve_cue("interlude", "museum-delay")
        self.assertEqual(resolution["action"], "standby")

    def test_revision_is_language_scoped(self):
        local = self.room.approved_head("courtyard-talk", "local")
        zh = self.room.approved_head("courtyard-talk", "zh")
        self.assertEqual((local["rev"], local["text"]), (2, "西院开唱"))
        self.assertEqual((zh["rev"], zh["text"]), (1, "演出将在西院开始"))

    def test_playback_facts_and_claims(self):
        # museum 只领到 rev1，未显示；plaza 已显示 zh rev1。
        museum_claims = [c for c in self.room.claims.values()
                         if c["screen"] == "museum-delay"]
        self.assertEqual(len(museum_claims), 1)
        self.assertTrue(museum_claims[0]["fetched"])
        self.assertFalse(museum_claims[0]["shown"])
        self.assertEqual(len(self.room.air_facts), 1)
        fact = self.room.air_facts[0]
        self.assertEqual((fact["screen"], fact["cue"], fact["rev"]),
                         ("plaza-live", "courtyard-talk", 1))

    def test_director_board(self):
        board = self.room.board()
        self.assertEqual(board["current_cue"], "courtyard-talk")
        museum = board["channels"]["museum-delay"]["claim"]
        self.assertTrue(museum["update_available"])
        self.assertTrue(museum["modifiable"])
        plaza = board["channels"]["plaza-live"]["claim"]
        self.assertTrue(plaza["shown"])
        self.assertFalse(plaza["modifiable"])

    def test_replay_editions(self):
        editions = {(e["cue"], e["lang"]): e for e in self.room.replay_editions()}
        self.assertEqual(editions[("courtyard-talk", "local")]["replay_rev"], 2)
        self.assertEqual(editions[("courtyard-talk", "local")]["usage"], "replay_corrected")
        zh = editions[("courtyard-talk", "zh")]
        self.assertEqual(zh["replay_rev"], 1)
        self.assertEqual(zh["usage"], "replay_as_aired")
        self.assertEqual(zh["aired_revs"], [1])
        self.assertEqual(editions[("interlude", "local")]["usage"], "standby")

    def test_museum_continuation_wants_safe_replace(self):
        pos = self.room.channel_position("museum-delay")
        item = pos["continuation"]["items"][0]
        self.assertEqual(item["held_rev"], 1)
        self.assertEqual(item["rev"], 2)
        self.assertTrue(item["replace"])
        self.assertTrue(item["modifiable"])
        self.assertTrue(item["displayable"])

    def test_plaza_continuation_finished_and_village_silent(self):
        plaza = self.room.channel_position("plaza-live")
        self.assertTrue(plaza["continuation"]["end_of_program"])
        self.assertEqual(plaza["feedback_state"], "confirmed")
        village = self.room.channel_position("village-live")
        self.assertEqual(village["feedback_state"], "no_feedback")
        handover = self.room.handover()
        self.assertFalse(handover["global_block"])
        self.assertTrue(handover["languages"]["zh"]["shown"])
        self.assertFalse(handover["languages"]["local"]["shown"])

    def test_event_cursor_idempotency(self):
        before = len(self.room.events_log)
        again = self.room.ingest({
            "cursor": 1, "source": "director", "minute": 5,
            "payload": {"cue": "interlude", "decision": "skip"}})
        self.assertTrue(again["duplicate"])
        self.assertEqual(len(self.room.events_log), before)


class SkipIsolationTest(unittest.TestCase):
    def test_skipped_caption_never_fills_nearby_cue(self):
        room = ControlRoom(base_registers())
        room.ingest({"cursor": 1, "source": "director", "minute": 5,
                     "payload": {"cue": "alpha", "decision": "skip"}})
        room.ingest({"cursor": 2, "source": "caption", "minute": 6,
                     "payload": {"cue": "alpha", "lang": "zh", "rev": 9,
                                 "state": "queued", "text": "跳过段的旧词"}})
        # 即使这份迟到版次随后被批准，也仍然只属于 alpha。
        room.upsert_caption({"cue": "alpha", "lang": "zh", "rev": 9,
                             "state": "approved", "text": "跳过段的旧词"})
        room.ingest({"cursor": 3, "source": "director", "minute": 7,
                     "payload": {"cue": "beta", "anchor_ms": 420000}})

        self.assertEqual(room.current_cue(), "beta")
        self.assertIsNone(room.approved_head("beta", "zh"))
        beta = room.resolve_cue("beta", "live")
        self.assertEqual(beta["action"], "notice")  # 直播屏不能延播，也没有 brief
        alpha = room.resolve_cue("alpha", "live")
        self.assertEqual(alpha["action"], "standby")

    def test_no_time_based_matching_even_when_minutes_collide(self):
        registers = base_registers(script=[
            {"cue": "one", "languages": ["zh"], "expected_minute": 8},
            {"cue": "two", "languages": ["zh"], "expected_minute": 8},
        ])
        room = ControlRoom(registers)
        room.upsert_caption({"cue": "one", "lang": "zh", "rev": 1,
                             "state": "approved", "text": "one"})
        self.assertIsNone(room.approved_head("two", "zh"))


class LanguageAndSegmentScopeTest(unittest.TestCase):
    def test_shared_line_independent_language_approvals(self):
        room = ControlRoom(base_registers(script=[
            {"cue": "x", "languages": ["zh", "local"]}]))
        room.upsert_caption({"cue": "x", "lang": "zh", "rev": 1, "line_id": "L",
                             "state": "approved", "text": "旧中"})
        room.upsert_caption({"cue": "x", "lang": "local", "rev": 1, "line_id": "L",
                             "state": "approved", "text": "old-local"})
        # zh 独立核定 rev2；local 也出校订。
        room.approve_caption("x", "zh", 2, text="新中", line_id="L")
        room.approve_caption("x", "local", 2, text="新local", line_id="L")

        self.assertEqual(room.approved_head("x", "zh")["text"], "新中")
        self.assertEqual(room.approved_head("x", "local")["text"], "新local")
        # 再改 local 不应连带 zh。
        room.review_caption("x", "local", 3, text="段一", applies_to=["p1"])
        self.assertEqual(room.approved_head("x", "zh")["rev"], 2)
        self.assertEqual(room.approved_head("x", "local")["rev"], 2)  # 通用 head 不变
        self.assertEqual(room.approved_head("x", "local", segment="p1")["rev"], 3)
        self.assertEqual(room.approved_head("x", "local", segment="p2")["rev"], 2)

    def test_program_version_follows_referenced_segment(self):
        registers = base_registers(
            script=[{"cue": "x", "languages": ["local"]}],
            extra={"program_versions": [
                {"id": "pv-seg1", "cue": "x", "lang": "local", "segment": "p1"},
                {"id": "pv-full", "cue": "x", "lang": "local"},
            ]})
        room = ControlRoom(registers)
        room.approve_caption("x", "local", 1, text="通用")
        room.review_caption("x", "local", 2, text="第一段校订", applies_to=["p1"])
        versions = {p["id"]: p for p in room.program_versions_view()}
        self.assertEqual(versions["pv-seg1"]["replay_rev"], 2)
        self.assertEqual(versions["pv-full"]["replay_rev"], 1)


class MonotonicPlaybackTest(unittest.TestCase):
    def test_late_shown_report_does_not_roll_position_back(self):
        room = ControlRoom(base_registers(script=[
            {"cue": "c0", "languages": ["zh"]},
            {"cue": "c1", "languages": ["zh"]},
            {"cue": "c2", "languages": ["zh"]},
        ]))
        room.record_playback("live", "c1", "shown", rev=1, minute=10)
        room.record_playback("live", "c0", "shown", rev=1, minute=12)  # 迟到旧段
        pos = room.channel_position("live")
        self.assertEqual(pos["last_report"]["last_shown_cue"], "c1")
        cues = [i["cue"] for i in pos["continuation"]["items"]]
        self.assertNotIn("c0", cues)
        self.assertIn("c2", cues)
        self.assertEqual(len(room.air_facts), 2)  # 事实仍如实保存

    def test_shown_revision_not_modifiable_but_replay_uses_correction(self):
        room = ControlRoom(base_registers(channels={
            "museum": {"lang": "zh", "can_hold": True, "minimum_delay": 3}}))
        room.approve_caption("beta", "zh", 1, text="旧译")
        room.record_playback("museum", "beta", "fetched", rev=1, minute=9)
        item = room.continuation("museum")["items"][0]
        self.assertTrue(item["modifiable"])
        room.record_playback("museum", "beta", "shown", rev=1, minute=10)
        room.approve_caption("beta", "zh", 2, text="校订译")

        claim_view = room._channel_claim_view("museum", "beta")
        self.assertFalse(claim_view["modifiable"])
        self.assertFalse(claim_view["update_available"])  # 已 shown，不推回屏幕
        self.assertTrue(room.continuation("museum")["end_of_program"])
        edition = {(e["cue"], e["lang"]): e for e in room.replay_editions()}
        self.assertEqual(edition[("beta", "zh")]["replay_rev"], 2)
        self.assertTrue(edition[("beta", "zh")]["corrected_after_air"])
        self.assertEqual(edition[("beta", "zh")]["aired_revs"], [1])

        corrected = room.replay_selection("beta", "zh", mode="corrected")
        aired = room.replay_selection("beta", "zh", mode="aired")
        self.assertEqual((corrected["rev"], corrected["text"]), (2, "校订译"))
        self.assertEqual((aired["rev"], aired["text"]), (1, "旧译"))

    def test_queued_state_is_also_modifiable(self):
        room = ControlRoom(base_registers())
        room.approve_caption("beta", "zh", 1, text="v1")
        room.record_playback("live", "beta", "queued", rev=1)
        claim = room._channel_claim_view("live", "beta")
        self.assertTrue(claim["modifiable"])
        self.assertTrue(claim["queued"])

    def test_replace_decision_independent_of_arrival_order(self):
        # 顺序 A：先领旧版，后到校订。
        a = ControlRoom(base_registers(channels={
            "m": {"lang": "zh", "can_hold": True}}))
        a.approve_caption("beta", "zh", 1, text="v1")
        a.record_playback("m", "beta", "fetched", rev=1)
        a.approve_caption("beta", "zh", 2, text="v2")
        item_a = a.continuation("m")["items"][0]

        # 顺序 B：校订先到，播放点才迟报领到旧版。
        b = ControlRoom(base_registers(channels={
            "m": {"lang": "zh", "can_hold": True}}))
        b.approve_caption("beta", "zh", 1, text="v1")
        b.approve_caption("beta", "zh", 2, text="v2")
        b.record_playback("m", "beta", "fetched", rev=1)
        item_b = b.continuation("m")["items"][0]

        for item in (item_a, item_b):
            self.assertEqual((item["held_rev"], item["rev"]), (1, 2))
            self.assertTrue(item["replace"] and item["modifiable"])


class FallbackPolicyTest(unittest.TestCase):
    def _room(self, channels):
        return ControlRoom(base_registers(channels=channels))

    def test_live_without_caption_or_brief_shows_notice(self):
        room = self._room({"p": {"lang": "zh", "can_hold": False}})
        decision = room.resolve_cue("beta", "p")
        self.assertEqual(decision["action"], "notice")
        self.assertIn("暂缺", decision["text"])

    def test_approved_brief_is_used_for_live(self):
        room = self._room({"p": {"lang": "zh", "can_hold": False}})
        room.upsert_caption({"cue": "beta", "lang": "zh", "rev": 1, "kind": "brief",
                             "state": "approved", "text": "简要预告"})
        decision = room.resolve_cue("beta", "p")
        self.assertEqual(decision["action"], "brief")
        self.assertEqual(decision["caption"]["text"], "简要预告")

    def test_hold_channel_delays(self):
        room = self._room({"m": {"lang": "zh", "can_hold": True, "minimum_delay": 3}})
        decision = room.resolve_cue("beta", "m")
        self.assertEqual(decision["action"], "delay")
        self.assertEqual(decision["minimum_delay"], 3)

    def test_explicit_guarantee_order_wins(self):
        room = self._room({"m": {
            "lang": "zh", "can_hold": True,
            "guarantee": {"on_missing": ["notice"], "unavailable_text": "信号暂缺"}}})
        decision = room.resolve_cue("beta", "m")
        self.assertEqual(decision["action"], "notice")
        self.assertEqual(decision["text"], "信号暂缺")

        room2 = self._room({"m2": {
            "lang": "zh", "can_hold": True, "on_missing": ["brief", "delay", "notice"]}})
        self.assertEqual(room2.resolve_cue("beta", "m2")["action"], "delay")


class ReceiptRulesTest(unittest.TestCase):
    def test_room_receipt_idempotency_and_reference_conflict(self):
        room = fixture_room()
        room.record_receipt("ref-1", "publish-cue", "museum-delay",
                            {"cue": "courtyard-talk", "rev": 2, "fetched": True})
        again = room.record_receipt("ref-1", "publish-cue", "museum-delay",
                                    {"cue": "courtyard-talk", "rev": 2, "fetched": True})
        self.assertTrue(again["idempotent"])
        with self.assertRaises(BackendError) as ctx:
            room.record_receipt("ref-1", "other-op", "museum-delay", {})
        self.assertEqual(ctx.exception.status, 409)

    def test_receipt_fact_does_not_duplicate_air_fact(self):
        room = fixture_room()
        before = len(room.air_facts)
        room.record_receipt("ref-p", "publish-cue", "plaza-live", {
            "sequence": "plaza-85", "cue": "courtyard-talk", "rev": 1, "shown": True})
        self.assertEqual(len(room.air_facts), before)
        fact = [f for f in room.air_facts if f["screen"] == "plaza-live"][0]
        self.assertEqual(fact["sequence"], "plaza-85")

    def test_pending_outbox_is_uncertain_never_shown(self):
        room = fixture_room()
        # 没有回执客户端：只登记 pending，模拟请求发出后再无反馈。
        result = publish_current_cue(room, "village-live")
        self.assertEqual(result["status"], "pending")
        pos = room.channel_position("village-live")
        self.assertIsNone(pos["last_confirmed_position"])
        handover = room.handover()
        self.assertEqual(
            handover["channels"]["village-live"]["uncertain"][0]["status"], "pending")
        # museum 只 fetched 未 shown，village 无反馈：local 语言不能算已播出。
        self.assertFalse(handover["languages"]["local"]["shown"])
        self.assertFalse(handover["global_block"])


class GatewayClientTest(unittest.TestCase):
    def setUp(self):
        self.gateway = FixtureGateway(0)
        self.addCleanup(self.gateway.server_close)
        self.thread = Thread(target=self.gateway.serve_forever,
                             kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.addCleanup(self.thread.join, 2)
        self.addCleanup(self.gateway.shutdown)
        self.client = ReceiptGatewayClient(port=self.gateway.server_port)

    def test_lost_reply_safe_retry_with_same_reference(self):
        result = self.client.post_receipt("local-museum-1", "publish-cue", "museum-delay")
        self.assertEqual(result["receipt"]["sequence"], "museum-48")
        self.assertEqual(result["receipt"]["rev"], 2)
        # 服务端确实只记录了这一个 reference，且 GET 可恢复。
        recovered = self.client.get_receipt("local-museum-1")
        self.assertEqual(recovered["reference"], "local-museum-1")
        # 再次重试仍是同一结果。
        again = self.client.post_receipt("local-museum-1", "publish-cue", "museum-delay")
        self.assertEqual(again, result)

    def test_missing_counterparty_and_reference_conflict(self):
        with self.assertRaises(NoCounterpartyExample):
            self.client.post_receipt("local-village-1", "publish-cue", "village-live")
        ok = self.client.post_receipt("dup-ref", "publish-cue", "plaza-live")
        self.assertTrue(ok["receipt"]["shown"])
        with self.assertRaises(ReceiptConflict):
            self.client.post_receipt("dup-ref", "another-op", "plaza-live")

    def test_safe_reference_matches_gateway_rules(self):
        ref = safe_reference("pub", "museum-delay", "courtyard-talk", "r2")
        self.assertLessEqual(len(ref), 96)
        self.client.post_receipt(ref, "publish-cue", "museum-delay")


class HttpApiTest(unittest.TestCase):
    def setUp(self):
        self.server = build_server(0, fixtures=FIXTURES, replay_fixture_events=True)
        self.port = self.server.server_port
        self.addCleanup(self.server.server_close)
        self.thread = Thread(target=self.server.serve_forever,
                             kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.addCleanup(self.thread.join, 2)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, payload=None):
        conn = HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            body = None if payload is None else json.dumps(
                payload, ensure_ascii=False).encode("utf-8")
            conn.request(method, path, body,
                          {"Content-Type": "application/json; charset=utf-8"})
            resp = conn.getresponse()
            raw = resp.read()
            return resp.status, json.loads(raw.decode("utf-8")) if raw else None
        finally:
            conn.close()

    def test_health_and_board(self):
        self.assertEqual(self.call("GET", "/healthz")[0], 200)
        status, board = self.call("GET", "/v1/board")
        self.assertEqual(status, 200)
        self.assertEqual(board["current_cue"], "courtyard-talk")
        self.assertIn("interlude", board["skipped_cues"])

    def test_replay_and_editions_endpoints(self):
        status, payload = self.call("GET", "/v1/replay?flat=1")
        self.assertEqual(status, 200)
        keys = {(e["cue"], e["lang"]): e for e in payload["editions"]}
        self.assertEqual(keys[("courtyard-talk", "local")]["replay_rev"], 2)
        status, payload = self.call("GET", "/v1/editions?cue=courtyard-talk&lang=zh")
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["editions"]), 1)

        _, aired = self.call(
            "GET", "/v1/replay/select?cue=courtyard-talk&lang=zh&mode=aired")
        self.assertEqual((aired["rev"], aired["text"]), (1, "演出将在西院开始"))
        _, no_air = self.call(
            "GET", "/v1/replay/select?cue=courtyard-talk&lang=local&mode=aired")
        self.assertFalse(no_air["available"])

    def test_channel_position_and_404(self):
        status, pos = self.call("GET", "/v1/channels/museum-delay/position")
        self.assertEqual(status, 200)
        item = pos["continuation"]["items"][0]
        self.assertTrue(item["replace"])
        self.assertEqual(item["rev"], 2)
        status, body = self.call("GET", "/v1/channels/nope/position")
        self.assertEqual(status, 404)
        self.assertEqual(body["code"], "not_found")

    def test_playback_report_updates_handover(self):
        status, rec = self.call("POST", "/v1/playback", {
            "screen": "village-live", "cue": "courtyard-talk",
            "lang": "local", "rev": 2, "state": "shown", "minute": 12})
        self.assertEqual(status, 201)
        self.assertTrue(rec["shown"])
        _, handover = self.call("GET", "/v1/handover")
        self.assertTrue(handover["languages"]["local"]["shown"])
        self.assertEqual(len(handover["air_facts"]), 2)

    def test_director_anchor_moves_current_cue(self):
        status, rec = self.call("POST", "/v1/director/anchor",
                                {"cue": "riverside", "anchor_ms": 900000, "minute": 18})
        self.assertEqual(status, 201)
        self.assertEqual(rec["status"], "anchored")
        _, board = self.call("GET", "/v1/board")
        self.assertEqual(board["current_cue"], "riverside")

    def test_event_batch_and_validation(self):
        status, body = self.call("POST", "/v1/events", {"events": [
            {"source": "director", "minute": 19,
             "payload": {"cue": "welcome", "decision": "skip"}}]})
        self.assertEqual(status, 201)
        self.assertEqual(body["applied"], 1)
        status, body = self.call("POST", "/v1/playback",
                                 {"screen": "x", "cue": "y", "state": "nonsense"})
        self.assertEqual(status, 400)
        status, _ = self.call("GET", "/v1/events?after=not-an-int")
        self.assertEqual(status, 400)

    def test_review_caption_endpoint(self):
        status, rec = self.call("POST", "/v1/reviews/caption", {
            "cue": "welcome", "lang": "zh", "rev": 5, "text": "修订欢迎词",
            "applies_to": ["opening"], "terms": [{"term": "古村", "translation": "old village"}],
        })
        self.assertEqual(status, 201)
        self.assertEqual(rec["state"], "approved")
        _, terms = self.call("GET", "/v1/terms?lang=zh")
        self.assertTrue(any(t["term"] == "古村" for t in terms["terms"]))

    def test_direct_receipt_endpoint(self):
        payload = {"reference": "http-ref-1", "operation": "publish-cue",
                   "recipient": "museum-delay",
                   "receipt": {"cue": "courtyard-talk", "rev": 2, "fetched": True}}
        self.assertEqual(self.call("POST", "/v1/receipts", payload)[0], 201)
        status, body = self.call("GET", "/v1/receipts/http-ref-1")
        self.assertEqual(status, 200)
        self.assertEqual(body["receipt"]["rev"], 2)
        self.assertEqual(self.call("POST", "/v1/receipts", payload)[0], 200)

    def test_unknown_route_404(self):
        self.assertEqual(self.call("GET", "/v1/nonsense")[0], 404)


class HttpGatewayIntegrationTest(unittest.TestCase):
    """真实 fixture gateway + 后端 HTTP：sync 与 publish 全链路。"""

    def setUp(self):
        self.gateway = FixtureGateway(0)
        self.addCleanup(self.gateway.server_close)
        self.gw_thread = Thread(target=self.gateway.serve_forever,
                                kwargs={"poll_interval": 0.02}, daemon=True)
        self.gw_thread.start()
        self.addCleanup(self.gw_thread.join, 2)
        self.addCleanup(self.gateway.shutdown)

        self.server = build_server(
            0, fixtures=FIXTURES, replay_fixture_events=False,
            gateway_url=f"http://127.0.0.1:{self.gateway.server_port}")
        self.port = self.server.server_port
        self.addCleanup(self.server.server_close)
        self.api_thread = Thread(target=self.server.serve_forever,
                                 kwargs={"poll_interval": 0.02}, daemon=True)
        self.api_thread.start()
        self.addCleanup(self.api_thread.join, 2)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, payload=None):
        conn = HTTPConnection("127.0.0.1", self.port, timeout=4)
        try:
            body = None if payload is None else json.dumps(
                payload, ensure_ascii=False).encode("utf-8")
            conn.request(method, path, body,
                          {"Content-Type": "application/json; charset=utf-8"})
            resp = conn.getresponse()
            raw = resp.read()
            return resp.status, json.loads(raw.decode("utf-8")) if raw else None
        finally:
            conn.close()

    def test_sync_then_publish_with_lost_retry(self):
        status, summary = self.call("POST", "/v1/sync", {})
        self.assertEqual(status, 200)
        self.assertEqual(summary["events"], 6)

        status, museum = self.call("POST", "/v1/publish", {"channel": "museum-delay"})
        self.assertEqual(status, 200, museum)
        self.assertEqual(museum["status"], "receipted")
        self.assertEqual(museum["receipt"]["rev"], 2)
        self.assertFalse(museum["receipt"]["shown"])

        _, pos = self.call("GET", "/v1/channels/museum-delay/position")
        self.assertEqual(pos["last_confirmed_position"]["state"], "fetched")
        self.assertEqual(pos["last_confirmed_position"]["rev"], 2)

        status, village = self.call("POST", "/v1/publish", {"channel": "village-live"})
        self.assertEqual(status, 202)
        self.assertEqual(village["status"], "no_counterparty")

        status, plaza = self.call("POST", "/v1/publish", {"channel": "plaza-live"})
        self.assertEqual(status, 200)
        self.assertTrue(plaza["receipt"]["shown"])

        _, handover = self.call("GET", "/v1/handover")
        self.assertFalse(handover["global_block"])
        self.assertTrue(handover["languages"]["zh"]["shown"])


class DemoScriptTest(unittest.TestCase):
    def test_demo_runs_and_self_checks(self):
        import demo
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            result = demo.run()
        self.assertEqual(result["current_cue"], "courtyard-talk")
        self.assertIn("DEMO OK", buf.getvalue())


class ConcurrencyTest(unittest.TestCase):
    def test_parallel_commands_keep_consistent_state(self):
        registers = base_registers(channels={
            "s1": {"lang": "zh", "can_hold": True},
            "s2": {"lang": "zh", "can_hold": False},
        })
        room = ControlRoom(registers)

        def worker(i):
            room.command("director", {"cue": f"cue-{i}", "anchor_ms": i * 1000})
            room.upsert_caption({"cue": f"cue-{i}", "lang": "zh", "rev": 1,
                                 "state": "approved", "text": f"t{i}"})
            room.record_playback("s1", f"cue-{i}", "fetched", rev=1)

        threads = [Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(room.cues) - 2, 20)  # 合成台账自带 alpha/beta
        self.assertEqual(len([c for c in room.claims.values() if c["screen"] == "s1"]), 20)
        # 看板与交接视图在并发结束后可正常序列化。
        json.dumps(room.board(), ensure_ascii=False)
        json.dumps(room.handover(), ensure_ascii=False, default=str)


if __name__ == "__main__":
    unittest.main()
