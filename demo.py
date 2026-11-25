#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""端到端演练：回放 fixtures 事件 -> 向合作方桩取回执 -> 输出三方视图。

直接运行：``python3 demo.py``。会在随机端口启动仓库自带的
``fixture_gateway.FixtureGateway``，不需要任何外部服务。
"""

from __future__ import annotations

import json
from threading import Thread

from backend import (
    ControlRoom, ReceiptGatewayClient, publish_current_cue,
)
from fixture_gateway import FixtureGateway


def start_gateway():
    gateway = FixtureGateway(0)
    thread = Thread(target=gateway.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    return gateway, thread


def run():
    gateway, thread = start_gateway()
    port = gateway.server_port
    try:
        room = ControlRoom.from_fixtures()
        room.replay_events()
        client = ReceiptGatewayClient(port=port)

        # 向三个渠道发布当前节目的当前 approved head。
        publishes = {}
        for screen in ("museum-delay", "plaza-live", "village-live"):
            publishes[screen] = publish_current_cue(room, screen, client=client)

        # 导播：现在能放什么
        board = room.board()
        # 审校：哪些版用于回看
        editions = room.replay_editions()
        # 播放点：上次确认位置 + 可接续内容
        positions = {s: room.channel_position(s) for s in room.channels}
        handover = room.handover()

        result = {
            "gateway_port": port,
            "publishes": publishes,
            "current_cue": board["current_cue"],
            "skipped_cues": board["skipped_cues"],
            "replay_editions": editions,
            "museum_position": positions["museum-delay"]["last_confirmed_position"],
            "museum_continuation": positions["museum-delay"]["continuation"]["items"],
            "plaza_position": positions["plaza-live"]["last_confirmed_position"],
            "plaza_continuation_end": positions["plaza-live"]["continuation"]["end_of_program"],
            "village_feedback": positions["village-live"]["feedback_state"],
            "handover_languages": handover["languages"],
            "air_facts": handover["air_facts"],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))

        # ---- 关键不变量断言（同时也是 demo 的自检） ----
        assert board["current_cue"] == "courtyard-talk"
        assert board["skipped_cues"] == ["interlude"]

        local_head = room.approved_head("courtyard-talk", "local")
        zh_head = room.approved_head("courtyard-talk", "zh")
        assert local_head["rev"] == 2 and local_head["text"] == "西院开唱"
        assert zh_head["rev"] == 1 and "西院" in zh_head["text"]

        # 跳过节目的 queued rev2 仍是备选，没有挂到 courtyard。
        interlude = room.captions[("interlude", "local", 2, "")]
        assert interlude["state"] == "queued"
        assert all(not (c["cue"] == "interlude" and c["cue"] == "courtyard-talk")
                   for c in room.captions.values())

        # museum：lost_reply 后同 reference 重试成功，回执确认 rev2 fetched/not shown。
        museum_pub = publishes["museum-delay"]
        assert museum_pub["status"] == "receipted", museum_pub
        assert museum_pub["receipt"]["rev"] == 2
        assert museum_pub["receipt"]["fetched"] is True
        assert museum_pub["receipt"]["shown"] is False
        museum_pos = room.channel_position("museum-delay")
        assert museum_pos["last_confirmed_position"]["rev"] == 2
        assert museum_pos["last_confirmed_position"]["state"] == "fetched"
        museum_item = museum_pos["continuation"]["items"][0]
        assert museum_item["rev"] == 2 and museum_item["replace"] is False
        assert museum_item["modifiable"] is True and museum_item["displayable"] is True

        # plaza：回执与事件都确认 rev1 shown；播出事实不可变，接续队列已到段尾。
        plaza_pub = publishes["plaza-live"]
        assert plaza_pub["status"] == "receipted"
        assert plaza_pub["receipt"]["shown"] is True
        assert len(room.air_facts) == 1
        assert room.air_facts[0]["screen"] == "plaza-live"
        assert room.air_facts[0]["rev"] == 1
        assert room.channel_position("plaza-live")["continuation"]["end_of_program"] is True

        # village：没有合作方样例 -> no_counterparty/no_feedback，不算播出，
        # 也不影响 museum/plaza 被确认。
        assert publishes["village-live"]["status"] == "no_counterparty"
        assert room.channel_position("village-live")["feedback_state"] == "no_feedback"
        assert handover["global_block"] is False
        assert handover["languages"]["zh"]["shown"] is True
        assert handover["languages"]["local"]["shown"] is False

        editions_by_key = {(e["cue"], e["lang"]): e for e in editions}
        assert editions_by_key[("courtyard-talk", "local")]["replay_rev"] == 2
        assert editions_by_key[("courtyard-talk", "zh")]["replay_rev"] == 1
        assert editions_by_key[("interlude", "local")]["usage"] == "standby"

        print("\nDEMO OK: 所有关键业务不变量成立。")
        return result
    finally:
        gateway.shutdown()
        thread.join(timeout=2)
        gateway.server_close()


if __name__ == "__main__":
    run()
