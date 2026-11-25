#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""古村巡游直播：导播 / 翻译审校 / 播放点共用的多语字幕后端。

仅使用 Python 3.11 标准库。领域规则集中在 :class:`ControlRoom`，HTTP 层只做
JSON 编解码与路由；这样既可以独立起服务，也可以在测试或脚本里直接 import
领域核心。

核心不变量
----------
* 节目按显式 ``cue`` 标识匹配；跳过节目的字幕只作备选（standby），绝不按
  分钟相近自动挂到后面的节目上。
* 版次按 ``(cue, lang, rev[, segment])`` 区分；校订只推进对应语言/段落的
  approved head，不连带另一语言（即使源字幕行共享 line id）。
* ``shown`` 生成不可变播出事实；迟到消息不能把播放位置向后推。
* 已领（fetched）/入队（queued）但未显示的内容仍可安全替换；未收到回执的
  远端只能记为 uncertain/no_feedback，不算播出，也不阻塞其他语言屏。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import threading
from http.client import HTTPConnection, RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

ROOT = Path(__file__).resolve().parent
FIXTURE_DIR = ROOT / "fixtures"
REFERENCE_RE = re.compile(r"[A-Za-z0-9_-]{1,96}")

STATE_RANK = {"draft": 0, "queued": 1, "approved": 2, "rejected": -1, "withdrawn": -1}
PLAYBACK_STATES = {
    "fetched": "fetched",
    "claim": "fetched",
    "claimed": "fetched",
    "queued": "queued",
    "enqueued": "queued",
    "in_queue": "queued",
    "shown": "shown",
    "displayed": "shown",
    "on_air": "shown",
}
FALLBACK_TOKENS = {
    "brief": "brief", "preview": "brief", "summary": "brief", "teaser": "brief",
    "简要": "brief", "简要预告": "brief", "预告": "brief",
    "delay": "delay", "hold": "delay", "delayed": "delay",
    "延播": "delay", "延时": "delay",
    "notice": "notice", "unavailable": "notice", "missing": "notice",
    "暂缺": "notice", "字幕暂缺": "notice",
}


# --------------------------------------------------------------------------- #
# 错误类型
# --------------------------------------------------------------------------- #
class BackendError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message=None, **details):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details


class ValidationError(BackendError):
    status = 400
    code = "invalid_request"


class NotFound(BackendError):
    status = 404
    code = "not_found"


class Conflict(BackendError):
    status = 409
    code = "conflict"


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #
def _require_str(value, field, *, max_len=120):
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} must be a non-empty string", field=field)
    value = value.strip()
    if len(value) > max_len:
        raise ValidationError(f"{field} is too long", field=field)
    return value


def _opt_int(value, field):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValidationError(f"{field} must be an integer", field=field)
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValidationError(f"{field} must be an integer", field=field)


def normalize_fallback(order):
    if order is None:
        return None
    if isinstance(order, str):
        values = [order]
    elif isinstance(order, (list, tuple)):
        values = list(order)
    else:
        return None
    out = []
    for token in values:
        key = str(token).strip()
        mapped = FALLBACK_TOKENS.get(key.lower()) or FALLBACK_TOKENS.get(key)
        if mapped and mapped not in out:
            out.append(mapped)
    return out


def safe_reference(*parts):
    """生成满足合作方桩正则 [A-Za-z0-9_-]{1,96} 的幂等 reference。"""
    raw = "-".join(str(p) for p in parts if p is not None and str(p) != "")
    raw = re.sub(r"[^A-Za-z0-9_-]+", "-", raw).strip("-_")
    if len(raw) > 96 or not REFERENCE_RE.fullmatch(raw or ""):
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        raw = (raw[:83] + "-" + digest).strip("-")
    return raw


def load_fixtures(folder):
    folder = Path(folder)
    registers = json.loads((folder / "registers.json").read_text(encoding="utf-8"))
    events = [
        json.loads(line)
        for line in (folder / "events.ndjson").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return registers, events


# --------------------------------------------------------------------------- #
# 领域核心
# --------------------------------------------------------------------------- #
class ControlRoom:
    """全部状态与业务规则；线程安全由一把 RLock 保护。"""

    def __init__(self, registers=None):
        self.lock = threading.RLock()
        self.registers = registers if isinstance(registers, dict) else {}

        self.script = list(self.registers.get("script", []))
        self.cue_index = {}
        for i, item in enumerate(self.script):
            cue = item.get("cue")
            if isinstance(cue, str):
                self.cue_index[cue] = i

        self.channels = dict(self.registers.get("channels", {}))
        self.program_versions = list(self.registers.get("program_versions", []))

        # cue -> 实际状态
        self.cues = {}
        for item in self.script:
            cue = item["cue"]
            self.cues[cue] = {
                "cue": cue,
                "status": "scheduled",
                "expected_minute": item.get("expected_minute"),
                "languages": list(item.get("languages", [])),
                "anchor_ms": None,
                "minute": None,
                "skip_minute": None,
                "extra": False,
            }
        self._extra_cue_seq = 0

        # 字幕版次：key=(cue, lang, rev, segment) -> record
        self.captions = {}
        for cv in self.registers.get("caption_versions", []):
            try:
                self.upsert_caption(cv, source="register")
            except BackendError:
                continue

        # 术语：key=(term, lang) -> record
        self.terms = {}
        for term in self.registers.get("terms", []):
            if isinstance(term, dict) and term.get("term"):
                self.confirm_term(
                    term["term"], term.get("lang", "zh"),
                    term.get("translation"), state=term.get("state", "approved"),
                    source="register",
                )

        # 播放端领取/显示：key=(screen, cue, rev) -> claim
        self.claims = {}
        self.air_facts = []          # 不可变播出事实

        # 事件日志（cursor 从 1 开始，作为 GET /v1/events 的游标）
        self.events_log = []
        self._external_cursors = set()

        # 合作方回执与发件箱
        self.receipts = {}           # reference -> {operation, recipient, receipt}
        self.outbox = {}             # reference -> {operation, recipient, status, ...}

    # ---------------- 构造辅助 ---------------- #
    @classmethod
    def from_fixtures(cls, folder=FIXTURE_DIR):
        registers, _ = load_fixtures(folder)
        return cls(registers)

    def replay_events(self, folder=FIXTURE_DIR):
        _, events = load_fixtures(folder)
        result = []
        for event in events:
            result.append(self.ingest(event))
        return result

    def append_log(self, source, payload, minute=None):
        entry = {"cursor": len(self.events_log) + 1, "source": source,
                 "minute": minute, "payload": payload}
        self.events_log.append(entry)
        return entry

    # ---------------- 节目锚点 / 跳段（纯状态变更） ---------------- #
    def _ensure_cue(self, cue):
        cue = _require_str(cue, "cue")
        if cue not in self.cues:
            with self.lock:
                if cue not in self.cues:
                    self._extra_cue_seq += 1
                    self.cues[cue] = {
                        "cue": cue, "status": "scheduled", "expected_minute": None,
                        "languages": [], "anchor_ms": None, "minute": None,
                        "skip_minute": None, "extra": True,
                        "_extra_order": self._extra_cue_seq,
                    }
        return cue

    def cue_order(self, cue):
        if cue in self.cue_index:
            return self.cue_index[cue]
        rec = self.cues.get(cue)
        if rec and rec.get("extra"):
            return 10_000 + int(rec.get("_extra_order", 1))
        return 20_000

    def skip_cue(self, cue, minute=None, reason=None, source="director"):
        with self.lock:
            cue = self._ensure_cue(cue)
            rec = self.cues[cue]
            rec["status"] = "skipped"
            rec["skip_minute"] = minute
            if reason is not None:
                rec["skip_reason"] = reason
            return dict(rec)

    def anchor_cue(self, cue, anchor_ms=None, minute=None, source="director", **extra):
        with self.lock:
            cue = self._ensure_cue(cue)
            rec = self.cues[cue]
            rec["status"] = "anchored"
            rec["anchor_ms"] = _opt_int(anchor_ms, "anchor_ms")
            if minute is not None:
                rec["minute"] = minute
            for key in ("actual_minute", "note"):
                if key in extra and extra[key] is not None:
                    rec[key] = extra[key]
            # 以送达顺序记录“当前实际节目”，分钟只作业务时间展示。
            rec["_anchored_seq"] = len(self.events_log) + 1
            return dict(rec)

    def current_cue(self):
        """最新送达的实际锚点节目；没有锚点时返回 None。"""
        with self.lock:
            anchored = [c for c in self.cues.values() if c["status"] == "anchored"]
            if not anchored:
                return None
            anchored.sort(key=lambda c: c.get("_anchored_seq", 0))
            return anchored[-1]["cue"]

    # ---------------- 字幕版次 / 审校 ---------------- #
    def upsert_caption(self, data, *, minute=None, source="caption"):
        with self.lock:
            if not isinstance(data, dict):
                raise ValidationError("caption must be a JSON object")
            cue = _require_str(data.get("cue"), "cue")
            lang = _require_str(data.get("lang"), "lang", max_len=24)
            rev = _opt_int(data.get("rev"), "rev")
            if rev is None or rev < 1:
                raise ValidationError("rev must be a positive integer")
            self._ensure_cue(cue)
            segment = str(data.get("segment") or data.get("paragraph") or "")
            applies_to = data.get("applies_to") or data.get("segments") or []
            if isinstance(applies_to, str):
                applies_to = [applies_to]
            applies_to = [str(s) for s in applies_to]

            key = (cue, lang, rev, segment)
            existing = self.captions.get(key)
            requested_state = data.get("state")
            if requested_state is None:
                state = existing["state"] if existing else "queued"
            elif requested_state not in STATE_RANK:
                state = "queued"
            else:
                state = requested_state
            if existing is not None and STATE_RANK.get(state, 0) < STATE_RANK.get(existing["state"], 0):
                # 迟到的旧状态（如 queued）不能把 approved 降级。
                state = existing["state"]

            if existing is not None:
                existing.update({
                    "text": data.get("text", existing.get("text")),
                    "state": state,
                    "edition": data.get("edition", existing.get("edition")),
                    "kind": data.get("kind", existing.get("kind", "full")) or "full",
                    "line_id": data.get("line_id") or data.get("shared_line") or existing.get("line_id"),
                    "applies_to": applies_to or existing.get("applies_to", []),
                })
                if data.get("terms") is not None:
                    existing["terms"] = list(data.get("terms"))
                rec = existing
            else:
                rec = {
                    "cue": cue, "lang": lang, "rev": rev, "segment": segment,
                    "applies_to": applies_to, "text": data.get("text"), "state": state,
                    "edition": data.get("edition"),
                    "kind": data.get("kind", "full") or "full",
                    "line_id": data.get("line_id") or data.get("shared_line"),
                    "terms": list(data.get("terms", []) or []),
                    "minute": minute if minute is not None else data.get("minute"),
                    "source": source,
                }
                self.captions[key] = rec
            if state == "approved":
                rec["approved_minute"] = minute if minute is not None else rec.get("approved_minute")
            return dict(rec)

    def approve_caption(self, cue, lang, rev, text=None, *, minute=None, **fields):
        """轻量审校通过：不存在则按 approved 建版，存在则推进状态。"""
        payload = {"cue": cue, "lang": lang, "rev": rev, "state": "approved"}
        if text is not None:
            payload["text"] = text
        payload.update(fields)
        return self.upsert_caption(payload, minute=minute, source="editor")

    def review_caption(self, cue, lang, rev, *, text=None, applies_to=None,
                       terms=None, minute=None, edition=None):
        """审校一次确认：译文 + 适用段落 + 术语。"""
        with self.lock:
            payload = {
                "cue": cue, "lang": lang, "rev": rev, "state": "approved",
                "text": text, "applies_to": applies_to or [],
                "edition": edition, "terms": terms or [],
            }
            rec = self.upsert_caption(payload, minute=minute, source="editor")
            for term in terms or []:
                if isinstance(term, dict) and term.get("term"):
                    self.confirm_term(term["term"], term.get("lang", lang),
                                      term.get("translation"), minute=minute)
                elif isinstance(term, str):
                    self.confirm_term(term, lang, None, minute=minute)
            return rec

    def confirm_term(self, term, lang="zh", translation=None, *, state="approved",
                     minute=None, source="editor"):
        with self.lock:
            term = _require_str(term, "term", max_len=80)
            lang = _require_str(lang, "lang", max_len=24)
            key = (term, lang)
            rec = self.terms.get(key)
            if rec is None:
                rec = {"term": term, "lang": lang, "translation": translation,
                       "state": state if state in STATE_RANK else "approved",
                       "minute": minute, "source": source}
                self.terms[key] = rec
            else:
                if translation is not None:
                    rec["translation"] = translation
                if STATE_RANK.get(state, 2) >= STATE_RANK.get(rec["state"], 0):
                    rec["state"] = state
            return dict(rec)

    def list_terms(self, lang=None):
        with self.lock:
            rows = [dict(t) for t in self.terms.values()]
            if lang:
                rows = [t for t in rows if t["lang"] == lang]
            return rows

    def approved_head(self, cue, lang, segment=None, kind="full"):
        """某 cue/lang 下当前 approved head（最高 rev）。

        segment=None 优先取整段通用版；指定 segment 时优先窄作用域，其次通用版。
        """
        with self.lock:
            rows = [
                c for c in self.captions.values()
                if c["cue"] == cue and c["lang"] == lang
                and c["state"] == "approved" and c.get("kind", "full") == kind
            ]
            if segment is not None:
                narrow = [c for c in rows
                          if c.get("segment") == segment or segment in c.get("applies_to", [])]
                if narrow:
                    return dict(max(narrow, key=lambda c: c["rev"]))
                rows = [c for c in rows if not c.get("segment") and not c.get("applies_to")]
            else:
                broad = [c for c in rows if not c.get("segment") and not c.get("applies_to")]
                if broad:
                    rows = broad
            if not rows:
                return None
            return dict(max(rows, key=lambda c: c["rev"]))

    def list_captions(self, cue=None, lang=None, state=None):
        with self.lock:
            rows = []
            for c in self.captions.values():
                if cue and c["cue"] != cue:
                    continue
                if lang and c["lang"] != lang:
                    continue
                if state and c["state"] != state:
                    continue
                rows.append(dict(c))
            rows.sort(key=lambda c: (self.cue_order(c["cue"]), c["lang"], c["rev"], c["segment"]))
            return rows

    # ---------------- 播放反馈 / 播出事实 ---------------- #
    def channel(self, screen):
        ch = self.channels.get(screen)
        if ch is None:
            raise NotFound(f"unknown channel/screen: {screen}", screen=screen)
        return ch

    def _claim(self, screen, cue, rev, lang):
        key = (screen, cue, int(rev))
        claim = self.claims.get(key)
        if claim is None:
            ch = self.channels.get(screen, {})
            claim = {
                "screen": screen, "cue": cue, "rev": int(rev),
                "lang": lang or ch.get("lang"),
                "fetched": False, "queued": False, "shown": False,
                "sequence": None, "minute": None, "sources": [], "reference": None,
            }
            self.claims[key] = claim
        if lang and not claim.get("lang"):
            claim["lang"] = lang
        return claim

    def record_playback(self, screen, cue, state, rev=None, lang=None, minute=None,
                        sequence=None, source="playback", reference=None):
        with self.lock:
            screen = _require_str(screen, "screen", max_len=120)
            cue = _require_str(cue, "cue")
            canonical = PLAYBACK_STATES.get(str(state))
            if canonical is None:
                raise ValidationError(f"unsupported playback state: {state}", state=state)
            if screen not in self.channels:
                if not lang:
                    raise ValidationError("lang is required for unregistered screen")
                self.channels[screen] = {"lang": lang, "can_hold": False, "_dynamic": True}
            ch = self.channels[screen]
            lang = lang or ch.get("lang")
            if rev is None:
                head = self.approved_head(cue, lang) if lang else None
                rev = head["rev"] if head else None
            rev = _opt_int(rev, "rev")
            if rev is None:
                raise ValidationError("rev is required when no approved head is available")
            self._ensure_cue(cue)

            claim = self._claim(screen, cue, rev, lang)
            if minute is not None:
                claim["minute"] = minute
            if sequence:
                claim["sequence"] = sequence
            if reference:
                claim["reference"] = reference
            if source not in claim["sources"]:
                claim["sources"].append(source)

            became_shown = False
            if canonical == "fetched":
                claim["fetched"] = True
            elif canonical == "queued":
                claim["fetched"] = True
                claim["queued"] = True
            elif canonical == "shown":
                claim["fetched"] = True
                if not claim["shown"]:
                    became_shown = True
                claim["shown"] = True

            if became_shown:
                fact_id = f"{screen}:{cue}:r{rev}"
                if not any(f["air_fact_id"] == fact_id for f in self.air_facts):
                    self.air_facts.append({
                        "air_fact_id": fact_id, "screen": screen, "cue": cue,
                        "lang": claim.get("lang"), "rev": rev, "minute": minute,
                        "sequence": sequence, "source": source, "reference": reference,
                    })
            elif canonical == "shown":
                # 回执补到了同一播出事实的 sequence/reference：补全审计字段，
                # 但绝不新增第二条事实，也不改变任何位置结论。
                fact_id = f"{screen}:{cue}:r{rev}"
                for fact in self.air_facts:
                    if fact["air_fact_id"] == fact_id:
                        if sequence and not fact.get("sequence"):
                            fact["sequence"] = sequence
                        if reference and not fact.get("reference"):
                            fact["reference"] = reference
                        break
            return dict(claim)

    # ---------------- 回执 / 发件箱 ---------------- #
    def register_outbox(self, reference, operation, recipient, payload=None):
        with self.lock:
            if reference in self.outbox:
                row = self.outbox[reference]
                if row["operation"] != operation or row["recipient"] != recipient:
                    raise Conflict("reference already used by another operation/recipient",
                                   reference=reference)
                return dict(row)
            row = {"reference": reference, "operation": operation, "recipient": recipient,
                   "status": "pending", "payload": payload or {}}
            self.outbox[reference] = row
            return dict(row)

    def mark_outbox(self, reference, status):
        with self.lock:
            if reference in self.outbox:
                self.outbox[reference]["status"] = status
                return dict(self.outbox[reference])
            return None

    def record_receipt(self, reference, operation, recipient, receipt):
        """记录合作方回执。相同 reference+内容幂等；异操作复用报 409。"""
        with self.lock:
            reference = _require_str(reference, "reference", max_len=96)
            operation = _require_str(operation, "operation", max_len=96)
            recipient = _require_str(recipient, "recipient", max_len=96)
            if not isinstance(receipt, dict):
                raise ValidationError("receipt must be an object")
            previous = self.receipts.get(reference)
            if previous is not None:
                if previous["operation"] != operation or previous["recipient"] != recipient:
                    raise Conflict("reference_reused", reference=reference)
                return {"reference": reference, "receipt": previous["receipt"],
                        "idempotent": True}

            self.receipts[reference] = {
                "operation": operation, "recipient": recipient, "receipt": receipt}
            if reference in self.outbox:
                self.outbox[reference]["status"] = "receipted"

            # publish-cue 回执本身就是播放端事实：shown 优先，其次 fetched。
            if operation == "publish-cue":
                cue = receipt.get("cue")
                rev = receipt.get("rev")
                if isinstance(cue, str) and rev is not None:
                    if receipt.get("shown"):
                        self.record_playback(
                            recipient, cue, "shown", rev=rev,
                            sequence=receipt.get("sequence"), source="receipt",
                            reference=reference)
                    elif receipt.get("fetched"):
                        self.record_playback(
                            recipient, cue, "fetched", rev=rev,
                            sequence=receipt.get("sequence"), source="receipt",
                            reference=reference)
            return {"reference": reference, "receipt": receipt, "idempotent": False}

    def get_receipt(self, reference):
        with self.lock:
            row = self.receipts.get(reference)
            if row is None:
                return None
            return {"reference": reference, "receipt": row["receipt"]}

    # ---------------- 事件接入 ---------------- #
    def apply(self, source, payload, minute=None):
        """把一条业务消息落到状态上，但不写事件日志（由 ingest/command 负责）。"""
        if source == "director":
            cue = payload.get("cue")
            decision = payload.get("decision") or payload.get("state")
            business_minute = payload.get("minute", minute)
            if decision == "skip":
                self.skip_cue(cue, minute=business_minute, reason=payload.get("reason"))
            elif payload.get("anchor_ms") is not None or decision in ("anchor", "anchored"):
                self.anchor_cue(cue, anchor_ms=payload.get("anchor_ms"),
                                minute=business_minute,
                                **{k: v for k, v in payload.items()
                                   if k in ("actual_minute", "note")})
            else:
                self.anchor_cue(cue, minute=business_minute)
        elif source in ("caption", "editor", "review"):
            if payload.get("term") and payload.get("rev") is None:
                self.confirm_term(payload["term"], payload.get("lang", "zh"),
                                  payload.get("translation"),
                                  state=payload.get("state", "approved"), minute=minute)
                return
            rec = self.upsert_caption(payload, minute=minute, source=source)
            for term in payload.get("terms", []) or []:
                if isinstance(term, dict) and term.get("term"):
                    self.confirm_term(term["term"], term.get("lang", payload.get("lang")),
                                      term.get("translation"), minute=minute)
                elif isinstance(term, str):
                    self.confirm_term(term, payload.get("lang", "zh"), None, minute=minute)
            return rec
        elif source == "playback":
            return self.record_playback(
                payload.get("screen"), payload.get("cue"), payload.get("state"),
                rev=payload.get("rev"), lang=payload.get("lang"),
                minute=payload.get("minute", minute),
                sequence=payload.get("sequence"), source=source)
        elif source == "receipt":
            return self.record_receipt(
                payload["reference"], payload.get("operation", "publish-cue"),
                payload.get("recipient", payload.get("screen")), payload.get("receipt", {}))
        # 未知来源不致命：仅记录日志，便于扩展。

    def command(self, source, payload, minute=None):
        """API 命令入口：应用变更并写一条事件日志。"""
        with self.lock:
            result = self.apply(source, payload, minute)
            entry = self.append_log(source, payload, minute)
            return {"entry": entry, "result": result}

    def ingest(self, event):
        """接入一条 fixture 风格事件（含 cursor/source/minute/payload）。

        带外部 cursor 的事件按 cursor 幂等；重复送达直接返回 duplicate。
        """
        with self.lock:
            if not isinstance(event, dict):
                raise ValidationError("event must be an object")
            external_cursor = event.get("cursor")
            if external_cursor is not None:
                external_cursor = _opt_int(external_cursor, "cursor")
                if external_cursor in self._external_cursors:
                    return {"duplicate": True, "cursor": external_cursor}
            source = _require_str(event.get("source"), "source", max_len=40)
            minute = event.get("minute")
            payload = event.get("payload")
            if not isinstance(payload, dict):
                raise ValidationError("event.payload must be an object")

            self.apply(source, payload, minute)
            entry = self.append_log(source, payload, minute)
            if external_cursor is not None:
                self._external_cursors.add(external_cursor)
                entry["external_cursor"] = external_cursor
            return entry

    def ingest_many(self, body):
        if isinstance(body, dict) and isinstance(body.get("events"), list):
            events = body["events"]
        elif isinstance(body, list):
            events = body
        elif isinstance(body, dict) and body.get("source"):
            events = [body]
        else:
            raise ValidationError("expected an event object, event list, or {events: [...]}")
        applied, duplicates = [], []
        for event in events:
            result = self.ingest(event)
            (duplicates if result.get("duplicate") else applied).append(result)
        return {"applied": len(applied), "duplicates": len(duplicates),
                "cursor": len(self.events_log), "events": applied}

    def read_events(self, after=0, limit=50):
        with self.lock:
            after = max(0, int(after))
            limit = min(100, max(1, int(limit)))
            rows = self.events_log[after:after + limit]
            next_cursor = rows[-1]["cursor"] if rows else after
            return {"events": rows, "next_cursor": next_cursor}

    # ---------------- 保障策略 / 可播解析 ---------------- #
    def policy_for(self, channel_id):
        ch = self.channels.get(channel_id, {})
        policy = ch.get("guarantee") or ch.get("policy") or {}
        on_missing = policy.get("on_missing") if isinstance(policy, dict) else None
        order = normalize_fallback(on_missing)
        if order is None:
            order = normalize_fallback(ch.get("on_missing") or ch.get("fallback"))
        if order is None:
            order = ["delay", "notice"] if ch.get("can_hold") else ["brief", "notice"]
        unavailable_text = None
        if isinstance(policy, dict):
            unavailable_text = policy.get("unavailable_text")
        return {
            "on_missing": order,
            "can_hold": bool(ch.get("can_hold")),
            "minimum_delay": ch.get("minimum_delay"),
            "unavailable_text": unavailable_text or ch.get("unavailable_text") or "字幕暂缺",
        }

    @staticmethod
    def _public_caption(c):
        return {"cue": c["cue"], "lang": c["lang"], "rev": c["rev"],
                "segment": c.get("segment") or None,
                "edition": c.get("edition"), "text": c.get("text"),
                "kind": c.get("kind", "full")}

    def resolve_cue(self, cue, channel_id, segment=None):
        """某渠道在某节目上现在能放什么：caption/brief/delay/notice/standby。"""
        with self.lock:
            ch = self.channels.get(channel_id)
            if ch is None:
                raise NotFound(f"unknown channel: {channel_id}")
            lang = ch.get("lang")
            cue_rec = self.cues.get(cue) or {"cue": cue, "status": "unknown"}
            policy = self.policy_for(channel_id)
            result = {
                "cue": cue, "lang": lang, "channel": channel_id,
                "actual_status": cue_rec.get("status"), "policy": policy["on_missing"],
            }
            if cue_rec.get("status") == "skipped":
                result.update({"action": "standby", "playable": False,
                               "reason": "cue_skipped_live_kept_as_alternative",
                               "caption": None})
                return result

            head = self.approved_head(cue, lang, segment=segment) if lang else None
            if head is not None:
                result.update({"action": "caption", "playable": True,
                               "rev": head["rev"], "caption": self._public_caption(head)})
                return result

            for action in policy["on_missing"]:
                if action == "brief":
                    brief = self.approved_head(cue, lang, segment=segment, kind="brief")
                    if brief is not None:
                        result.update({"action": "brief", "playable": True,
                                       "rev": brief["rev"],
                                       "caption": self._public_caption(brief),
                                       "note": "approved_brief_preview"})
                        return result
                elif action == "delay" and policy["can_hold"]:
                    result.update({"action": "delay", "playable": False,
                                   "minimum_delay": policy["minimum_delay"],
                                   "note": "wait_for_translation_under_delay_agreement"})
                    return result
                elif action == "notice":
                    result.update({"action": "notice", "playable": True,
                                   "text": policy["unavailable_text"],
                                   "note": "explicit_unavailable_notice"})
                    return result
            result.update({"action": "notice", "playable": True,
                           "text": policy["unavailable_text"]})
            return result

    # ---------------- 导播看板 ---------------- #
    def _channel_claim_view(self, screen, cue):
        rows = [c for c in self.claims.values() if c["screen"] == screen and c["cue"] == cue]
        if not rows:
            return None
        rows.sort(key=lambda c: c["rev"])
        active = rows[-1]
        head = self.approved_head(cue, active.get("lang")) if active.get("lang") else None
        any_shown = any(c["shown"] for c in rows)
        return {
            "held_revs": [c["rev"] for c in rows if c["fetched"]],
            "shown_revs": [c["rev"] for c in rows if c["shown"]],
            "active_rev": active["rev"],
            "shown": any_shown,
            "queued": active["queued"],
            "head_rev": head["rev"] if head else None,
            "update_available": bool(head and active["rev"] < head["rev"] and not any_shown),
            "modifiable": not any_shown,
            "sequence": active.get("sequence"),
        }

    def board(self):
        with self.lock:
            current = self.current_cue()
            channels_view = {}
            for screen in self.channels:
                ch = self.channels[screen]
                item = {"lang": ch.get("lang"), "can_hold": bool(ch.get("can_hold"))}
                if current:
                    item["now"] = self.resolve_cue(current, screen)
                    item["claim"] = self._channel_claim_view(screen, current)
                channels_view[screen] = item
            cues = [dict(c) for c in self.cues.values()]
            cues.sort(key=lambda c: self.cue_order(c["cue"]))
            return {
                "cursor": len(self.events_log),
                "current_cue": current,
                "cues": cues,
                "skipped_cues": [c["cue"] for c in cues if c["status"] == "skipped"],
                "channels": channels_view,
            }

    # ---------------- 播放点位置 / 可接续内容 ---------------- #
    def _screen_claims(self, screen):
        return [dict(c) for c in self.claims.values() if c["screen"] == screen]

    def _position_summary(self, screen):
        claims = [c for c in self.claims.values()
                  if c["screen"] == screen and c["fetched"]]
        if not claims:
            return None
        shown = [c for c in claims if c["shown"]]
        claims.sort(key=lambda c: (self.cue_order(c["cue"]), c["rev"]))
        shown.sort(key=lambda c: (self.cue_order(c["cue"]), c["rev"]))
        last = claims[-1]
        last_shown = shown[-1] if shown else None
        return {
            "cue": last["cue"], "rev": last["rev"], "lang": last.get("lang"),
            "sequence": last.get("sequence"), "minute": last.get("minute"),
            "shown": last["shown"],
            "last_shown_cue": last_shown["cue"] if last_shown else None,
            "last_shown_rev": last_shown["rev"] if last_shown else None,
            "order": self.cue_order(last["cue"]),
        }

    def continuation(self, screen):
        """该播放点可接续的内容；严格不回退到已越过的节目。"""
        with self.lock:
            self.channel(screen)
            lang = self.channels[screen].get("lang")

            shown_orders = [self.cue_order(c["cue"]) for c in self.claims.values()
                            if c["screen"] == screen and c["shown"]]
            shown_order = max(shown_orders) if shown_orders else -1

            # 未显示且严格位于“已显示位置”之后的领取记录。
            pending = {}
            for c in self.claims.values():
                if c["screen"] != screen or c["shown"] or not c["fetched"]:
                    continue
                order = self.cue_order(c["cue"])
                if order <= shown_order:
                    continue
                cur = pending.get(c["cue"])
                if cur is None or c["rev"] > cur["rev"]:
                    pending[c["cue"]] = c

            items = []
            handled_orders = set()
            for cue, claim in sorted(pending.items(), key=lambda kv: self.cue_order(kv[0])):
                order = self.cue_order(cue)
                handled_orders.add(order)
                head = self.approved_head(cue, lang or claim.get("lang"))
                head_rev = head["rev"] if head else None
                target_rev = head_rev or claim["rev"]
                items.append({
                    "cue": cue, "order": order,
                    "held_rev": claim["rev"], "rev": target_rev,
                    "replace": bool(head_rev and claim["rev"] < head_rev),
                    "modifiable": True,
                    "displayable": bool(head_rev and target_rev == head_rev),
                    "resolution": self.resolve_cue(cue, screen),
                })

            # 后续脚本节目：跳过的节目不进入直播接续队列（留作备选）。
            start_after = max(handled_orders, default=shown_order)
            for cue, order in sorted(self.cue_index.items(), key=lambda kv: kv[1]):
                if order <= start_after:
                    continue
                if self.cues.get(cue, {}).get("status") == "skipped":
                    continue
                resolution = self.resolve_cue(cue, screen)
                ready = resolution.get("action") in ("caption", "brief")
                items.append({
                    "cue": cue, "order": order, "held_rev": None,
                    "rev": resolution.get("rev") if ready else None,
                    "replace": False, "modifiable": False, "displayable": ready,
                    "resolution": resolution,
                })

            return {
                "screen": screen, "lang": lang,
                "after_shown_order": shown_order if shown_orders else None,
                "items": items,
                "next": items[0] if items else None,
                "end_of_program": not items,
            }

    def channel_position(self, screen):
        with self.lock:
            ch = self.channel(screen)
            summary = self._position_summary(screen)
            receipts_for = [{"reference": ref, **row}
                            for ref, row in self.receipts.items()
                            if row["recipient"] == screen]
            receipts_for.sort(key=lambda r: r["reference"])
            latest_receipt = receipts_for[-1] if receipts_for else None
            outbox_for = [dict(row) for row in self.outbox.values()
                          if row["recipient"] == screen]

            last_confirmed = None
            confirmed_via = None
            if latest_receipt:
                rc = latest_receipt["receipt"]
                last_confirmed = {
                    "cue": rc.get("cue"), "rev": rc.get("rev"),
                    "sequence": rc.get("sequence"),
                    "state": ("shown" if rc.get("shown")
                              else "fetched" if rc.get("fetched") else "unknown"),
                    "reference": latest_receipt["reference"],
                }
                confirmed_via = "receipt"
            elif summary is not None:
                last_confirmed = {
                    "cue": summary["cue"], "rev": summary["rev"],
                    "sequence": summary.get("sequence"),
                    "state": "shown" if summary["shown"] else "fetched",
                }
                confirmed_via = "playback_report"

            return {
                "screen": screen,
                "lang": ch.get("lang"),
                "guarantee": self.policy_for(screen),
                "last_confirmed_position": last_confirmed,
                "confirmed_via": confirmed_via,
                "last_report": summary,
                "last_receipt": latest_receipt,
                "receipts": receipts_for,
                "outbox": [{k: v for k, v in o.items() if k != "payload"} for o in outbox_for],
                "continuation": self.continuation(screen),
                "feedback_state": "confirmed" if (summary or latest_receipt) else "no_feedback",
            }

    # ---------------- 换班交接 ---------------- #
    def handover(self):
        with self.lock:
            channels = {}
            lang_confirmation = {}
            for screen in self.channels:
                ch = self.channels[screen]
                pos = self.channel_position(screen)
                claims = self._screen_claims(screen)
                claimed = [c for c in claims if c["fetched"]]
                shown = [c for c in claims if c["shown"]]
                modifiable = []
                for c in claimed:
                    if c["shown"]:
                        continue
                    head = self.approved_head(c["cue"], c.get("lang")) if c.get("lang") else None
                    item = dict(c)
                    item["head_rev"] = head["rev"] if head else None
                    item["update_available"] = bool(head and c["rev"] < head["rev"])
                    modifiable.append(item)
                uncertain = [
                    {"reference": o["reference"], "operation": o["operation"], "status": o["status"]}
                    for o in self.outbox.values()
                    if o["recipient"] == screen
                    and o["status"] in ("pending", "uncertain", "transport_lost")
                ]
                channels[screen] = {
                    "lang": ch.get("lang"),
                    "feedback_state": pos["feedback_state"],
                    "claimed": claimed,
                    "shown": shown,
                    "modifiable": modifiable,
                    "uncertain": uncertain,
                    "last_confirmed_position": pos["last_confirmed_position"],
                    "continuation": pos["continuation"],
                }
                lang = ch.get("lang")
                if lang:
                    bucket = lang_confirmation.setdefault(lang, {"shown": False, "screens": []})
                    bucket["screens"].append(screen)
                    if shown:
                        bucket["shown"] = True

            return {
                "cursor": len(self.events_log),
                "channels": channels,
                "languages": lang_confirmation,
                # 没有任何远端反馈绝不构成全局阻塞：各语言屏独立确认。
                "global_block": False,
                "air_facts": [dict(f) for f in self.air_facts],
            }

    # ---------------- 回看 / 审校 ---------------- #
    def program_versions_view(self):
        """解析引用某 cue/段落的节目版本，使校订沿引用关系生效。"""
        with self.lock:
            out = []
            for pv in self.program_versions:
                if not isinstance(pv, dict):
                    continue
                refs = pv.get("references") if isinstance(pv.get("references"), dict) else {}
                ref_cue = pv.get("cue") or refs.get("cue")
                segment = pv.get("segment") or refs.get("segment")
                lang = pv.get("lang")
                head = self.approved_head(ref_cue, lang, segment=segment) if ref_cue and lang else None
                out.append({
                    "id": pv.get("id"), "cue": ref_cue, "lang": lang, "segment": segment,
                    "replay_rev": head["rev"] if head else None,
                    "text": head["text"] if head else None,
                    "follows_current_approved": head is not None,
                })
            return out

    def replay_view(self, cue_filter=None, lang_filter=None):
        with self.lock:
            cue_ids = list(self.cues.keys())
            if cue_filter:
                cue_ids = [c for c in cue_ids if c == cue_filter]
            cue_ids.sort(key=self.cue_order)
            view = []
            for cue in cue_ids:
                rec = self.cues[cue]
                langs = set(rec.get("languages", []))
                langs.update(c["lang"] for c in self.captions.values() if c["cue"] == cue)
                if lang_filter:
                    langs = {lang_filter} & langs
                lang_views = {}
                for lang in sorted(langs):
                    versions = self.list_captions(cue=cue, lang=lang)
                    head = self.approved_head(cue, lang)
                    aired = [f for f in self.air_facts if f["cue"] == cue and f["lang"] == lang]
                    aired_revs = sorted({f["rev"] for f in aired})
                    if rec["status"] == "skipped":
                        usage = "standby"
                    elif head is None:
                        usage = "unavailable"
                    elif aired_revs and head["rev"] == max(aired_revs):
                        usage = "replay_as_aired"
                    else:
                        usage = "replay_corrected"
                    lang_views[lang] = {
                        "replay_rev": head["rev"] if head else None,
                        "replay_edition": self._public_caption(head) if head else None,
                        "usage": usage,
                        "aired_revs": aired_revs,
                        "air_facts": [dict(f) for f in aired],
                        "corrected_after_air": bool(aired_revs and head
                                                    and head["rev"] > max(aired_revs)),
                        "versions": versions,
                    }
                view.append({"cue": cue, "actual_status": rec["status"],
                             "standby": rec["status"] == "skipped",
                             "languages": lang_views})
            return {"replay": view, "program_versions": self.program_versions_view()}

    def replay_selection(self, cue, lang, mode="corrected", screen=None, rev=None):
        """回看选版：corrected（最新 approved 校订版，默认）或 aired（播出事实版）。"""
        with self.lock:
            if mode in ("aired", "as-aired", "as_aired"):
                facts = [f for f in self.air_facts if f["cue"] == cue and f["lang"] == lang]
                if screen:
                    facts = [f for f in facts if f["screen"] == screen]
                if rev is not None:
                    rev = int(rev)
                    facts = [f for f in facts if f["rev"] == rev]
                if not facts:
                    return {"cue": cue, "lang": lang, "mode": "aired",
                            "available": False, "reason": "no_matching_air_fact"}
                chosen_rev = rev if rev is not None else max(f["rev"] for f in facts)
                cap = self.captions.get((cue, lang, chosen_rev, ""))
                return {"cue": cue, "lang": lang, "mode": "aired", "available": cap is not None,
                        "rev": chosen_rev,
                        "edition": cap.get("edition") if cap else None,
                        "text": cap.get("text") if cap else None,
                        "air_facts": [dict(f) for f in facts]}
            head = self.approved_head(cue, lang)
            if head is None:
                return {"cue": cue, "lang": lang, "mode": "corrected",
                        "available": False, "reason": "no_approved_edition"}
            return {"cue": cue, "lang": lang, "mode": "corrected", "available": True,
                    **self._public_caption(head)}

    def replay_editions(self, cue_filter=None, lang_filter=None):
        """审校视角：哪些版次用于回看（扁平清单）。"""
        with self.lock:
            out = []
            for cue_view in self.replay_view(cue_filter, lang_filter)["replay"]:
                for lang, lv in cue_view["languages"].items():
                    head = lv["replay_edition"]
                    if cue_view["actual_status"] == "skipped":
                        out.append({
                            "cue": cue_view["cue"], "lang": lang, "usage": "standby",
                            "replay_rev": lv["replay_rev"],
                            "edition": head["edition"] if head else None,
                            "text": head["text"] if head else None,
                            "reason": ("skipped_live_kept_as_standby_alternative" if head
                                      else "skipped_live_no_approved_edition_standby_only"),
                        })
                        continue
                    if head is None:
                        out.append({
                            "cue": cue_view["cue"], "lang": lang, "usage": "unavailable",
                            "replay_rev": None, "edition": None, "text": None,
                            "reason": "no_approved_caption_edition",
                        })
                        continue
                    out.append({
                        "cue": cue_view["cue"], "lang": lang, "usage": lv["usage"],
                        "replay_rev": head["rev"], "edition": head.get("edition"),
                        "text": head.get("text"),
                        "corrected_after_air": lv["corrected_after_air"],
                        "aired_revs": lv["aired_revs"],
                        "reason": ("latest_approved_revision_for_replay"
                                   if lv["usage"] == "replay_corrected"
                                   else "matches_confirmed_air_fact"),
                    })
            return out


# --------------------------------------------------------------------------- #
# 合作方回执客户端（幂等 + lost_reply 重试 + GET 恢复）
# --------------------------------------------------------------------------- #
class ReceiptConflict(Exception):
    pass


class NoCounterpartyExample(Exception):
    pass


class ReceiptUncertain(Exception):
    pass


class ReceiptGatewayClient:
    def __init__(self, base_url=None, host="127.0.0.1", port=8765, timeout=2.0, attempts=3):
        if base_url:
            parts = urlsplit(base_url)
            host = parts.hostname or host
            port = parts.port or (443 if parts.scheme == "https" else 80)
        self.host = host
        self.port = port
        self.timeout = timeout
        self.attempts = attempts

    def _post_once(self, reference, operation, recipient):
        connection = HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            body = json.dumps({"reference": reference, "operation": operation,
                               "recipient": recipient}, ensure_ascii=False)
            connection.request("POST", "/v1/receipts", body.encode("utf-8"),
                               {"Content-Type": "application/json"})
            response = connection.getresponse()
            raw = response.read()
            data = json.loads(raw.decode("utf-8")) if raw else {}
            if response.status == 200:
                return data
            if response.status == 409:
                raise ReceiptConflict(data.get("code", "reference_reused"))
            if response.status == 422:
                raise NoCounterpartyExample(data.get("code", "no_counterparty_example"))
            raise BackendError(f"receipt gateway returned {response.status}",
                               status_code=response.status)
        finally:
            connection.close()

    def get_receipt(self, reference):
        connection = HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            connection.request("GET", f"/v1/receipts/{reference}")
            response = connection.getresponse()
            raw = response.read()
            if response.status == 404:
                return None
            return json.loads(raw.decode("utf-8"))
        finally:
            connection.close()

    def post_receipt(self, reference, operation, recipient):
        """相同 reference 安全重试；连接断开后重试 POST，再用 GET 恢复。"""
        last_error = None
        for _ in range(max(1, self.attempts)):
            try:
                return self._post_once(reference, operation, recipient)
            except ReceiptConflict:
                raise
            except NoCounterpartyExample:
                raise
            except (RemoteDisconnected, ConnectionError, TimeoutError, OSError) as exc:
                last_error = exc
                continue
        recovered = self.get_receipt(reference)
        if recovered is not None:
            return recovered
        raise ReceiptUncertain(str(last_error))


def sync_events_from_gateway(room, base_url, limit=100, timeout=5.0):
    """分页拉取 fixture gateway 的事件并按送达顺序接入。"""
    parts = urlsplit(base_url)
    host, port = parts.hostname, parts.port or 80
    after = 0
    summary = {"pages": 0, "events": 0}
    while True:
        conn = HTTPConnection(host, port, timeout=timeout)
        try:
            conn.request("GET", f"/v1/events?after={after}&limit={limit}")
            resp = conn.getresponse()
            data = json.loads(resp.read().decode("utf-8"))
            if resp.status != 200:
                raise BackendError(f"event gateway returned {resp.status}")
        finally:
            conn.close()
        events = data.get("events", [])
        if not events:
            break
        room.ingest_many(events)
        summary["events"] += len(events)
        summary["pages"] += 1
        new_after = data.get("next_cursor", after)
        if new_after == after:
            break
        after = new_after
    summary["cursor"] = after
    return summary


def publish_current_cue(room, screen, *, cue=None, rev=None, client=None,
                        operation="publish-cue"):
    """把某渠道当前节目的指定版次发给合作方并登记回执。

    没有 client 时只登记 pending outbox；有 client 时执行幂等 POST 并把回执
    事实写回 room。任何远端异常都被收敛为状态，不向其他渠道传播。
    """
    with room.lock:
        if cue is None:
            cue = room.current_cue()
        if cue is None:
            raise ValidationError("no current cue to publish")
        ch = room.channel(screen)
        lang = ch.get("lang")
        if rev is None:
            head = room.approved_head(cue, lang)
            if head is None:
                raise ValidationError(f"no approved caption for {cue}/{lang}")
            rev = head["rev"]
        rev = int(rev)
        reference = safe_reference("pub", screen, cue, "r" + str(rev))
        room.register_outbox(reference, operation, screen, {"cue": cue, "rev": rev})

    if client is None:
        return {"reference": reference, "status": "pending"}

    try:
        response = client.post_receipt(reference, operation, screen)
        recorded = room.record_receipt(reference, operation, screen, response["receipt"])
        return {"reference": reference, "status": "receipted",
                "receipt": response["receipt"], "recorded": recorded}
    except NoCounterpartyExample as exc:
        room.mark_outbox(reference, "no_counterparty")
        return {"reference": reference, "status": "no_counterparty", "detail": str(exc)}
    except ReceiptConflict as exc:
        room.mark_outbox(reference, "conflict")
        return {"reference": reference, "status": "conflict", "detail": str(exc)}
    except ReceiptUncertain as exc:
        room.mark_outbox(reference, "uncertain")
        return {"reference": reference, "status": "uncertain", "detail": str(exc)}


# --------------------------------------------------------------------------- #
# HTTP 服务
# --------------------------------------------------------------------------- #
class BackendServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, handler, room=None, client=None, gateway_url=None):
        super().__init__(addr, handler)
        self.room = room or ControlRoom()
        self.client = client
        self.gateway_url = gateway_url


def build_server(port=0, room=None, *, fixtures=None, replay_fixture_events=False,
                 gateway_url=None):
    if room is None:
        if fixtures:
            room = ControlRoom.from_fixtures(fixtures)
            if replay_fixture_events:
                room.replay_events(fixtures)
        else:
            room = ControlRoom()
    client = ReceiptGatewayClient(gateway_url) if gateway_url else None
    return BackendServer(("127.0.0.1", port), BackendHandler, room=room,
                         client=client, gateway_url=gateway_url)


class BackendHandler(BaseHTTPRequestHandler):
    server: BackendServer

    def _send(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _read_json(self):
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            size = 0
        if size <= 0 or size > 4_000_000:
            raise ValidationError("request body must be 1..4MB JSON")
        try:
            return json.loads(self.rfile.read(size).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValidationError("body must be valid JSON")

    def log_message(self, *args):
        return

    # -- GET -- #
    def do_GET(self):
        try:
            parts = urlsplit(self.path)
            path = parts.path.rstrip("/") or "/"
            query = parse_qs(parts.query)
            room = self.server.room
            one = lambda key, default=None: query.get(key, [default])[0]

            if path == "/" or path == "/v1":
                return self._send(200, {
                    "service": "village-parade-caption-backend",
                    "roles": {
                        "director": "GET /v1/board ; POST /v1/director/anchor|skip",
                        "editor": "GET /v1/replay?flat=1 ; POST /v1/reviews/caption|term",
                        "playback": "GET /v1/channels/<id>/position ; POST /v1/playback",
                        "handover": "GET /v1/handover",
                    },
                    "endpoints": [
                        "/healthz", "/v1/registers", "/v1/events", "/v1/board",
                        "/v1/replay", "/v1/editions", "/v1/channels",
                        "/v1/channels/<id>/position", "/v1/handover", "/v1/cues",
                        "/v1/captions", "/v1/terms", "/v1/air-facts",
                        "/v1/receipts/<reference>",
                        "POST /v1/events", "POST /v1/sync", "POST /v1/publish",
                        "POST /v1/receipts",
                    ],
                })
            if path == "/healthz":
                return self._send(200, {"ok": True})
            if path == "/v1/registers":
                return self._send(200, room.registers)
            if path in ("/v1/board", "/v1/now"):
                return self._send(200, room.board())
            if path == "/v1/handover":
                return self._send(200, room.handover())
            if path == "/v1/air-facts":
                return self._send(200, {"air_facts": [dict(f) for f in room.air_facts]})
            if path == "/v1/replay":
                cue, lang = one("cue"), one("lang")
                if one("flat", "0") in ("1", "true", "yes"):
                    return self._send(200, {"editions": room.replay_editions(cue, lang)})
                return self._send(200, room.replay_view(cue, lang))
            if path == "/v1/editions":
                return self._send(200, {"editions": room.replay_editions(one("cue"), one("lang"))})
            if path == "/v1/replay/select":
                cue, lang = one("cue"), one("lang")
                if not cue or not lang:
                    raise ValidationError("cue and lang are required")
                return self._send(200, room.replay_selection(
                    cue, lang, mode=one("mode", "corrected"),
                    screen=one("screen"), rev=one("rev")))
            if path == "/v1/channels":
                return self._send(200, {"channels": {cid: room.channel_position(cid)
                                                     for cid in room.channels}})
            match = re.fullmatch(r"/v1/channels/([^/]+)/position", path)
            if match:
                return self._send(200, room.channel_position(unquote(match.group(1))))
            if path == "/v1/cues":
                status = one("status")
                cues = [dict(c) for c in room.cues.values() if not status or c["status"] == status]
                cues.sort(key=lambda c: room.cue_order(c["cue"]))
                return self._send(200, {"cues": cues})
            if path == "/v1/captions":
                return self._send(200, {"captions": room.list_captions(
                    one("cue"), one("lang"), one("state"))})
            if path == "/v1/terms":
                return self._send(200, {"terms": room.list_terms(one("lang"))})
            if path == "/v1/events":
                return self._send(200, room.read_events(
                    int(one("after", "0")), int(one("limit", "50"))))
            match = re.fullmatch(r"/v1/receipts/([A-Za-z0-9_-]{1,96})", path)
            if match:
                row = room.get_receipt(match.group(1))
                if row is None:
                    raise NotFound("receipt not found")
                return self._send(200, row)
            return self._send(404, {"code": "not_found", "path": path})
        except BackendError as exc:
            return self._send(exc.status, {"code": exc.code, "message": exc.message,
                                           **exc.details})
        except (ValueError, KeyError) as exc:
            return self._send(400, {"code": "invalid_query", "message": str(exc)})

    # -- POST -- #
    def do_POST(self):
        try:
            path = urlsplit(self.path).path.rstrip("/")
            room = self.server.room
            body = self._read_json()

            if path == "/v1/events":
                return self._send(201, room.ingest_many(body))
            if path == "/v1/director/anchor":
                rec = room.anchor_cue(body.get("cue"), body.get("anchor_ms"),
                                       minute=body.get("minute"),
                                       **{k: v for k, v in body.items()
                                          if k in ("actual_minute", "note")})
                room.append_log("director", body, body.get("minute"))
                return self._send(201, rec)
            if path == "/v1/director/skip":
                rec = room.skip_cue(body.get("cue"), minute=body.get("minute"),
                                    reason=body.get("reason"))
                room.append_log("director", body, body.get("minute"))
                return self._send(201, rec)
            if path == "/v1/captions":
                rec = room.upsert_caption(body)
                room.append_log("caption", body, body.get("minute"))
                return self._send(201, rec)
            if path == "/v1/reviews/caption":
                rec = room.review_caption(
                    body.get("cue"), body.get("lang"), body.get("rev"),
                    text=body.get("text"), applies_to=body.get("applies_to"),
                    terms=body.get("terms"), minute=body.get("minute"),
                    edition=body.get("edition"))
                room.append_log("editor", body, body.get("minute"))
                return self._send(201, rec)
            if path == "/v1/reviews/term":
                rec = room.confirm_term(
                    body.get("term"), body.get("lang", "zh"), body.get("translation"),
                    state=body.get("state", "approved"), minute=body.get("minute"))
                room.append_log("editor", {"term": body.get("term"), "lang": body.get("lang")},
                                body.get("minute"))
                return self._send(201, rec)
            if path == "/v1/playback":
                rec = room.record_playback(
                    body.get("screen"), body.get("cue"), body.get("state"),
                    rev=body.get("rev"), lang=body.get("lang"), minute=body.get("minute"),
                    sequence=body.get("sequence"))
                room.append_log("playback", body, body.get("minute"))
                return self._send(201, rec)
            if path == "/v1/receipts":
                result = room.record_receipt(body.get("reference"), body.get("operation"),
                                             body.get("recipient"), body.get("receipt"))
                room.append_log("receipt", {"reference": body.get("reference")}, None)
                return self._send(200 if result.get("idempotent") else 201, result)
            if path == "/v1/sync":
                base_url = body.get("gateway_url") or self.server.gateway_url
                if not base_url:
                    raise ValidationError("gateway_url is required")
                return self._send(200, sync_events_from_gateway(room, base_url=base_url))
            if path == "/v1/publish":
                result = publish_current_cue(
                    room, body.get("channel") or body.get("screen"),
                    cue=body.get("cue"), rev=body.get("rev"), client=self.server.client)
                if result["status"] == "receipted":
                    return self._send(200, result)
                if result["status"] == "conflict":
                    return self._send(409, result)
                return self._send(202, result)
            return self._send(404, {"code": "not_found", "path": path})
        except BackendError as exc:
            return self._send(exc.status, {"code": exc.code, "message": exc.message,
                                           **exc.details})
        except (ValueError, KeyError, TypeError) as exc:
            return self._send(400, {"code": "invalid_request", "message": str(exc)})


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8787")))
    parser.add_argument("--fixtures", type=Path, default=FIXTURE_DIR)
    parser.add_argument("--no-replay", action="store_true",
                        help="不要在启动时回放 fixtures/events.ndjson")
    parser.add_argument("--gateway", default=None,
                        help="合作方 fixture gateway 地址，例如 http://127.0.0.1:8765")
    parser.add_argument("--sync", action="store_true",
                        help="启动时通过 gateway HTTP 拉取事件（而非本地 ndjson）")
    args = parser.parse_args(argv)

    if args.fixtures and (args.fixtures / "registers.json").exists():
        room = ControlRoom.from_fixtures(args.fixtures)
        if args.sync and args.gateway:
            summary = sync_events_from_gateway(room, base_url=args.gateway)
            print(f"synced {summary['events']} events from {args.gateway}", flush=True)
        elif not args.no_replay:
            room.replay_events(args.fixtures)
    else:
        room = ControlRoom()

    client = ReceiptGatewayClient(args.gateway) if args.gateway else None
    httpd = BackendServer((args.host, args.port), BackendHandler, room=room,
                          client=client, gateway_url=args.gateway)
    print(f"字幕后端 http://{args.host}:{httpd.server_port}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
