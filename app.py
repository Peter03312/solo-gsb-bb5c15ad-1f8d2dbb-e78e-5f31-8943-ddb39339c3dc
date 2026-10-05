#!/usr/bin/env python3
"""共享厨房过敏原交叉污染排程系统

核心规则：
1. 同一台器具的已排入批次时段不得重叠，冲突批次拒绝排入并保留原因；
2. 器具上前一批次留下的残留过敏原中，凡不在本批次配方内的，均构成
   交叉污染风险，切换前必须存在一条清洁记录满足：
   - 清洁时间在前一批次结束之后、本批次开始之前；
   - 清洁有效期（valid_until）覆盖本批次开始时间；
   - 清洁范围覆盖全部新增残留源（或标记为“全部”）；
   缺少清洁 / 清洁失效 / 覆盖不足时，批次被阻塞并保留阻塞原因；
3. 被阻塞批次不占用器具、不产生残留；调整顺序后重新计算即可。
"""
import csv
import io
import json
import os
import sqlite3
from datetime import datetime

from flask import (Flask, Response, abort, flash, g, redirect,
                   render_template, request, url_for)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data", "scheduler.db"))
PORT = int(os.environ.get("PORT", "8000"))
APP_URL = os.environ.get("APP_URL", f"http://localhost:{PORT}")

ALLERGENS = ["花生", "坚果", "牛奶", "鸡蛋", "麸质", "大豆",
             "鱼类", "贝类", "芝麻", "芹菜", "芥末", "亚硫酸盐"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS equipment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS batch (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    allergens TEXT NOT NULL DEFAULT '',
    planned_start TEXT NOT NULL,
    planned_end TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS batch_equipment (
    batch_id INTEGER NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
    equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
    PRIMARY KEY (batch_id, equipment_id)
);
CREATE TABLE IF NOT EXISTS cleaning (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
    cleaned_at TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    covers TEXT NOT NULL DEFAULT '*'
);
CREATE TABLE IF NOT EXISTS allergen_revision (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
    allergens_before TEXT NOT NULL DEFAULT '',
    allergens_after TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_allergen_revision_batch
    ON allergen_revision(batch_id, changed_at, id);
"""

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "allergen-scheduler-dev-key")


# ---------------------------------------------------------------- 数据库

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    dirpath = os.path.dirname(DB_PATH)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    seed_if_empty(conn)
    conn.close()


def seed_if_empty(conn):
    """首次启动写入演示数据：覆盖 已排入/已缓解/清洁失效/覆盖不足/时段冲突 各情形。"""
    if conn.execute("SELECT COUNT(*) AS c FROM equipment").fetchone()["c"]:
        return
    today = datetime.now().date()

    def t(h, m=0):
        return datetime(today.year, today.month, today.day, h, m).isoformat(timespec="minutes")

    for name in ["台面A", "台面B", "搅拌机", "烤箱"]:
        conn.execute("INSERT INTO equipment(name) VALUES (?)", (name,))
    eq = {r["name"]: r["id"] for r in conn.execute("SELECT * FROM equipment")}

    batches = [
        ("牛奶燕麦糊", "牛奶,麸质", "台面A", t(8), t(9)),
        ("花生能量棒", "花生,大豆", "台面A", t(9, 30), t(10, 30)),
        ("芝麻饼干", "芝麻,麸质", "台面A", t(11), t(12)),
        ("清蒸鲈鱼", "鱼类", "台面B", t(8, 30), t(9, 30)),
        ("贝类意面", "贝类,麸质", "台面B", t(9), t(10)),
        ("香蕉奶昔", "牛奶", "搅拌机", t(8), t(8, 30)),
        ("杏仁奶", "坚果", "搅拌机", t(9), t(9, 30)),
    ]
    for i, (name, alg, eqname, s, e) in enumerate(batches, 1):
        cur = conn.execute(
            "INSERT INTO batch(name, allergens, planned_start, planned_end, seq)"
            " VALUES (?,?,?,?,?)", (name, alg, s, e, i))
        conn.execute("INSERT INTO batch_equipment(batch_id, equipment_id) VALUES (?,?)",
                     (cur.lastrowid, eq[eqname]))

    cleanings = [
        ("台面A", t(9), t(12), "牛奶,麸质"),      # 覆盖 B1→B2 的切换
        ("台面A", t(10, 30), t(11, 30), "花生"),   # 只覆盖花生，未覆盖大豆 → B3 阻塞
        ("搅拌机", t(8, 30), t(8, 45), "*"),        # 09:00 前已失效 → B7 阻塞
    ]
    for eqname, c, v, covers in cleanings:
        conn.execute(
            "INSERT INTO cleaning(equipment_id, cleaned_at, valid_until, covers)"
            " VALUES (?,?,?,?)", (eq[eqname], c, v, covers))
    conn.commit()


# ---------------------------------------------------------------- 工具

def parse_dt(s):
    try:
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def split_allergens(s):
    return {a.strip() for a in (s or "").split(",") if a.strip()}


def join_alg(lst):
    return "、".join(lst) if lst else "无"


def canonical_allergens(values):
    """按系统过敏原目录排序的规范化集合字符串（逗号连接），顺序差异不构成实际变化。"""
    chosen = {v.strip() for v in values if v.strip() in ALLERGENS}
    return [a for a in ALLERGENS if a in chosen], chosen


def set_diff_text(before, after):
    before, after = set(before), set(after)
    added = sorted(after - before, key=ALLERGENS.index)
    removed = sorted(before - after, key=ALLERGENS.index)
    parts = []
    if added:
        parts.append(f"新增 {join_alg(added)}")
    if removed:
        parts.append(f"移除 {join_alg(removed)}")
    return "；".join(parts)


def revision_note(rev):
    """最近一次过敏原变更的说明，用于排程风险标注与导出。"""
    if not rev:
        return ""
    before = split_allergens(rev["allergens_before"])
    after = split_allergens(rev["allergens_after"])
    return (f"📝 过敏原配方最近一次变更（{rev['changed_at'].replace('T', ' ')}）："
            f"{set_diff_text(before, after)}；原因：{rev['reason']}")


def latest_revision_map(conn, batch_ids=None):
    """返回 batch_id -> 最近一条修订（按操作者填写时间，并列时取较晚录入的一条）。"""
    if batch_ids is not None and not batch_ids:
        return {}
    rows = conn.execute(
        "SELECT * FROM allergen_revision ORDER BY changed_at, id").fetchall()
    latest = {}
    for r in rows:  # 升序遍历，后者覆盖前者，最终留下每批次的最近一条
        if batch_ids is None or r["batch_id"] in batch_ids:
            latest[r["batch_id"]] = r
    return latest


def risk_source_text(risk):
    return (f"器具「{risk['equipment']}」残留 {join_alg(risk['residual'])}"
            f"（来自批次「{risk['from_batch']}」）")


def risk_action_text(risk):
    if risk["level"] == "none":
        return "残留过敏原均在本批次配方内，无交叉污染风险"
    if risk["level"] == "mitigated":
        c = risk["cleaning"]
        covers = "全部过敏原" if c["covers"].strip() == "*" else c["covers"]
        return (f"新增风险 {join_alg(risk['new_risks'])}，已由清洁 #{c['id']} 覆盖"
                f"（{c['cleaned_at']} 清洁，有效至 {c['valid_until']}，范围：{covers}）")
    return risk["block_reason"]


# ---------------------------------------------------------------- 排程引擎

def compute_schedule(conn):
    """按当前顺序（seq, 计划开始时间）逐个尝试排入，返回每批次的结果。"""
    eq_names = {r["id"]: r["name"] for r in conn.execute("SELECT * FROM equipment")}
    equip_of = {}
    for r in conn.execute("SELECT * FROM batch_equipment"):
        equip_of.setdefault(r["batch_id"], []).append(r["equipment_id"])
    cleanings = {}
    for r in conn.execute("SELECT * FROM cleaning ORDER BY cleaned_at, id"):
        c = dict(r)
        c["cleaned_dt"] = parse_dt(c["cleaned_at"])
        c["valid_dt"] = parse_dt(c["valid_until"])
        cleanings.setdefault(c["equipment_id"], []).append(c)

    usage = {}  # equipment_id -> [已排入批次的占用记录]
    results = []
    rows = conn.execute(
        "SELECT * FROM batch ORDER BY seq, planned_start, id").fetchall()
    latest_rev = latest_revision_map(conn, [r["id"] for r in rows])
    for row in rows:
        start = parse_dt(row["planned_start"])
        end = parse_dt(row["planned_end"])
        b_allergens = split_allergens(row["allergens"])
        eqs = sorted(equip_of.get(row["id"], []))
        blocks, risks = [], []

        if start is None or end is None:
            blocks.append("时段格式无效，请使用 YYYY-MM-DDTHH:MM")
        elif end <= start:
            blocks.append("时段无效：结束时间必须晚于开始时间")
        elif not eqs:
            blocks.append("未指定台面/器具，无法排入")
        else:
            for eq in eqs:
                eq_name = eq_names.get(eq, f"#{eq}")
                usages = usage.get(eq, [])
                conflicts = [u for u in usages
                             if u["start"] < end and start < u["end"]]
                if conflicts:
                    names = "、".join(f"「{u['batch_name']}」" for u in conflicts)
                    blocks.append(
                        f"器具「{eq_name}」时段冲突：与已排入批次 {names} 时间重叠")
                    continue
                prevs = [u for u in usages if u["end"] <= start]
                if not prevs:
                    continue  # 该器具上无前置批次，无残留
                prev = max(prevs, key=lambda u: u["end"])
                residual = sorted(prev["allergens"])
                new_risks = sorted(prev["allergens"] - b_allergens)
                risk = {"equipment": eq_name, "from_batch": prev["batch_name"],
                        "residual": residual, "new_risks": new_risks,
                        "cleaning": None, "level": "none", "block_reason": None}
                if not new_risks:
                    risks.append(risk)
                    continue
                # 切换过敏原：需要覆盖全部残留源的有效清洁
                cand = [c for c in cleanings.get(eq, [])
                        if c["cleaned_dt"] and c["valid_dt"]
                        and prev["end"] <= c["cleaned_dt"] <= start]
                valid = [c for c in cand if c["valid_dt"] >= start]
                covering = [c for c in valid
                            if c["covers"].strip() == "*"
                            or set(new_risks) <= split_allergens(c["covers"])]
                src = join_alg(new_risks)
                if covering:
                    risk["level"] = "mitigated"
                    risk["cleaning"] = covering[-1]
                else:
                    risk["level"] = "blocked"
                    if not cand:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"切换过敏原前缺少覆盖全部残留源的清洁记录")
                    elif not valid:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"清洁已失效（有效期早于本批次开始时间）")
                    else:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"现有清洁未覆盖全部残留源")
                    risk["block_reason"] = reason
                    blocks.append(reason)
                risks.append(risk)

        status = "blocked" if blocks else "accepted"
        if status == "accepted":  # 只有排入成功的批次才占用器具、留下残留
            for eq in eqs:
                usage.setdefault(eq, []).append({
                    "start": start, "end": end,
                    "batch_name": row["name"], "allergens": b_allergens})
        rev = latest_rev.get(row["id"])
        results.append({
            "id": row["id"], "name": row["name"],
            "allergens": sorted(b_allergens),
            "equipment_names": [eq_names.get(e, f"#{e}") for e in eqs],
            "start": start, "end": end,
            "status": status, "risks": risks, "blocks": blocks,
            "revision": dict(rev) if rev is not None else None,
            "revision_note": revision_note(rev) if rev is not None else "",
        })
    return results


def normalize_seq(conn):
    rows = conn.execute(
        "SELECT id FROM batch ORDER BY seq, planned_start, id").fetchall()
    for i, r in enumerate(rows, 1):
        conn.execute("UPDATE batch SET seq=? WHERE id=?", (i, r["id"]))
    conn.commit()


# ---------------------------------------------------------------- 页面路由

@app.context_processor
def inject_globals():
    return {"app_url": APP_URL, "port": PORT, "allergen_options": ALLERGENS,
            "risk_source_text": risk_source_text,
            "risk_action_text": risk_action_text,
            "set_diff_text": set_diff_text,
            "revision_note": revision_note,
            "join_alg": join_alg}


@app.route("/")
def index():
    results = compute_schedule(get_db())
    accepted = sum(1 for r in results if r["status"] == "accepted")
    return render_template("index.html", results=results,
                           accepted=accepted,
                           blocked_count=len(results) - accepted)


@app.post("/recalculate")
def recalculate():
    flash("已按当前顺序、器具占用与清洁有效期重新计算排程", "ok")
    return redirect(url_for("index"))


@app.route("/batches", methods=["GET", "POST"])
def batches():
    conn = get_db()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        allergens = request.form.getlist("allergens")
        equipment = request.form.getlist("equipment")
        start = request.form.get("planned_start", "")
        end = request.form.get("planned_end", "")
        if not name or not start or not end:
            flash("请填写批次名称与预计时段", "error")
        else:
            seq = conn.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS s FROM batch").fetchone()["s"]
            cur = conn.execute(
                "INSERT INTO batch(name, allergens, planned_start, planned_end, seq)"
                " VALUES (?,?,?,?,?)",
                (name, ",".join(allergens), start, end, seq))
            for eq in equipment:
                conn.execute("INSERT INTO batch_equipment VALUES (?,?)",
                             (cur.lastrowid, int(eq)))
            conn.commit()
            flash(f"批次「{name}」已录入，当前排第 {seq} 位", "ok")
        return redirect(url_for("batches"))
    rows = conn.execute("SELECT * FROM batch ORDER BY seq").fetchall()
    eq_names = {r["id"]: r["name"]
                for r in conn.execute("SELECT * FROM equipment")}
    equip_of = {}
    for r in conn.execute("SELECT * FROM batch_equipment"):
        equip_of.setdefault(r["batch_id"], []).append(eq_names.get(r["equipment_id"]))
    equipment = conn.execute("SELECT * FROM equipment ORDER BY id").fetchall()
    revisions = {}
    for r in conn.execute(
            "SELECT * FROM allergen_revision ORDER BY changed_at DESC, id DESC"):
        revisions.setdefault(r["batch_id"], []).append(r)
    return render_template("batches.html", rows=rows, equipment=equipment,
                           equip_of=equip_of, revisions=revisions,
                           now_minute=datetime.now().isoformat(timespec="minutes"))


@app.post("/batches/<int:bid>/allergens")
def revise_allergens(bid):
    """修订已录入批次的配方过敏原集合。

    必须填写变更原因；无实际变化（仅勾选顺序差异不算）或原因为空时拒绝保存。
    修订只更新 batch.allergens，不影响批次顺序、器具关联与清洁记录。
    """
    conn = get_db()
    row = conn.execute("SELECT * FROM batch WHERE id=?", (bid,)).fetchone()
    if row is None:
        abort(404)

    reason = request.form.get("reason", "").strip()
    changed_at_raw = request.form.get("changed_at", "").strip()
    new_order, new_set = canonical_allergens(request.form.getlist("allergens"))
    old_set = split_allergens(row["allergens"])

    if not reason:
        flash("修改过敏原必须填写变更原因", "error")
    elif parse_dt(changed_at_raw) is None:
        flash("请填写有效的变更时间", "error")
    elif new_set == old_set:
        flash("过敏原集合没有实际变化，未保存修订", "error")
    else:
        changed_at = parse_dt(changed_at_raw).isoformat(timespec="minutes")
        before_str = ",".join(a for a in ALLERGENS if a in old_set)
        after_str = ",".join(new_order)
        conn.execute(
            "UPDATE batch SET allergens=? WHERE id=?", (after_str, bid))
        conn.execute(
            "INSERT INTO allergen_revision(batch_id, allergens_before,"
            " allergens_after, reason, changed_at, recorded_at)"
            " VALUES (?,?,?,?,?,?)",
            (bid, before_str, after_str, reason, changed_at,
             datetime.now().isoformat(timespec="seconds")))
        conn.commit()
        flash(f"批次「{row['name']}」过敏原已修订（{set_diff_text(old_set, new_set)}），"
              f"下次重新计算将使用最新集合", "ok")
        return redirect(url_for("batches", _anchor=f"rev-{bid}"))
    return redirect(url_for("batches", _anchor=f"rev-{bid}"))


@app.post("/batches/<int:bid>/delete")
def delete_batch(bid):
    conn = get_db()
    conn.execute("DELETE FROM batch WHERE id=?", (bid,))
    conn.commit()
    normalize_seq(conn)
    flash("批次已删除", "ok")
    return redirect(url_for("batches"))


@app.post("/batches/<int:bid>/move/<direction>")
def move_batch(bid, direction):
    conn = get_db()
    normalize_seq(conn)
    rows = conn.execute("SELECT id, seq FROM batch ORDER BY seq").fetchall()
    ids = [r["id"] for r in rows]
    if bid in ids:
        i = ids.index(bid)
        j = i - 1 if direction == "up" else i + 1
        if 0 <= j < len(ids):
            a, b = rows[i], rows[j]
            conn.execute("UPDATE batch SET seq=? WHERE id=?", (b["seq"], a["id"]))
            conn.execute("UPDATE batch SET seq=? WHERE id=?", (a["seq"], b["id"]))
            conn.commit()
            flash("已调整顺序并重新计算排程", "ok")
    return redirect(url_for("index"))


@app.route("/equipment", methods=["GET", "POST"])
def equipment_page():
    conn = get_db()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if name:
            try:
                conn.execute("INSERT INTO equipment(name) VALUES (?)", (name,))
                conn.commit()
                flash(f"器具「{name}」已添加", "ok")
            except sqlite3.IntegrityError:
                flash(f"器具「{name}」已存在", "error")
        return redirect(url_for("equipment_page"))
    equipment = conn.execute("SELECT * FROM equipment ORDER BY id").fetchall()
    cleanings = conn.execute(
        "SELECT c.*, e.name AS equipment_name FROM cleaning c"
        " JOIN equipment e ON e.id = c.equipment_id"
        " ORDER BY c.cleaned_at DESC, c.id DESC").fetchall()
    return render_template("equipment.html", equipment=equipment,
                           cleanings=cleanings)


@app.post("/equipment/<int:eid>/delete")
def delete_equipment(eid):
    conn = get_db()
    conn.execute("DELETE FROM equipment WHERE id=?", (eid,))
    conn.commit()
    flash("器具已删除（其清洁记录与批次关联一并移除）", "ok")
    return redirect(url_for("equipment_page"))


@app.post("/cleanings")
def add_cleaning():
    conn = get_db()
    equipment_id = request.form.get("equipment_id")
    cleaned_at = request.form.get("cleaned_at", "")
    valid_until = request.form.get("valid_until", "")
    covers = request.form.getlist("covers")
    cover_all = request.form.get("cover_all")
    if not (equipment_id and cleaned_at and valid_until):
        flash("请完整填写清洁记录（器具、清洁时间、有效至）", "error")
    else:
        covers_str = "*" if cover_all else ",".join(covers)
        if not covers_str:
            flash("请选择清洁覆盖的过敏原，或勾选“全部过敏原”", "error")
        else:
            conn.execute(
                "INSERT INTO cleaning(equipment_id, cleaned_at, valid_until, covers)"
                " VALUES (?,?,?,?)",
                (equipment_id, cleaned_at, valid_until, covers_str))
            conn.commit()
            flash("清洁记录已登记", "ok")
    return redirect(url_for("equipment_page"))


@app.post("/cleanings/<int:cid>/delete")
def delete_cleaning(cid):
    conn = get_db()
    conn.execute("DELETE FROM cleaning WHERE id=?", (cid,))
    conn.commit()
    flash("清洁记录已删除", "ok")
    return redirect(url_for("equipment_page"))


# ---------------------------------------------------------------- 导出

def export_rows(results):
    """把排程结果展开为可导出的行。"""
    rows = []
    for i, r in enumerate(results, 1):
        base = {
            "order": i,
            "batch": r["name"],
            "allergens": r["allergens"],
            "equipment": r["equipment_names"],
            "planned_start": r["start"].isoformat(timespec="minutes") if r["start"] else "",
            "planned_end": r["end"].isoformat(timespec="minutes") if r["end"] else "",
            "status": "已排入" if r["status"] == "accepted" else "已拒绝",
            "revision_note": r.get("revision_note", ""),
        }
        if r["risks"]:
            for risk in r["risks"]:
                row = dict(base)
                row["risk_source"] = risk_source_text(risk)
                row["action"] = risk_action_text(risk)
                rows.append(row)
        else:
            base["risk_source"] = "无前置残留" if r["status"] == "accepted" else "—"
            base["action"] = "；".join(r["blocks"]) if r["blocks"] else "无交叉污染风险"
            rows.append(base)
    return rows


@app.route("/export.csv")
def export_csv():
    results = compute_schedule(get_db())
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["顺序", "批次", "本批次过敏原", "使用器具", "计划开始", "计划结束",
                "状态", "风险来源", "处置 / 阻塞原因", "最近过敏原变更"])
    for row in export_rows(results):
        w.writerow([row["order"], row["batch"], join_alg(row["allergens"]),
                    "、".join(row["equipment"]), row["planned_start"],
                    row["planned_end"], row["status"],
                    row["risk_source"], row["action"], row["revision_note"]])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=schedule_risk.csv"})


@app.route("/export.json")
def export_json():
    results = compute_schedule(get_db())
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "app_url": APP_URL,
        "schedule": export_rows(results),
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=schedule_risk.json"})


# ---------------------------------------------------------------- 启动

init_db()

if __name__ == "__main__":
    print(f" * 共享厨房过敏原排程系统")
    print(f" * 访问地址: {APP_URL}  (监听端口 {PORT})")
    app.run(host="0.0.0.0", port=PORT)
