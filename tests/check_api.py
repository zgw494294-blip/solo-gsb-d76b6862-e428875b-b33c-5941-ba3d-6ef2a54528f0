"""端到端检查：需要一个已启动的 uvicorn（见下方 BASE）。"""
import gzip
import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

import httpx

BASE = os.environ.get("BASE", "http://127.0.0.1:8011")
DB_PATH = os.environ.get("CHECK_DB_PATH", "/tmp/ndjson_check/batches.db")
# tests/ 的上一级即仓库根（含 app/ 包），供“重启”子进程作为工作目录
WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
try:
    import uvicorn  # noqa: F401

    HAVE_UVICORN = True
except Exception:  # 运行检查脚本的解释器未必装了服务依赖
    HAVE_UVICORN = False

passed = 0
failed = []


def check(name, cond, extra=""):
    global passed
    if cond:
        passed += 1
        print(f"  PASS {name}")
    else:
        failed.append(name)
        print(f"  FAIL {name} {extra}")


def gz(data: bytes) -> bytes:
    return gzip.compress(data, mtime=0)


def ndjson(records, trailing_newline=True, raw_last: bytes | None = None) -> bytes:
    buf = b"".join(
        (json.dumps(r, ensure_ascii=False).encode() + b"\n") for r in records
    )
    if not trailing_newline:
        buf = buf[:-1]
    if raw_last is not None:
        buf += raw_last
    return buf


def list_batch_ids():
    r = httpx.get(f"{BASE}/batches", params={"limit": 1000})
    r.raise_for_status()
    return {b["batch_id"] for b in r.json()["items"]}


def search_checks(c: httpx.Client):
    print("== 日志检索 ==")
    # 每次运行唯一标记，保证结果不受历史数据影响
    mk = "ZQ" + uuid.uuid4().hex[:8]

    def srec(i, msg):
        return {"id": f"{mk}-{i}", "message": msg}

    # 两个批次，提交顺序即排列顺序；刻意制造同 id 跨批
    batch_a = [
        srec(1, f"{mk} Hello World"),
        {"id": "same-id", "message": f"{mk} batch-A row"},
        srec(3, f"{mk} 100% done_next a_b literal"),
        srec(4, f"{mk}  double  space"),
        {"id": "dup-id", "message": f"{mk} DUP first"},
        srec(6, f"{mk} zzq lowercase marker"),
    ]
    batch_b = [
        {"id": "dup-id", "message": f"{mk} DUP second"},
        srec(8, f"{mk} world is lowercase here"),
        {"id": "same-id", "message": f"{mk} batch-B row"},
    ]
    bid_a = c.post("/batches", content=gz(ndjson(batch_a))).json()["batch_id"]
    bid_b = c.post("/batches", content=gz(ndjson(batch_b))).json()["batch_id"]

    def search(params, **kw):
        return c.get(f"{BASE}/logs/search", params=params, **kw)

    # 基本检索：大小写敏感，按批次提交顺序、批内 seq 升序；同 id 跨批保留
    r = search({"q": mk, "limit": 100})
    check("search 200", r.status_code == 200, r.text)
    body = r.json()
    check("search fields",
          all(set(i) == {"batch_id", "seq", "id", "message"} for i in body["items"]),
          body)
    check("search item count", len(body["items"]) == 9,
          f"{len(body['items'])}")
    check("search order across batches",
          [(i["batch_id"], i["seq"]) for i in body["items"]]
          == [(bid_a, 1), (bid_a, 2), (bid_a, 3), (bid_a, 4), (bid_a, 5),
              (bid_a, 6), (bid_b, 1), (bid_b, 2), (bid_b, 3)],
          body["items"])
    check("same id kept across batches",
          [i["id"] for i in body["items"]].count("dup-id") == 2
          and [i["id"] for i in body["items"]].count("same-id") == 2)
    check("search last page cursor null", body["next_cursor"] is None)

    # 大小写敏感：World 只匹配 A 批第 1 行，world 只匹配 B 批第 2 行
    r = search({"q": f"{mk} Hello World"})
    check("case sensitive uppercase",
          [(i["batch_id"], i["seq"]) for i in r.json()["items"]]
          == [(bid_a, 1)], r.text)
    r = search({"q": "world"})
    check("case sensitive lowercase ignores other batches' data",
          [(i["batch_id"], i["seq"]) for i in r.json()["items"]
           if mk in i["message"]]
          == [(bid_b, 2)], r.text)
    r = search({"q": f"{mk} DUP"})
    check("case sensitive DUP two rows",
          len(r.json()["items"]) == 2, r.text)
    r = search({"q": f"{mk} dup"})
    check("case sensitive dup zero rows",
          r.json()["items"] == [] and r.json()["next_cursor"] is None, r.text)

    # % 和 _ 是字面子串，不是通配符
    r = search({"q": f"{mk} 100% done_next a_b"})
    check("literal percent/underscore",
          [(i["batch_id"], i["seq"]) for i in r.json()["items"]]
          == [(bid_a, 3)], r.text)
    r = search({"q": f"{mk} 100% done?next a_b"})
    check("no wildcard matching", r.json()["items"] == [], r.text)

    # 空白保留：连续两个空格命中，单个空格不命中；首尾空白也保留
    r = search({"q": f"{mk}  double  space"})
    check("inner double spaces preserved",
          [(i["batch_id"], i["seq"]) for i in r.json()["items"]]
          == [(bid_a, 4)], r.text)
    r = search({"q": f"{mk} double space"})
    check("single spaces no match", r.json()["items"] == [])
    r = search({"q": f"  {mk} Hello World   "})
    check("leading/trailing spaces part of term",
          r.json()["items"] == [])

    # 无匹配：空 items + null 游标
    r = search({"q": f"{mk}-definitely-absent"})
    check("no match empty", r.json() == {"items": [], "next_cursor": None}, r.text)

    # 分页：limit=2 共 5 页（9 行），游标逐页推进且内容与一次性查询一致
    r = search({"q": mk, "limit": 2})
    pages = [r.json()["items"]]
    cursors = [r.json()["next_cursor"]]
    check("first page size 2", len(pages[0]) == 2)
    while cursors[-1] is not None:
        r = search({"cursor": cursors[-1], "limit": 2})
        check("continuation 200", r.status_code == 200, r.text)
        pages.append(r.json()["items"])
        cursors.append(r.json()["next_cursor"])
    flat = [i for p in pages for i in p]
    check("pagination page sizes",
          [len(p) for p in pages] == [2, 2, 2, 2, 1],
          [len(p) for p in pages])
    check("pagination cursors then null",
          all(cur is not None for cur in cursors[:-1]) and cursors[-1] is None)
    check("pagination content consistent",
          [(i["batch_id"], i["seq"]) for i in flat]
          == [(bid_a, n) for n in range(1, 7)]
          + [(bid_b, n) for n in range(1, 4)],
          flat)

    # 续页改变页大小仍沿用原查询词与范围
    r = search({"q": mk, "limit": 4})
    cur = r.json()["next_cursor"]
    r = search({"cursor": cur, "limit": 100})
    check("continue with different page size",
          [(i["batch_id"], i["seq"]) for i in r.json()["items"]]
          == [(bid_a, 5), (bid_a, 6), (bid_b, 1), (bid_b, 2), (bid_b, 3)],
          r.text)

    # 翻页期间新提交批次不混入旧查询
    r = search({"q": mk, "limit": 3})  # 固定快照，停在 A 批 seq3
    cur = r.json()["next_cursor"]
    r_new = c.post(
        "/batches",
        content=gz(ndjson([{"id": "late", "message": f"{mk} late batch"}])),
    )
    late_bid = r_new.json()["batch_id"]
    r = search({"cursor": cur, "limit": 100})
    check("new batch excluded from paged query",
          late_bid not in {i["batch_id"] for i in r.json()["items"]},
          r.text)
    check("paged query continues within old snapshot",
          [(i["batch_id"], i["seq"]) for i in r.json()["items"]]
          == [(bid_a, 4), (bid_a, 5), (bid_a, 6), (bid_b, 1), (bid_b, 2),
              (bid_b, 3)],
          r.text)
    check("paged query next null at old snapshot end",
          r.json()["next_cursor"] is None)
    # 但新查询能看到新批次
    r = search({"q": f"{mk} late batch"})
    check("fresh query sees newly committed batch",
          [(i["batch_id"], i["seq"]) for i in r.json()["items"]]
          == [(late_bid, 1)], r.text)

    print("== 检索：暂存批次不参与 ==")
    # 慢上传进行中：其暂存内容不得被检索命中；快照也不含该批次。
    # 附带不可压缩随机尾巴，避免压缩体过小导致上传在取样前就结束。
    slow_text = f"{mk} slow staging 999"
    slow_payload = gz(b"".join(
        json.dumps({"id": f"{mk}-sl{i}",
                    "message": slow_text + os.urandom(300).hex()})
            .encode() + b"\n"
        for i in range(60)
    ))
    chunks = [slow_payload[i:i + 900]
              for i in range(0, len(slow_payload), 900)]
    result = {}

    def slow_post():
        try:
            with httpx.Client(base_url=BASE, timeout=60) as cc:
                result["r"] = cc.post(
                    "/batches",
                    content=(ch for ch in _gen_slow(chunks)))
        except Exception as e:  # noqa: BLE001
            result["e"] = e

    def _gen_slow(chunks):
        for ch in chunks:
            time.sleep(0.05)
            yield ch

    t = threading.Thread(target=slow_post)
    t.start()
    time.sleep(1.0)
    r = search({"q": slow_text})
    check("staging rows not searchable mid-flight",
          r.json()["items"] == [], r.text)
    t.join()
    check("slow upload ok", "r" in result and result["r"].status_code == 201,
          str(result.get("e", "")))
    slow_bid = result["r"].json()["batch_id"]
    r = search({"q": slow_text})
    check("slow batch searchable after commit",
          len(r.json()["items"]) == 60
          and {i["batch_id"] for i in r.json()["items"]} == {slow_bid},
          f"{len(r.json()['items'])}")

    print("== 检索：参数与游标错误 -> 422 ==")
    def expect422_search(name, params):
        r = search(params)
        check(name, r.status_code == 422, f"got {r.status_code} {r.text[:120]}")

    expect422_search("no q and no cursor", {})
    expect422_search("both q and cursor",
                     {"q": "x", "cursor": "v1.aaaa.bbbb"})
    expect422_search("empty q", {"q": ""})
    expect422_search("q over 200 chars", {"q": "x" * 201})
    expect422_search("limit zero", {"q": "x", "limit": 0})
    expect422_search("limit 101", {"q": "x", "limit": 101})
    expect422_search("garbage cursor", {"cursor": "not-a-cursor"})
    expect422_search("bad version cursor", {"cursor": "v2.aa.bb"})
    r = search({"q": mk, "limit": 3})
    if r.json()["next_cursor"]:
        good_cur = r.json()["next_cursor"]
        head, payload, sig = good_cur.split(".")
        expect422_search("tampered signature",
                         {"cursor": f"{head}.{payload}.{sig[:-1]}A"})
        expect422_search("tampered payload",
                         {"cursor": f"{head}.{payload[:-1]}A.{sig}"})
    else:
        check("obtain real cursor for tamper tests", False, "no cursor")
    expect422_search("cursor not base64",
                     {"cursor": "v1.!!!.bbb"})
    r = search({"q": "   "})
    check("whitespace-only q -> 200", r.status_code == 200, r.text)

    print("== 检索：游标在服务重启后仍可续用 ==")
    if not HAVE_UVICORN:
        check("restart continuation (skipped: no uvicorn in test interpreter)",
              True)
    else:
        port = 8023
        env = dict(os.environ)
        env["DB_PATH"] = DB_PATH
        env["PYTHONPATH"] = WORKSPACE_ROOT
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port)],
            cwd=WORKSPACE_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            base2 = f"http://127.0.0.1:{port}"
            deadline = time.time() + 20
            up = False
            while time.time() < deadline:
                try:
                    if httpx.get(f"{base2}/batches", timeout=1).status_code == 200:
                        up = True
                        break
                except httpx.HTTPError:
                    time.sleep(0.3)
            check("second instance started on same data dir", up)
            if up:
                with httpx.Client(base_url=base2, timeout=20) as c2:
                    r = c2.get(f"{base2}/logs/search",
                               params={"q": mk, "limit": 3})
                    check("new process queries same data",
                          r.status_code == 200 and len(r.json()["items"]) == 3,
                          r.text[:120])
                    cur2 = r.json()["next_cursor"]
                    check("new process issues cursor", cur2 is not None)
                    # 用原服务首请求签发的游标访问新进程（模拟重启后续页）：
                    # cursors[0] 指向 A 批 seq2，续页应为 A 批 seq3、seq4
                    r = c2.get(f"{base2}/logs/search",
                               params={"cursor": cursors[0], "limit": 2})
                    check("old cursor survives restart (page 2)",
                          r.status_code == 200
                          and [(i["batch_id"], i["seq"]) for i in r.json()["items"]]
                          == [(bid_a, 3), (bid_a, 4)],
                          r.text[:160])
                    # 反过来：新进程签发的游标原服务也能续用。该游标快照固定在
                    # 晚到批次/慢批次提交之后，但起点仍是 A 批 seq3
                    r = search({"cursor": cur2, "limit": 100})
                    check("cursor from restarted process usable",
                          r.status_code == 200
                          and [(i["batch_id"], i["seq"]) for i in r.json()["items"][:3]]
                          == [(bid_a, 4), (bid_a, 5), (bid_a, 6)],
                          r.text[:200])
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


def main():
    c = httpx.Client(base_url=BASE, timeout=30)

    print("== 正常路径 ==")
    before = list_batch_ids()
    recs = [
        {"id": "a", "message": "hello"},
        {"id": "b", "message": "世界"},
        {"id": "c", "message": "x" * 70000},
    ]
    # 第三行正文超过 64KiB？message 70000 字节的 ASCII，整行 > 64KiB，应 413
    r = c.post("/batches", content=gz(ndjson(recs)))
    check("line >64KiB -> 413", r.status_code == 413, r.text)
    check("failed upload not listed", list_batch_ids() == before)

    recs = [
        {"id": "a", "message": "hello"},
        {"id": "b", "message": "世界"},
        {"id": "c", "message": "ok"},
    ]
    r = c.post("/batches", content=gz(ndjson(recs, trailing_newline=False)))
    check("normal upload no trailing newline -> 201", r.status_code == 201, r.text)
    bid = r.json()["batch_id"]
    check("line_count", r.json()["line_count"] == 3)

    r = c.get(f"{BASE}/batches/{bid}/lines")
    check("read lines 200", r.status_code == 200)
    items = r.json()["items"]
    check(
        "order & content preserved",
        [(i["seq"], i["id"], i["message"]) for i in items]
        == [(1, "a", "hello"), (2, "b", "世界"), (3, "c", "ok")],
        items,
    )
    r = c.get(f"{BASE}/batches/{bid}/lines", params={"limit": 2, "offset": 1})
    check("pagination", [i["id"] for i in r.json()["items"]] == ["b", "c"])
    check("pagination total", r.json()["line_count"] == 3)

    r = c.get(f"{BASE}/batches/deadbeef/lines")
    check("unknown id -> 404", r.status_code == 404, r.status_code)

    r = c.get(f"{BASE}/batches")
    body = r.json()
    check("list has new batch", bid in {b["batch_id"] for b in body["items"]})
    check("list fields", all({"batch_id", "line_count", "created_at"} <= set(b)
                             for b in body["items"]))
    r = c.get(f"{BASE}/batches", params={"limit": 1})
    check("list pagination", len(r.json()["items"]) == 1)

    print("== 422 类 ==")
    def expect422(name, payload, headers=None):
        try:
            r = c.post("/batches", content=payload, headers=headers or {})
            check(name, r.status_code == 422, f"got {r.status_code} {r.text[:200]}")
        except httpx.HTTPError as e:
            check(name, False, f"client error {e}")

    expect422("empty body -> 422", b"")
    expect422("gzip of empty -> 422", gz(b""))
    expect422("corrupt gzip -> 422", b"\x1f\x8b\x08not really gzip")
    good = gz(ndjson([{"id": "a", "message": "m"}]))
    expect422("trailing data -> 422", good + b"X")
    expect422("truncated gzip -> 422", good[:-8])
    # 故意改坏 CRC（gzip 尾部倒数第 8 字节起）
    bad_crc = bytearray(good)
    bad_crc[-8] ^= 0xFF
    expect422("bad CRC -> 422", bytes(bad_crc))
    expect422("empty line -> 422", gz(b'{"id":"a","message":"m"}\n\n'))
    expect422("leading empty line -> 422", gz(b'\n{"id":"a","message":"m"}\n'))
    expect422("invalid JSON -> 422", gz(b'{not json\n'))
    expect422("invalid UTF-8 -> 422", gz(b'{"id":"a","message":"\xff"}\n'))
    expect422("missing key -> 422", gz(b'{"id":"a"}\n'))
    expect422("extra key -> 422", gz(b'{"id":"a","message":"m","x":1}\n'))
    expect422("empty id -> 422", gz(b'{"id":"","message":"m"}\n'))
    expect422("empty message -> 422", gz(b'{"id":"a","message":""}\n'))
    expect422("non-string id -> 422", gz(b'{"id":1,"message":"m"}\n'))
    expect422("null message -> 422", gz(b'{"id":"a","message":null}\n'))
    expect422("duplicate id in batch -> 422",
              gz(b'{"id":"a","message":"1"}\n{"id":"a","message":"2"}\n'))
    expect422("json number top-level -> 422", gz(b'123\n'))
    expect422("duplicate json key -> 422",
              gz(b'{"id":"a","id":"b","message":"m"}\n'))
    # 第二条记录坏：第一条不得变可见
    expect422("bad 2nd record -> 422",
              gz(b'{"id":"z1","message":"m"}\n{"id":"z2","message":}\n'))
    check("no zombie batch from failures", list_batch_ids() == before | {bid})

    print("== 413 类 ==")
    # 解压后 >8MiB（高压缩比，压缩体远小于 2MiB）；id 必须唯一
    big_lines = b"".join(
        b'{"id":"%07d","message":"aaaaaaaa"}\n' % i for i in range(290_000)
    )
    check("fixture decompressed size > 8MiB",
          len(big_lines) > 8 * 1024 * 1024, str(len(big_lines)))
    check("fixture compressed size < 2MiB",
          len(gz(big_lines)) <= 2 * 1024 * 1024, str(len(gz(big_lines))))
    r = c.post("/batches", content=gz(big_lines))
    check("decompressed >8MiB -> 413", r.status_code == 413, r.text[:120])

    # 边界：单行恰好 64KiB（整行字节）合法；用紧凑分隔符精确构造
    skeleton = json.dumps(
        {"id": "boundary", "message": ""}, separators=(",", ":")).encode()
    msg = "y" * (64 * 1024 - len(skeleton))
    raw = json.dumps(
        {"id": "boundary", "message": msg}, separators=(",", ":")).encode()
    check("fixture line exactly 64KiB", len(raw) == 64 * 1024, str(len(raw)))
    r = c.post("/batches", content=gz(raw))
    check("line exactly 64KiB -> 201", r.status_code == 201, r.text[:150])
    if r.status_code == 201:
        boundary_bid = r.json()["batch_id"]
    else:
        boundary_bid = None
    committed_so_far = before | {bid}
    if boundary_bid:
        committed_so_far |= {boundary_bid}

    # 单行 >64KiB（非 JSON 也先判字节）
    r = c.post("/batches", content=gz(b"x" * 70000 + b"\n"))
    check("single line >64KiB -> 413", r.status_code == 413, r.text[:120])

    # 压缩体 >2MiB：合法 gzip 封装的不可压缩数据（约 3MiB），服务端中途即停
    over = gz(os.urandom(3 * 1024 * 1024))
    try:
        r = c.post("/batches", content=over)
        check("compressed >2MiB -> 413",
              r.status_code == 413, f"got {r.status_code}")
    except httpx.HTTPError:
        # 服务端已立即回 413 并关连接，客户端写剩余 body 时可能感知到断连
        check("compressed >2MiB -> 413 (connection closed)", True)

    check("still no zombie batches", list_batch_ids() == committed_so_far)

    print("== 并发：慢上传不阻塞其它请求，且完成前不可见 ==")
    # 每条记录带不同的不可压缩内容：压缩后仍足够大，上传可持续数秒；
    # 总解压量约 500*1280B ≈ 640KB，接收中就会多次刷入独立暂存文件
    slow_payload = gz(b"".join(
        json.dumps({"id": f"s{i:06d}", "message": os.urandom(600).hex()})
            .encode() + b"\n"
        for i in range(500)
    ))
    chunks = [slow_payload[i:i + 1000]
              for i in range(0, len(slow_payload), 1000)]
    total_seconds = len(chunks) * 0.05
    check("slow upload lasts several seconds", total_seconds > 5,
          f"{total_seconds:.1f}s, {len(slow_payload)} compressed bytes")

    def gen():
        for ch in chunks:
            time.sleep(0.05)
            yield ch

    result = {}
    def slow_post():
        try:
            with httpx.Client(base_url=BASE, timeout=60) as cc:
                result["r"] = cc.post("/batches", content=gen())
        except Exception as e:
            result["e"] = e

    t = threading.Thread(target=slow_post)
    before_slow = list_batch_ids()
    t.start()
    time.sleep(1.5)  # 慢上传进行中
    ids_mid = list_batch_ids()
    check("no new batch visible mid-flight", ids_mid == before_slow,
          f"extra={ids_mid - before_slow}")
    # 每批次独占一个暂存 SQLite 文件；接收中应已创建并有内容落盘，
    # 但主库里绝不可见
    import glob as _glob
    stage_files = [p for p in _glob.glob(
        os.path.join(os.path.dirname(DB_PATH), "staging", "stage_*.db"))]
    stage_size = sum(os.path.getsize(p) for p in stage_files)
    check("dedicated staging file present mid-flight",
          len(stage_files) == 1 and stage_size > 0,
          f"files={stage_files} size={stage_size}")
    t0 = time.time()
    r1 = c.get("/batches")
    read_ms = (time.time() - t0) * 1000
    t0 = time.time()
    r2 = c.post("/batches",
                content=gz(ndjson([{"id": "fast1", "message": "go"}])))
    post_ms = (time.time() - t0) * 1000
    fast_bid = r2.json()["batch_id"]
    check("read during slow upload fast", read_ms < 3000, f"{read_ms:.0f}ms")
    check("another submit during slow upload fast -> 201",
          r2.status_code == 201 and post_ms < 3000,
          f"{r2.status_code} {post_ms:.0f}ms")
    ids_after_fast = list_batch_ids()
    check("fast batch visible, slow still invisible",
          ids_after_fast == before_slow | {fast_bid},
          f"extra={ids_after_fast - before_slow - {fast_bid}}")
    t.join()
    check("slow upload took its time", "r" in result
          and result["r"].status_code == 201,
          str(result.get("e", "")))
    slow_bid = result["r"].json()["batch_id"]
    check("slow batch visible after completion",
          slow_bid in list_batch_ids() and slow_bid != fast_bid)
    check("slow batch line count", result["r"].json()["line_count"] == 500)
    r = c.get(f"{BASE}/batches/{slow_bid}/lines", params={"limit": 3})
    check("slow batch lines ordered",
          [(i["seq"], i["id"]) for i in r.json()["items"]]
          == [(1, "s000000"), (2, "s000001"), (3, "s000002")])

    print("== 暂存清理 ==")
    import glob as _glob
    leftovers = _glob.glob(
        os.path.join(os.path.dirname(DB_PATH), "staging", "stage_*.db*"))
    check("no staging files left", leftovers == [], str(leftovers))
    conn = sqlite3.connect(DB_PATH)
    orphan_lines = conn.execute(
        "SELECT count(*) FROM lines WHERE batch_id NOT IN (SELECT id FROM batches)"
    ).fetchone()[0]
    check("no orphan lines", orphan_lines == 0, str(orphan_lines))
    conn.close()

    search_checks(c)

    print(f"\n{passed} passed, {len(failed)} failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
