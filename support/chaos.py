#!/usr/bin/env python3
"""The chaos run behind the article: a prologue, then four versions under the same load and the same chaos.

    mise run demo                                   # prologue and all four scenarios
    mise run scenario -- outbox+inbox [--seed 42]   # one scenario

Each scenario, top to bottom: reset, start two instances per app, load plus chaos, drain, measure, print and write
results. The seed fixes the chaos schedule and the crash draws, not the timing. Exits 1 if any gate is red.
"""
import argparse
import hashlib
import http.client
import json
import random
import re
import shutil
import socket
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS, LOGS = ROOT / "results", ROOT / ".logs"
TOXIPROXY = "http://127.0.0.1:8474"
DEFAULT_SEED = 42  # the seed of the four-row table; any other seed goes to results/<scenario>.seed-<n>.json
SALES, RATE, THREADS = 2000, 40, 250
DRAIN_BOUND, TOXIC_SECONDS = 120, 6
LOYALTY = [("loyalty-1", 8091, 21011), ("loyalty-2", 8092, 21012)]  # name, http port, its toxiproxy port
BARISTA = [("barista-1", 8081, 21001), ("barista-2", 8082, 21002)]
# crash point and probability per pass, by barista mode; loyalty halts at its point in every scenario
CRASH = {"after-commit": ("after-commit-before-send", 0.0025), "in-transaction": ("after-send-before-commit", 0.0025),
         "outbox": ("after-accept-before-complete", 0.002)}
LOYALTY_CRASH = ("after-commit-before-settle", 0.0025)
FAILURE = {"after-commit+naive": "lost", "in-transaction+naive": "ghosts", "outbox+naive": "double-applied"}
SCENARIOS = [*FAILURE, "outbox+inbox"]
COLUMNS = ["committed", "lost", "ghosts", "double-applied", "duplicates delivered", "hash mismatch"]
# over the stamp table: surplus stamp rows (what naive applied twice) plus what the inbox counted and suppressed
DUPLICATES = "COUNT(*) - COUNT(DISTINCT message_id) + (SELECT COALESCE(SUM(duplicates), 0) FROM inbox)"
HALT = re.compile(r"^halt\(137\) \S+ at (\S+), (?:sale|message) (\S+)$", re.M)
DUPLICATE = re.compile(r"Message \S+ duplicate on delivery (\d+)")
HASH_PARKED = re.compile(r"Message (\S+) parked: HASH_MISMATCH")
START, END = "<!-- results:start -->", "<!-- results:end -->"

TTY = sys.stdout.isatty()
lock = threading.Lock()
status_line, status_printed, transcript = "", 0.0, None


def say(text=""):
    """A line that stays; on a terminal the status line is redrawn below it."""
    with lock:
        sys.stdout.write(f"\r\033[K{text}\n{status_line}" if TTY else f"{text}\n")
        sys.stdout.flush()
        if transcript:
            transcript.write(text + "\n")


def status(text):
    """The live line: redrawn in place on a terminal, printed as a line every 5 s otherwise."""
    global status_line, status_printed
    if TTY:
        with lock:
            status_line = text[:shutil.get_terminal_size().columns - 1]
            sys.stdout.write(f"\r\033[K{status_line}")
            sys.stdout.flush()
    elif text and time.time() - status_printed >= 5:
        status_printed = time.time()
        say(text)


def clock(t0):
    return f"t+{time.time() - t0:.1f} s"


def dc(*args, check=True):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT / "support", capture_output=True, encoding="utf-8",
                          check=check)


def psql(db, sql):
    user = db.split("-")[0]
    return dc("exec", "-T", db, "psql", "-U", user, "-d", user, "-X", "-At", "-c", sql).stdout.strip()


def rows(db, sql):
    return json.loads(psql(db, f"SELECT COALESCE(json_agg(t), '[]'::json) FROM ({sql}) t"))


def queues():
    """Ready plus unacknowledged messages per queue."""
    out = dc("exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_queues", "name", "messages_ready",
             "messages_unacknowledged", "--formatter", "json").stdout
    return {q["name"]: q["messages_ready"] + q["messages_unacknowledged"] for q in json.loads(out)}


def request(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data, {"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.status


def healthy(port):
    try:
        return request("GET", f"http://localhost:{port}/actuator/health", timeout=2) == 200
    except (OSError, http.client.HTTPException):
        return False


def wait_for(condition, bound, what):
    deadline = time.time() + bound
    while not condition():
        if time.time() > deadline:
            raise SystemExit(f"timed out after {bound} s waiting for {what}")
        time.sleep(0.5)


def reset():
    for _, port, _ in LOYALTY + BARISTA:
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise SystemExit(f"port {port} is taken: a JVM from an aborted run may still be alive")
    dc("up", "-d", "--wait")
    request("POST", f"{TOXIPROXY}/reset")
    for db in ("barista-db", "loyalty-db"):
        psql(db, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")  # Liquibase and Modulith recreate it on start
    for queue in ("loyalty.coffee-sold", "loyalty.coffee-sold.failed"):
        dc("exec", "-T", "rabbitmq", "rabbitmqctl", "delete_queue", queue, check=False)
    shutil.rmtree(LOGS, ignore_errors=True)
    LOGS.mkdir()


def launch(name, port, proxy, props, restarts=0):
    app = name.split("-")[0]
    args = ["java", "-Xmx256m", "-jar", ROOT / app / "target" / f"{app}.jar", f"--server.port={port}",
            f"--spring.amqp.rabbitmq.port={proxy}", f"--{app}.instance-id={name}", f"--chaos.restart-count={restarts}",
            *(f"--{key}={value}" for key, value in props.items())]
    with open(LOGS / f"{name}.log", "ab") as log:
        return subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)


class Instance:
    """One JVM in a restart loop: halted or killed, it is back a second later with its restart count raised."""

    def __init__(self, name, port, proxy, props):
        self.args, self.proc, self.restarts, self.stopping = (name, port, proxy, props), None, 0, False
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    def loop(self):
        while not self.stopping:
            self.proc = launch(*self.args, self.restarts)
            if self.stopping:  # stop() ran before this process existed
                self.proc.kill()
            self.proc.wait()
            if not self.stopping:
                time.sleep(1)
                self.restarts += 1

    def kill(self):
        """The pid it killed, or None when the JVM is down (halted, or between restarts)."""
        proc = self.proc
        if proc and proc.poll() is None:
            proc.kill()
            return proc.pid
        return None

    def stop(self):
        self.stopping = True
        self.kill()
        self.thread.join(20)


def prologue():
    """One sale, one halt between commit and send: barista-db has the sale, loyalty never gets it."""
    say("prologue: after-commit + naive, one sale, barista-1 halts between commit and send")
    reset()
    procs = [launch("loyalty-1", 8091, 21011, {"loyalty.mode": "naive"})]
    try:
        wait_for(lambda: healthy(8091) and "loyalty.coffee-sold" in queues(), 120, "loyalty-1 and its queue")
        procs.append(launch("barista-1", 8081, 21001,
                            {"barista.mode": "after-commit", "chaos.crash.after-commit-before-send": 1.0}))
        wait_for(lambda: healthy(8081), 120, "barista-1")
        try:
            answer = request("POST", "http://localhost:8081/api/sales", {"cardId": "card-1", "cups": 2}, timeout=20)
        except (OSError, http.client.HTTPException) as e:
            answer = f"no response ({type(e).__name__})"
        say(f"POST /api/sales → {answer}")
        log = LOGS / "barista-1.log"
        wait_for(lambda: HALT.search(log.read_text(errors="replace")), 30, "the crash-point line")
        halt = HALT.search(log.read_text(errors="replace"))
        say(halt[0])
        procs[-1].wait()
        # back without the crash point: whatever happens next is what after-commit does on its own
        procs.append(launch("barista-1", 8081, 21001, {"barista.mode": "after-commit"}, restarts=1))
        wait_for(lambda: healthy(8081), 120, "barista-1 restart")
        say("barista-1 restarted without the crash point")
        time.sleep(8)  # long enough for anything that would send it again
        sale = halt[2]
        committed = int(psql("barista-db", f"SELECT COUNT(*) FROM sale WHERE sale_id = '{sale}'"))
        applied = int(psql("loyalty-db", f"SELECT COUNT(*) FROM stamp WHERE sale_id = '{sale}'"))
        queued = queues()["loyalty.coffee-sold"]
        say(f"barista-db  sales  with sale_id {sale}: {committed}")
        say(f"loyalty-db  stamps with sale_id {sale}: {applied} (loyalty up, queue depth {queued})")
        if (committed, applied, queued) != (1, 0, 0):
            say("[FAIL] the prologue did not show a committed sale that was never sent")
            sys.exit(1)
        say("committed, never sent, and nothing in this version sends it again")
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()


def start(scenario, seed, instances):
    """One JVM at a time, so two never create a schema together; loyalty-1 first, so the queue exists before a sale."""
    barista_mode, loyalty_mode = scenario.split("+")
    point, p = CRASH[barista_mode]
    props = {"loyalty": {"loyalty.mode": loyalty_mode, "chaos.seed": seed,
                         f"chaos.crash.{LOYALTY_CRASH[0]}": LOYALTY_CRASH[1]},
             "barista": {"barista.mode": barista_mode, "chaos.seed": seed, f"chaos.crash.{point}": p}}
    for name, port, proxy in LOYALTY + BARISTA:
        instances[name] = Instance(name, port, proxy, props[name.split("-")[0]])
        wait_for(lambda: healthy(port) and "loyalty.coffee-sold" in queues(), 120, name)


def schedule(seed):
    """Seeded: three kill -9, one broker SIGKILL, two lost confirms, two lost settlements, inside the load window."""
    rnd = random.Random(seed)
    events = [("kill -9", rnd.choice([n for n, _, _ in LOYALTY + BARISTA])) for _ in range(3)]
    events.append(("SIGKILL", "rabbitmq"))
    # one toxic of each kind per proxy: a second one of the same name would be refused
    events += [("confirm-lost", n) for n in rnd.sample([n for n, _, _ in BARISTA], 2)]
    events += [("settle-lost", n) for n in rnd.sample([n for n, _, _ in LOYALTY], 2)]
    return sorted((round(rnd.uniform(0.1, 0.8) * (SALES / RATE), 1), *e) for e in events)


def act(at, kind, target, instances, t0, executed):
    """One chaos event at its time, printed when it happens; a failure is recorded, never swallowed."""
    time.sleep(max(0, t0 + at - time.time()))
    entry = {"t": round(time.time() - t0, 1), "action": kind, "target": target}
    executed.append(entry)
    try:
        if kind == "kill -9":
            if instances[target].kill() is None:
                entry["skipped"] = "not running"
            say(f"{clock(t0)} kill -9 {target}" + (" (not running)" if "skipped" in entry else ""))
        elif kind == "SIGKILL":
            dc("kill", "-s", "SIGKILL", "rabbitmq")
            say(f"{clock(t0)} SIGKILL rabbitmq")
            time.sleep(2)
            dc("up", "-d", "--wait", "rabbitmq")
            say(f"{clock(t0)} rabbitmq healthy")
        else:
            # timeout 0 drops the bytes and keeps the connection open: confirms to barista, settlements from loyalty
            stream = "downstream" if kind == "confirm-lost" else "upstream"
            request("POST", f"{TOXIPROXY}/proxies/{target}/toxics", {
                "name": kind, "type": "timeout", "stream": stream, "toxicity": 1.0, "attributes": {"timeout": 0}})
            say(f"{clock(t0)} {kind} on {target} ({TOXIC_SECONDS} s)")
            time.sleep(TOXIC_SECONDS)
            request("DELETE", f"{TOXIPROXY}/proxies/{target}/toxics/{kind}")
    except Exception as e:  # a failed injection must show in the record and the gate, not vanish
        entry["error"] = repr(e)
        say(f"{clock(t0)} {kind} {target} FAILED: {e!r}")


def send_sales():
    """SALES requests at RATE per second, alternating the two baristas, each sent once."""
    def sale(i):
        try:
            request("POST", f"http://localhost:{BARISTA[i % 2][1]}/api/sales",
                    {"cardId": f"card-{i % 50}", "cups": 1 + i % 3}, timeout=20)
        except (OSError, http.client.HTTPException):
            pass  # refused, failed or cut off: the sale counts only if barista-db has it

    began = time.time()
    with ThreadPoolExecutor(THREADS) as pool:
        for i in range(SALES):
            time.sleep(max(0, began + i / RATE - time.time()))
            pool.submit(sale, i)


def tick(t0, offsets, executed):
    for log in sorted(LOGS.glob("*.log")):
        with open(log, "rb") as f:
            f.seek(offsets.get(log, 0))
            data = f.read()
        complete = data[:data.rfind(b"\n") + 1]  # a line still being written waits for the next tick
        offsets[log] = offsets.get(log, 0) + len(complete)
        for halt in HALT.finditer(complete.decode(errors="replace")):
            say(f"{clock(t0)} {halt[0]}")
    committed = psql("barista-db", "SELECT COUNT(*) FROM sale")
    stamped, duplicates = psql("loyalty-db", f"SELECT COUNT(*), {DUPLICATES} FROM stamp").split("|")
    try:
        queued = queues()["loyalty.coffee-sold"]
    except (subprocess.CalledProcessError, ValueError, KeyError):
        queued = "?"  # the broker is down
    status(f"{clock(t0)} committed {committed} | in queue {queued} | stamped {stamped} | duplicates {duplicates}"
           f" | chaos {len(executed)}")


def drain(t0, offsets, executed):
    """Load and chaos over, toxics reset: wait until the queue is empty and every publication complete, twice."""
    say(f"{clock(t0)} drain")
    request("POST", f"{TOXIPROXY}/reset")
    began, quiet = time.time(), 0
    while quiet < 2 and time.time() - began < DRAIN_BOUND:
        time.sleep(3)
        tick(t0, offsets, executed)
        queued = queues()["loyalty.coffee-sold"]
        incomplete = int(psql("barista-db",
                              "SELECT COUNT(*) FROM event_publication WHERE status IS DISTINCT FROM 'COMPLETED'"))
        quiet = quiet + 1 if queued == incomplete == 0 else 0
    return {"seconds": round(time.time() - began, 1), "queued": queued, "incomplete_publications": incomplete,
            "drained": quiet == 2}


def timeline(t0, sales, stamps):
    """Events per second since the load began. A failure counts when it happened: a lost sale when it was sold, a
    ghost at its first stamp, a double-apply at its second."""
    stamped_at = defaultdict(list)  # message id: its stamp times, oldest first
    for s in sorted(stamps, key=lambda s: s["at"]):
        stamped_at[s["message_id"]].append(s["at"])
    sold_at = {r["message_id"]: r["at"] for r in sales}
    events = {"committed": sold_at.values(), "stamped": [s["at"] for s in stamps],
              "lost": [at for m, at in sold_at.items() if m not in stamped_at],
              "ghosts": [at[0] for m, at in stamped_at.items() if m not in sold_at],
              "double_applied": [at[1] for at in stamped_at.values() if len(at) > 1]}
    seconds = {name: Counter(int(at - t0) for at in ats) for name, ats in events.items()}
    assert all(i >= 0 for counts in seconds.values() for i in counts), "an event 1 s or more before t0: clock skew"
    last = max(max(counts, default=0) for counts in seconds.values())
    return {"bucket_seconds": 1, **{name: [counts[i] for i in range(last + 1)] for name, counts in seconds.items()}}


def measure(scenario, seed, executed, drained, t0):
    """Joins barista-db, loyalty-db, the broker and the JVM logs into the six cells and the gates."""
    point = CRASH[scenario.split("+")[0]][0]
    sales = rows("barista-db", "SELECT message_id, cups, EXTRACT(EPOCH FROM sold_at) AS at FROM sale")
    sold = {r["message_id"]: r["cups"] for r in sales}
    stamps = rows("loyalty-db", """SELECT message_id, stamps, body, content_sha256,
        encode(sha256(convert_to(body, 'UTF8')), 'hex') AS db_sha256, EXTRACT(EPOCH FROM applied_at) AS at
        FROM stamp""")
    stamped = Counter(s["message_id"] for s in stamps)
    # the producer's hash travels as content-sha256 and loyalty parks a difference on receipt; the other three
    # (loyalty on receipt, loyalty-db, hashlib) must give one value per message
    hashes = defaultdict(set)
    for s in stamps:
        hashes[s["message_id"]] |= {s["content_sha256"], s["db_sha256"], hashlib.sha256(s["body"].encode()).hexdigest()}
    logs = "\n".join(log.read_text(errors="replace") for log in sorted(LOGS.glob("*.log")))
    cells = {
        "committed": len(sold),
        "lost": len(sold.keys() - stamped.keys()),
        "ghosts": len(stamped.keys() - sold.keys()),
        "double-applied": sum(n > 1 for n in stamped.values()),
        "duplicates delivered": int(psql("loyalty-db", f"SELECT {DUPLICATES} FROM stamp")),
        "hash mismatch": len({m for m, h in hashes.items() if len(h) > 1} | set(HASH_PARKED.findall(logs))),
    }
    halts = Counter(halt[1] for halt in HALT.finditer(logs))
    fired = {p: halts[p] for p in (point, LOYALTY_CRASH[0])}
    suppressed = [int(n) for n in DUPLICATE.findall(logs)]  # the delivery count of each duplicate the inbox skipped
    redelivered = sum(n > 0 for n in suppressed)
    parked = queues()["loyalty.coffee-sold.failed"]
    cups, stamp_total = sum(sold.values()), sum(s["stamps"] for s in stamps)
    errors = sum("error" in e for e in executed)

    gates = {}
    if scenario in FAILURE:
        cell = FAILURE[scenario]
        gates[f"{cell} > 0 ({cells[cell]})"] = cells[cell] > 0
    gates[f"every eligible crash point fired ({', '.join(f'{p} {n}' for p, n in fired.items())})"] = all(fired.values())
    gates[f"every chaos action ran without error ({len(executed) - errors} of {len(executed)}, "
          f"{sum('skipped' in e for e in executed)} kills found the instance down)"] = errors == 0
    gates[f"drained within {DRAIN_BOUND} s ({drained['seconds']} s, queue {drained['queued']}, "
          f"incomplete publications {drained['incomplete_publications']})"] = drained["drained"]
    if scenario == "outbox+inbox":
        gates[f"lost = ghosts = double-applied = hash mismatch = 0, parking queue empty ({parked})"] = \
            not any(cells[c] for c in ("lost", "ghosts", "double-applied", "hash mismatch")) and parked == 0
        gates[f"the inbox suppressed a broker redelivery ({redelivered} of {len(suppressed)} suppressed duplicates "
              "had a delivery count above 0)"] = redelivered > 0
        gates[f"cups sold = stamps in loyalty-db ({cups} = {stamp_total})"] = cups == stamp_total
    return {"scenario": scenario, "seed": seed, "cells": cells, "gates": gates, "crash_points_fired": fired,
            "chaos": executed, "drain": drained, "timeline": timeline(t0, sales, stamps)}


def run(scenario, seed):
    point, p = CRASH[scenario.split("+")[0]]
    events = schedule(seed)
    say()
    say(f"{scenario} · seed {seed} · expect " +
        (f"{FAILURE[scenario]} > 0" if scenario in FAILURE else "lost = ghosts = double-applied = hash mismatch = 0"))
    say(textwrap.fill("chaos  " + " · ".join(f"t+{at} s {kind} {target}" for at, kind, target in events), 118,
                      subsequent_indent="       "))
    say(f"crash  {point} p={p} · {LOYALTY_CRASH[0]} p={LOYALTY_CRASH[1]}")
    reset()
    instances, executed, offsets = {}, [], {}
    try:
        start(scenario, seed, instances)
        t0 = time.time()
        threads = [threading.Thread(target=act, args=(*e, instances, t0, executed), daemon=True) for e in events]
        threads.append(threading.Thread(target=send_sales, daemon=True))
        for thread in threads:
            thread.start()
        while any(thread.is_alive() for thread in threads):
            tick(t0, offsets, executed)
            time.sleep(1)
        drained = drain(t0, offsets, executed)
    finally:
        status("")
        for instance in instances.values():
            instance.stop()
    result = measure(scenario, seed, executed, drained, t0)
    say(" · ".join(f"{column} {value}" for column, value in result["cells"].items()))
    for gate, ok in result["gates"].items():
        say(f"{'[ OK ]' if ok else '[FAIL]'} {gate}")
    name = scenario if seed == DEFAULT_SEED else f"{scenario}.seed-{seed}"
    # a series on one line: a timeline is read across, not down
    text = re.sub(r"\[\s+([\d,\s]+?)\s+\]", lambda m: f"[{' '.join(m[1].split())}]", json.dumps(result, indent=2))
    (RESULTS / f"{name}.json").write_text(text + "\n")
    return result


def write_summary():
    """results/SUMMARY.md and the same block in README.md: the four rows at the default seed, then every seed of
    outbox+inbox."""
    def table(first, results):
        body = [[r[first], *(r["cells"][c] for c in COLUMNS), "ok" if all(r["gates"].values()) else "RED"]
                for r in results]
        lines = [[first, *COLUMNS, "gates"], *body]
        widths = [max(len(str(line[i])) for line in lines) for i in range(len(lines[0]))]
        row = lambda line: "| " + " | ".join(str(v).ljust(w) for v, w in zip(line, widths)) + " |"
        return "\n".join([row(lines[0]), row(["-" * w for w in widths]), *map(row, body)])

    four = [json.loads(path.read_text()) for s in SCENARIOS if (path := RESULTS / f"{s}.json").exists()]
    seeds = sorted((json.loads(p.read_text()) for p in RESULTS.glob("outbox+inbox*.json")), key=lambda r: r["seed"])
    text = table("scenario", four)
    if len(seeds) > 1:
        text += (f"\n\nSeeds of the last row, same load and chaos recipe (n = {len(seeds)}). Seed {DEFAULT_SEED} is the "
                 "last row of the table above, the others come from `mise run scenario -- outbox+inbox --seed N`. The "
                 "seed fixes the chaos schedule and the crash draws, not the timing, so the duplicate counts differ."
                 "\n\n" + table("seed", seeds))
    (RESULTS / "SUMMARY.md").write_text(text + "\n")
    readme = ROOT / "README.md"
    before, rest = readme.read_text().split(START)
    readme.write_text(f"{before}{START}\n\n{text}\n\n{END}{rest.split(END)[1]}")
    return text


def main():
    global transcript
    parser = argparse.ArgumentParser(description="The prologue and the four chaos scenarios, measured and gated.")
    parser.add_argument("run", choices=["demo", *SCENARIOS], help="demo runs the prologue and all four scenarios")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    began = time.time()
    RESULTS.mkdir(exist_ok=True)
    if args.run == "demo":
        transcript = open(RESULTS / "demo.txt", "w", encoding="utf-8")
        prologue()
    red = []
    for scenario in SCENARIOS if args.run == "demo" else [args.run]:
        red += [f"{scenario}: {gate}" for gate, ok in run(scenario, args.seed)["gates"].items() if not ok]
    say()
    say(write_summary())
    took = int(time.time() - began)
    say()
    say(f"runtime {took // 60} min {took % 60} s · results in results/ and README.md · infrastructure stays up, "
        "mise run infra:down stops it")
    if red:
        say("[FAIL] " + "; ".join(red))
    sys.exit(1 if red else 0)


if __name__ == "__main__":
    main()
