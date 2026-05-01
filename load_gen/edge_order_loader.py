#!/usr/bin/env python3
"""
Bulk-load random order documents into Sync Gateway via async PUT.

Usage:
    python3 sg_order_loader.py -r 10 -i 50             # 10/sec, 50 docs
    python3 sg_order_loader.py -r 10 -R 20 -i 200      # random 10-20/sec
    python3 sg_order_loader.py -r 5 -i 100 -e 300      # expire in 300s
    python3 sg_order_loader.py -r 10 -i 50 -l          # loop forever

Requires: pip install aiohttp
"""

import argparse
import asyncio
import json
import random
import string
import aiohttp
import base64
import ssl
from datetime import datetime, timedelta

BASE_URL = "https://localhost:59840"
KEYSPACE = "db.us.prices"
USERNAME = "bob"
PASSWORD = "password"

PRODUCTS = [
    "prod_A",
    "prod_B",
    "prod_C",
    "prod_D",
    "prod_E",
    "prod_F",
    "prod_G",
    "prod_H",
    "prod_I",
    "prod_J",
    "prod_K",
    "prod_L",
    "prod_M",
    "prod_N",
    "prod_O",
    "prod_P",
]
STATUSES = [
    "pending",
    "shipped",
    "delivered",
    "cancelled",
    "processing",
    "returned",
    "refunded",
    "backordered",
    "on_hold",
    "partially_shipped",
]
CHANNELS = ["bob", "kevin", "stuart", "dave", "gru"]


def build_auth_header(user: str, pwd: str) -> str:
    creds = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    return f"Basic {creds}"


def random_order(doc_id: str, exp: int | None) -> dict:
    num_items = random.randint(1, 10)
    items = []
    for _ in range(num_items):
        qty = random.randint(1, 500)
        price = round(random.uniform(1.0, 150.0), 3)
        item = {"product_id": random.choice(PRODUCTS), "qty": qty, "price": price}
        if random.random() < 0.3:
            item["discount"] = random.choice([True, False])
        if random.random() < 0.2:
            item["draft"] = "".join(random.choices(string.ascii_lowercase, k=4))
        items.append(item)

    total = round(sum(i["qty"] * i["price"] for i in items), 2)
    days_ago = random.randint(0, 365)
    order_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    num_channels = random.randint(1, 3)
    channels = random.sample(CHANNELS, num_channels)

    doc = {
        "_id": doc_id,
        "type": "order",
        "status": random.choice(STATUSES),
        "customer_id": f"cust_{random.randint(1, 200)}",
        "order_date": order_date,
        "total": total,
        "items": items,
        "channels": channels,
    }
    if exp is not None:
        doc["_exp"] = exp
    return doc


async def get_rev(session: aiohttp.ClientSession, url: str) -> str | None:
    async with session.get(url) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data.get("_rev")
    return None


async def put_doc(
    session: aiohttp.ClientSession,
    base_url: str,
    keyspace: str,
    doc_id: str,
    body: dict,
) -> int:
    url = f"{base_url}/{keyspace}/{doc_id}"
    async with session.put(url, json=body) as resp:
        status = resp.status

    if status == 409:
        rev = await get_rev(session, url)
        if rev:
            body["_rev"] = rev
            async with session.put(url, json=body) as resp:
                status = resp.status

    return status


class Stats:
    def __init__(self):
        self.sent = 0
        self.errors = 0
        self.conflicts = 0


async def send_one(
    session: aiohttp.ClientSession,
    base_url: str,
    keyspace: str,
    doc_id: str,
    exp: int | None,
    stats: Stats,
    total_label: str,
):
    body = random_order(doc_id, exp)
    try:
        status = await put_doc(session, base_url, keyspace, doc_id, body)
        stats.sent += 1
        if status == 409:
            stats.conflicts += 1
        if status not in (200, 201):
            stats.errors += 1
            print(f"  [{stats.sent}{total_label}] {doc_id} -> HTTP {status}")
        elif stats.sent % 50 == 0:
            print(f"  [{stats.sent}{total_label}] ok")
    except Exception as e:
        stats.errors += 1
        print(f"  [Error] {doc_id}: {e}")


async def run(args):
    auth_header = build_auth_header(args.user, args.password)
    headers = {"Authorization": auth_header, "Content-Type": "application/json"}

    # --- SSL CONFIGURATION FOR LOCALHOST/SELF-SIGNED CERTS ---
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    # ---------------------------------------------------------

    rate_min = args.rate
    rate_max = args.rate_max if args.rate_max > 0 else args.rate
    total = args.items
    stats = Stats()
    rate_label = f"{rate_min}-{rate_max}" if rate_max > rate_min else str(rate_min)
    exp_label = f"  _exp={args.exp}s" if args.exp else ""
    loop_label = "  (loop)" if args.loop else ""
    print(
        f"PUT {total} orders -> {args.url}/{args.keyspace} @ {rate_label}/sec{exp_label}{loop_label}"
    )

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        lap = 0
        while True:
            lap += 1
            if args.loop and lap > 1:
                print(f"\n--- loop {lap} ---")

            tasks: list[asyncio.Task] = []
            for i in range(total):
                delay = random.uniform(1.0 / rate_max, 1.0 / rate_min) * i
                doc_id = f"foo_{i}"
                t = asyncio.create_task(
                    _delayed_send(
                        session,
                        args.url,
                        args.keyspace,
                        doc_id,
                        args.exp,
                        stats,
                        "",
                        delay,
                    )
                )
                tasks.append(t)

            await asyncio.gather(*tasks)

            if not args.loop:
                break

    print(
        f"\nDone. Sent={stats.sent}  Errors={stats.errors}  "
        f"Conflicts(resolved)={stats.conflicts - stats.errors}"
    )


async def _delayed_send(
    session, base_url, keyspace, doc_id, exp, stats, total_label, delay
):
    await asyncio.sleep(delay)
    await send_one(session, base_url, keyspace, doc_id, exp, stats, total_label)


def main():
    p = argparse.ArgumentParser(description="Load order docs into Sync Gateway")
    p.add_argument("-r", "--rate", type=int, required=True, help="Min docs per second")
    p.add_argument(
        "-R", "--rate-max", type=int, default=0, help="Max docs/sec (range with -r)"
    )
    p.add_argument(
        "-i", "--items", type=int, default=100, help="Total docs to write (default 100)"
    )
    p.add_argument(
        "-e", "--exp", type=int, default=None, help="Set _exp (TTL in seconds)"
    )
    p.add_argument(
        "-l",
        "--loop",
        action="store_true",
        help="Loop forever through the same -i docs",
    )
    p.add_argument(
        "--url", default=BASE_URL, help=f"SG public URL (default {BASE_URL})"
    )
    p.add_argument(
        "--keyspace", default=KEYSPACE, help=f"db.scope.collection (default {KEYSPACE})"
    )
    p.add_argument(
        "-u", "--user", default=USERNAME, help=f"Username (default {USERNAME})"
    )
    p.add_argument("-p", "--password", default=PASSWORD, help="Password")
    args = p.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print(f"\nStopped.")


if __name__ == "__main__":
    main()
