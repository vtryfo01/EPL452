#!/usr/bin/env python3
"""Download the plain-text (UTF-8) version of the books in a list.

  fetch_gutenberg.py data/lists/classics-60.txt --out data/raw/gutenberg

Books already present are skipped, so the script can be re-run after an
interruption. Requests are spaced by --delay seconds: Project Gutenberg
blocks aggressive crawlers. For thousands of books prefer a mirror (see
https://www.gutenberg.org/help/mirroring.html), point --url-template at it
and download a few books at a time with --jobs. In the template, {id} is the
book number and {dirs} the directory of the mirrors that keep the original
layout (book 1090 -> 1/0/9). Books the server does not have are listed in
<out>/_MISSING.txt. A book that still fails after 3 attempts is reported and
skipped, and the exit status is then 1.
"""

import argparse
import http.client
import os
import sys
import threading
import time
import urllib.error
import urllib.request

from dslib import read_id_list

DEFAULT_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
USER_AGENT = "cs452-spark-connect-go-datasets/1.0"


def book_url(template, book):
    digits = str(book)
    return template.format(id=book, dirs="/".join(digits[:-1]) or "0")


def fetch(url, retries=3):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            err = e
        except (OSError, http.client.HTTPException) as e:  # network errors, truncated responses
            err = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{url}: {err}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("list", help="book list written by select_gutenberg.py")
    p.add_argument("--out", default="data/raw/gutenberg", help="download directory")
    p.add_argument("--delay", type=float, default=2.0, help="seconds between the requests of a job (default 2)")
    p.add_argument("--jobs", type=int, default=1, help="books downloaded at the same time (default 1)")
    p.add_argument("--url-template", default=DEFAULT_URL, help="download URL with {id} (default: %(default)s)")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    ids = read_id_list(args.list)
    path = {book: os.path.join(args.out, f"pg{book}.txt") for book in ids}
    todo = [(n, book) for n, book in enumerate(ids, 1)
            if not (os.path.exists(path[book]) and os.path.getsize(path[book]) > 0)]
    lock = threading.Lock()
    done, missing, failed = [], [], []

    def report(n, book, msg, into):
        with lock:
            into.append(book)
            print(f"[{n}/{len(ids)}] pg{book}: {msg}", file=sys.stderr)

    def job(books):
        for k, (n, book) in enumerate(books):
            if k:
                time.sleep(args.delay)
            try:
                data = fetch(book_url(args.url_template, book))
            except RuntimeError as e:
                report(n, book, f"failed: {e}", failed)
                continue
            if data is None:
                report(n, book, "not found", missing)
                continue
            with open(path[book] + ".tmp", "wb") as f:
                f.write(data)
            os.replace(path[book] + ".tmp", path[book])
            report(n, book, f"{len(data)} bytes", done)

    jobs = max(1, min(args.jobs, len(todo)))
    threads = [threading.Thread(target=job, args=(todo[j::jobs],)) for j in range(jobs)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if missing:
        with open(os.path.join(args.out, "_MISSING.txt"), "a", encoding="utf-8") as f:
            f.writelines(f"{b}\n" for b in missing)
    print(f"downloaded {len(done)} books, {len(missing)} not found, {len(failed)} failed, "
          f"{len(ids) - len(todo)} already present", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
