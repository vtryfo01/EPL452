#!/usr/bin/env python3
"""Download the plain-text (UTF-8) version of the books in a list.

  fetch_gutenberg.py data/lists/classics-60.txt --out data/raw/gutenberg

Books already present are skipped, so the script can be re-run after an
interruption. Requests are spaced by --delay seconds: Project Gutenberg
blocks aggressive crawlers. For thousands of books prefer a mirror (see
https://www.gutenberg.org/help/mirroring.html) and point --url-template at it.
Books that cannot be downloaded are listed in <out>/_MISSING.txt.
"""

import argparse
import http.client
import os
import sys
import time
import urllib.error
import urllib.request

from dslib import read_id_list

DEFAULT_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
USER_AGENT = "cs452-spark-connect-go-datasets/1.0"


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
    p.add_argument("--delay", type=float, default=2.0, help="seconds between requests (default 2)")
    p.add_argument("--url-template", default=DEFAULT_URL, help="download URL with {id} (default: %(default)s)")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    ids = read_id_list(args.list)
    missing, fetched = [], 0
    for n, book in enumerate(ids, 1):
        path = os.path.join(args.out, f"pg{book}.txt")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            continue
        if fetched:
            time.sleep(args.delay)
        data = fetch(args.url_template.format(id=book))
        fetched += 1
        if data is None:
            print(f"[{n}/{len(ids)}] pg{book}: not found", file=sys.stderr)
            missing.append(book)
            continue
        with open(path + ".tmp", "wb") as f:
            f.write(data)
        os.replace(path + ".tmp", path)
        print(f"[{n}/{len(ids)}] pg{book}: {len(data)} bytes", file=sys.stderr)

    if missing:
        with open(os.path.join(args.out, "_MISSING.txt"), "a", encoding="utf-8") as f:
            f.writelines(f"{b}\n" for b in missing)
    print(f"downloaded {fetched - len(missing)} books, {len(missing)} missing, "
          f"{len(ids) - fetched} already present", file=sys.stderr)


if __name__ == "__main__":
    main()
