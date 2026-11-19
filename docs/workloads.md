# Workloads: WordCount and Inverted Index in Go

Code: `cmd/wordcount`, `cmd/invertedindex`, `cmd/verify`, `internal/`; the
PySpark baseline is in `baseline/` (docs/benchmarks.md).

Both programs use the Apache Spark Connect Go client
(`github.com/apache/spark-connect-go`, pinned in `go.mod`). The Go program only
builds a query plan and sends it to the Spark Connect server over gRPC; the
work runs on the Spark executors, so the same binary works against a local
server or the CloudLab cluster.

## Semantics (shared with the PySpark baseline)

The PySpark baseline must implement exactly these rules so that the results,
and therefore the timings, are comparable.

**Input record.** One line per record, `<doc_id>\t<text>`, as produced by
`scripts/data`. Only the first two tab-separated fields are used
(`split_part(value, '\t', 1)` and `split_part(value, '\t', 2)`); a line without a
tab is a document with no text. A document may span several part files.

**Terms.** Lower-case the text, split it on every run of characters outside
`[a-z0-9]`, drop empty strings. Apostrophes and hyphens are separators
(`it's` gives `it`, `s`; `e-mail` gives `e`, `mail`), digits are kept
(`2026` is a term). The preprocessing folds all text to ASCII, so this is
the same in Java, Go and Python. Defined once in `internal/textproc`.

**WordCount.** `(term, count)`: the number of occurrences of each term.

**Inverted Index.** `(term, docs, doc_freq)`: the sorted, distinct ids of the
documents containing the term, and their number.

Spark plan built by `internal/jobs`; the PySpark baseline
(`baseline/sparkjobs.py`) is these five lines:

```python
lines = spark.read.format("text").load(input)
records = lines.select(F.split_part("value", F.lit("\t"), F.lit(1)).alias("doc_id"),
                       F.split_part("value", F.lit("\t"), F.lit(2)).alias("text"))
terms = (records.select("doc_id", F.explode(F.split(F.lower("text"), "[^a-z0-9]+")).alias("term"))
                .filter(F.col("term") != ""))
wordcount = terms.groupBy("term").agg(F.count("term").alias("count"))
index = (terms.groupBy("term").agg(F.array_sort(F.collect_set("doc_id")).alias("docs"))
              .withColumn("doc_freq", F.size("docs")))
```

Note: the Go client's generated `functions.Split` sends its `limit` as a
BIGINT, which Spark 4's ANSI mode does not accept for `split`; `internal/jobs`
therefore calls the two-argument `split` directly (and `array_sort`, which has
no generated wrapper).

## Running

```sh
go build -o bin/ ./cmd/...
export SPARK_REMOTE=sc://node0:15002            # Spark Connect server (default sc://localhost:15002)
IN=hdfs://node0:9000/datasets/classics-60

bin/wordcount     -input $IN                     # prints the 20 most frequent terms
bin/invertedindex -input $IN -show 10            # the 10 terms found in most documents

# Benchmark form: run the whole job, store the result, append the timing to a JSON-lines file.
bin/wordcount -input $IN -output hdfs://node0:9000/results/wc -metrics runs.jsonl -label "strong workers=4 run=1"
bin/invertedindex -input $IN -format noop -metrics runs.jsonl   # run the job without storing the result
```

Flags common to both programs:

| flag | meaning |
| --- | --- |
| `-input` | corpus file or directory, local (`file:///...`) or `hdfs://...` |
| `-output`, `-format` | store the result (`parquet` default, or `json`), overwriting `-output` |
| `-format noop` | execute the full job but discard the result (Spark's `noop` sink) |
| `-show n` | print the first `n` rows (by count / doc_freq, then term) |
| `-dump` | print the whole result as sorted TSV on stdout (small inputs only) |
| `-stats` | also print the number of terms and the total count / postings |
| `-metrics f`, `-label s` | append `{"job", "client", "label", "input", "action", "start", "elapsed_ms", "stats", ...}` to `f` |

`elapsed_ms` is the wall-clock time of the action seen by the client (write,
collect or show), i.e. the end-to-end job time including Spark scheduling. It
excludes connecting and reading the result back for `-show`/`-stats`.
`client` is `go`. The PySpark baseline takes the same flags and writes the
same JSON lines with `client` `pyspark`:
`/opt/pyspark/bin/python baseline/wordcount.py -input $IN ...`.

## Correctness tests

1. **Unit tests** (no Spark): `go test ./...`. `internal/textproc` checks the
   tokenizer against the literal regular-expression definition;
   `internal/reference` checks the single-machine implementation against the
   hand-computed expected outputs in `testdata/small/expected`.
2. **Small corpus on Spark**: `testdata/small/corpus` exercises mixed case,
   punctuation, apostrophes, digits, hyphens, a non-ASCII word, an empty text,
   a line without a tab, a third tab-separated field, documents split across
   both part files, and a `_MANIFEST.json` that must be ignored. Its 42 tokens,
   29 terms and 33 postings were counted by hand.

   ```sh
   SPARK_REMOTE=sc://localhost:15002 go test ./internal/jobs        # server on this machine
   SPARK_REMOTE=sc://node0:15002 SPARK_TEST_INPUT=hdfs://node0:9000/datasets/corpus go test ./internal/jobs
   ```
3. **Real datasets**: `cmd/verify` runs the reference implementation on a local
   copy of the corpus and compares it with Spark's result. Up to 100,000 terms
   it compares every row; above that it compares the totals (terms and total
   count / postings) plus the 10 most frequent terms, 50 seeded random terms and
   terms that must be absent. With `-result` it checks a stored benchmark output
   instead of re-running the job.
4. **Weak-scaling inputs**: these are *k* complete copies of `sample-2000`
   (`scale_dataset.py --copies k`), too large to keep a local copy of. With
   `-copies k`, `verify` computes the reference of `sample-2000` and expects
   every count and document frequency multiplied by *k*, and each document
   list to contain every copy of each document (`pg84`, `pg84.r1`, ...).

   ```sh
   bin/verify -job wordcount -input hdfs://node0:9000/datasets/classics-60 -local data/processed/classics-60
   bin/verify -job invertedindex -input $IN -local data/processed/sample-2000 -result hdfs://node0:9000/results/ii
   bin/verify -job invertedindex -input hdfs://node0:9000/datasets/weak-3 -local /mydata/datasets/processed/sample-2000 -copies 15
   ```

   On the cluster, `scripts/data/cluster_datasets.sh` and
   `scripts/data/weak_datasets.sh` run these checks for every dataset (see
   docs/datasets-and-hdfs.md).

## Verified so far (week 10)

On the CloudLab cluster (`node0` and 3 workers, `m510` nodes with 8 cores /
16 hardware threads and 62 GB RAM, deployed as in docs/deployment.md):

* `classics-60`: both jobs equal to the reference on all 85,015 terms
  (`verify -mode full`);
* `sample-2000` (1,999 books, 777.9 MiB, 129,670,403 tokens, 541,397 terms,
  11,980,430 postings): both jobs equal to the reference on all 541,397 terms
  (`verify -mode full`);
* `weak-1` to `weak-3` (5, 10 and 15 copies of `sample-2000`): totals exactly
  5, 10 and 15 times the reference and the spot checks pass (`verify -copies`);
  on 3 copies of `classics-60`, `-copies 3` passes in full and `-copies 2`
  fails, as it should.

First timings, one run of each job with `-format noop` on 3 workers after a
warm-up job, used to choose the weak-scaling size (not benchmark results):

| input | WordCount | Inverted Index |
| --- | --- | --- |
| `weak-1` (3.9 GiB) | 28 s | 106 s |
| `weak-3` (11.9 GiB) | 88 s | 269 s |

Earlier, on one machine (Spark 4.0.5 Spark Connect server in local mode and a
single-node HDFS 3.4.3 set up with `scripts/hdfs`, both under Ubuntu 22.04;
Go client on Windows):

* the integration tests pass with the small corpus read from the local disk
  and from HDFS;
* `classics-60` (55.8 MiB, 9,706,910 tokens, 85,015 terms, 527,802 postings)
  loaded into HDFS, both jobs written back to HDFS as Parquet, and every row
  of both stored results equal to the reference (`verify -mode full`);
* sampled checks on a 3.3 MiB replicated dataset pass;
* a deliberately wrong reference corpus makes `verify` fail with the
  differing totals and terms.
