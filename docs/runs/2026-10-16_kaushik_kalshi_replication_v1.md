# Fresh Kalshi replication: 16 October 2026

Status: implementation and source admission in progress. No fresh estimates or
report are accepted. The user explicitly selected a fresh raw-data replication,
not a presentation of saved sports estimates.

Scientific contract: [Kalshi v1](../analysis_specs/kaushik_kalshi_replication_v1.md).
Root owns instance lifecycle; workers have no lifecycle authority. The prior
inventory stopped the instance and verified it stopped. This task restarted it
for the approved fresh replication. Final shutdown remains required.

## Source admission to date

- Original trade manifest: 228,872 bytes; SHA-256
  `37aae4a94b1125cea81eb2222caba0daaa089eb9c241a0e6dd90bb0144fdf0ee`.
  Inspected header `file,bytes,sha256`; 1,984 entries including the old quote
  release, which is not an input to this study.
- Market metadata manifest: 11,046 bytes; SHA-256
  `0ad2a1c6167f5171a5323e3bee021a27ae58516ee68ca23aab64d10ca5683ecf`.
  Inspected header `path,bytes,rows,sha256`; 80 shards. Dropbox basenames must
  map bijectively to its S3-relative paths.
- Native event reference: 43,666,531 bytes; SHA-256
  `f535371e2b2c50946125e5ef0004aa418c1ad0fe1dea186c90768717a0480c47`.
  Footer: 822,590 rows; category and series key are on these event rows, not
  on the inspected market-metadata footer.
- First daily trade file: 1,980 bytes, one row, valid canonical UUID; SHA-256
  `1236148e7d08d94b1dfd066353b02b649c0d2d42b2a010ab326a2684b9574977`.
  Actual schema includes undocumented `src:string`. It is explicitly admitted
  as provenance-only; `source/src` disagreement is audited, not filtered.
- Inspected `bulk_v2` metadata has dollar notional 1, while inspected `forward`
  metadata has notional 0 and valid ISO-8601 settlement strings. Missing/zero
  notional is not a zero payout; the explicit binary venue payout rule is
  separate from this unusable raw numeric field. Full-source admission remains
  required before interpreting these samples as representative.

The immutable exploratory metadata directories are
`/mnt/data/runs/2026-10-16_kalshi_source_admission_v1` and `v2`. The first failed
before transfer because installed rclone rejects `copyto --max-size` on a single
file. It is preserved. The staging helper now validates exact remote metadata,
uses the compatible hard transfer ceiling, and checks bytes/SHA-256 before
processing; no exception workaround or original-data deletion was used.

Direct S3 listing and a documented-manifest HEAD were denied for the existing
IAM profile during the preceding inventory. No IAM or credential changes were
made. Existing Dropbox reads work, so the planned serial source staging does
not require new S3 permissions or a larger disk.

## Validation and remaining gates

Pure normalization, serial-staging and grouped-estimator fixtures have passed,
including direct print-level numerical oracles. Full combined runtime tests,
committed-source hashes and actual stage bindings will be recorded on execution.
Fixture success is not a billion-row capacity or source-integrity certificate.

Independent integration review caught and repaired the initial mean-versus-sum
cell interface mismatch before production. A direct 1,728-print builder-through-
estimator fixture then matched independent raw-print coefficients and joint CR0
for all 12 regression specifications. Runtime review also reproduced DuckDB's
temporary-flag change after a table COMMENT; owned daily-cache cleanup uses the
authoritative temporary database identity plus its ownership marker, not that
unstable flag. Strict timestamp-format/range parity and immutable JSON
publication have separate regression fixtures. These are implementation checks,
not findings about the archive's empirical results.

The first committed full metadata stage
`/mnt/data/runs/2026-10-17_kaushik_kalshi_metadata_v1` stopped before publishing
any completed metadata receipts. Its native event reference had 822,590 unique,
non-null keys and one valid legacy key, `GDP-232022 Q4`, containing an internal
ASCII space. Our identifier check was too restrictive, not evidence of a
duplicate or corrupt event. The immutable failure is preserved. A narrow
validator repair keeps this exact identity, rejects padding/other whitespace
and controls, leaves UUID validation unchanged, and adds Python/SQL regression
fixtures before a new committed stage is attempted. No source record is dropped
or rewritten because of that space.

Before estimates: admit all source hashes/schemas, complete native ID uniqueness,
all metadata keys and joins, binary outcome/combination/category rules, clock
coverage, lossless cell moments and every disjoint exclusion. Preserve the 20 GB
free-space floor and measured RAM/spill/output ceilings. Sports identity and
actual play-clock binding is a separate admission gate, not inferred from native
settlement or milestone end fields.
