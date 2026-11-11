# Phase 21 connector and catalog evidence

`acceptance.tar.gz` contains the frozen case inventory, baseline and source
digests, executable digests, service versions, fixture inputs, per-process
requests and outputs, and selected local sink bytes. Retain the archive pin
from `manifest.json` in a separate trusted record before using the archive
for verification.

```sh
python3 scripts/phase21/verify.py docs/evidence/phase21/acceptance.tar.gz \
  --pin SHA256_FROM_MANIFEST --repo .
```

The verifier independently decodes Parquet, Iceberg manifests and selected
sink IPC. It reconstructs Delta active-file membership and checks Kafka
receipt/progress semantics against the Java producer ledger. It checks both
whole-scan digests in each timing pair. Fully rehashed mutations are preserved
with their verifier results. Use the Python environment in the operator guide.

`toolchain.tar.gz` contains complete final workspace check logs and exit
codes. The `audit` and `legacy` directories contain the baseline/current
RustSec comparison and the four detached predecessor verifier results.

The `attempts` directory preserves the first five candidate
inventories with raw commands, outputs and fixtures. They retain their own
criteria and executable hashes. The first campaign exposed Delta writer
compatibility. The second exposed inadmissible timing SQL. The third exposed
a verifier assumption about stage lifetime at the sink kill cuts. The fourth
proved the hardened transports and independent fault cases before the final
inventory added extra format and path refusals. The fifth passed the semantic verifier
before source-inventory path validation was tightened. Historical evidence from
Phases 17–20 has separate archives and source identities.
