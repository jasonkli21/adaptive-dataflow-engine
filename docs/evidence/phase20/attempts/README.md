# Preserved Phase 20 preliminary runs

| Run | Disposition |
| --- | --- |
| check-01 | Fixture lock comparison stopped before execution because histogram keys differed between Python and JSON. |
| check-02 | Strict traversal request refused via a missing-path I/O error. Path admission was repaired to validate all components before walking the path. |
| check-03 | Nested topology refused. The driver incorrectly expected a different Serde diagnostic and stopped before the remaining cases. |
| check-04 | 210 functional process observations and a detached round/state verification passed. This rehearsal predates the final preselection source attestation and release timing campaign. |
| enospc-01 | Stopped before image creation at the same fixture lock comparison as check-01. |
| measured-01 | All timing guards and offline checks passed. The campaign was rerun with identical criteria after fixing the reproduction driver to retain the historical baseline when run from the final commit. |
| measured-02 | The release runner reproduced the frozen cases and passed all timing guards after the baseline fix. The next candidate additionally pins each historical Phase 19 archive to its own verifier in the toolchain script. |
| toolchain-01 | Workspace, smoke and Python tests passed. The original Phase 19 archive refused in the newer remediation verifier due to a fixture layout change. The final toolchain replays both archives with their respective historical verifiers. |
| measured-03 | All timing guards and offline checks passed before including the original Phase 19 verifier's digest dependency in the release runner. |
| toolchain-02 | The original Phase 19 verifier was extracted from the correct commit. Its first replay attempt was missing the historical digest module. The final runner extracts both. |
| enospc-02 | Real OS ENOSPC reached with zero free bytes in the private image. This executable predates the final preselection source attestation. |

Each attempt retains the raw artifacts produced before it stopped. The
frozen criteria SHA-256 remains unchanged.
