"""Exercise parent binding with both the original and a rehashed forged pin."""
import argparse
import copy
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts" / "phase20"))
from campaign import BFS, Campaign, pin, save, sha
from verify import domain, raw


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("work", type=Path)
    args = parser.parse_args()
    work = args.work.resolve()
    work.mkdir()
    driver = Campaign(args.binary.resolve(), work)
    request = driver.request("parent-test", "mixed", BFS, checkpoint_every=1, limits={"rounds": 1})
    first = driver.run("prefix", "run", request, "rounds before fixed point")
    request.update(expected=pin(first), limits={})
    root = Path(request["root"])
    witness_path = root / "w20-0001"
    before = witness_path.read_bytes()
    changed = json.loads(before)
    changed["parent"] = "0" * 64
    after = raw(changed)
    witness_path.write_bytes(after)
    forged = copy.deepcopy(request)
    forged["expected"]["chain"] = domain("ade-cycle20-witness-v1", after)
    cases = [driver.run("altered-parent-original-pin", "resume", request, "witness sequence, round or parent mismatch"),
             driver.run("altered-parent-rehashed-pin", "resume", forged, "witness sequence, round or parent mismatch")]
    assert not (root / "s20-0002").exists()
    witness_path.write_bytes(before)
    resumed = driver.run("resume-restored-witness", "resume", request)
    request["expected"] = pin(resumed)
    terminal = driver.terminal("terminal", request, driver.expected("mixed", request))
    save(work / "proof.json", {"binary_sha256": sha(driver.binary), "script_sha256": sha(Path(__file__)),
                              "original_witness": json.loads(before), "altered_witness": changed,
                              "refusals": [case["name"] for case in cases],
                              "terminal": terminal["name"]})


if __name__ == "__main__":
    main()
