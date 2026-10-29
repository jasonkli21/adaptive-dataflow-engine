#!/usr/bin/env python3
"""Phase16 adapter for the frozen Phase15 70-case evidence schema.
The original verifier recognizes only its two historical base SHAs. This adapter
pins its exact source, adds the actual a89f1ae preparation base to the modern
70-case branch, and preserves every oracle, negative, source and integrity check.
Evidence base/source identities are never rewritten. Historical archives must
still use their unchanged original verifier.
"""
import hashlib, pathlib, sys
ROOT=pathlib.Path(__file__).resolve().parents[1]/'phase15'
raw=(ROOT/'verify_evidence.py').read_bytes()
if hashlib.sha256(raw).hexdigest()!='a6f7a73606d0c2f4012da9e8689b972e02355656b2a75aee1a097e8f2fde0c8c':raise ValueError('historical verifier source changed')
source=raw.decode().replace('7f838db7bfb0275f6f8e7c65aab8da90e4d6bd79','a89f1aeb2b7c36a23d77d702e705d6ffe4bdbfea')
sys.path.insert(0,str(ROOT))
namespace={'__name__':'phase15_phase16_adapter','__file__':str(ROOT/'verify_evidence.py')}
exec(compile(source,str(ROOT/'verify_evidence.py'),'exec'),namespace)
verify=namespace['verify']
if __name__=='__main__':namespace['main']()
