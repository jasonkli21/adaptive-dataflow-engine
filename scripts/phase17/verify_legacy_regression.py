#!/usr/bin/env python3
"""Exact frozen Phase15 verifier, with only Phase17's base admitted to its70-case schema.
No historical source/evidence is rewritten and no semantic assertion is changed.
"""
import hashlib,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parents[1]/'phase15'
raw=(ROOT/'verify_evidence.py').read_bytes()
if hashlib.sha256(raw).hexdigest()!='a6f7a73606d0c2f4012da9e8689b972e02355656b2a75aee1a097e8f2fde0c8c':raise ValueError('historical verifier changed')
source=raw.decode().replace('7f838db7bfb0275f6f8e7c65aab8da90e4d6bd79','46a976d41d8642d54fe0829bd7944c9a1ddd39d1')
sys.path.insert(0,str(ROOT));namespace={'__name__':'phase15_phase17_adapter','__file__':str(ROOT/'verify_evidence.py')}
exec(compile(source,str(ROOT/'verify_evidence.py'),'exec'),namespace)
verify=namespace['verify']
if __name__=='__main__':namespace['main']()
