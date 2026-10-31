"""Portable unkeyed BLAKE3-256 for bounded offline evidence, from the public spec.
Independent Python implementation; not used by the engine or acceptance decisions.
Spec: https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf
"""
import struct
IV=(0x6A09E667,0xBB67AE85,0x3C6EF372,0xA54FF53A,0x510E527F,0x9B05688C,0x1F83D9AB,0x5BE0CD19)
PERM=(2,6,3,10,7,0,4,13,1,11,12,5,9,14,15,8)
MASK=0xffffffff

def compress(cv,words,counter,length,flags):
    v=list(cv)+list(IV[:4])+[counter&MASK,counter>>32,length,flags]
    def g(a,b,c,d,x,y):
        v[a]=(v[a]+v[b]+x)&MASK;v[d]^=v[a];v[d]=((v[d]>>16)|(v[d]<<16))&MASK
        v[c]=(v[c]+v[d])&MASK;v[b]^=v[c];v[b]=((v[b]>>12)|(v[b]<<20))&MASK
        v[a]=(v[a]+v[b]+y)&MASK;v[d]^=v[a];v[d]=((v[d]>>8)|(v[d]<<24))&MASK
        v[c]=(v[c]+v[d])&MASK;v[b]^=v[c];v[b]=((v[b]>>7)|(v[b]<<25))&MASK
    m=list(words)
    for _ in range(7):
        for a,b,c,d,j in [(0,4,8,12,0),(1,5,9,13,2),(2,6,10,14,4),(3,7,11,15,6),(0,5,10,15,8),(1,6,11,12,10),(2,7,8,13,12),(3,4,9,14,14)]:g(a,b,c,d,m[j],m[j+1])
        m=[m[i] for i in PERM]
    return tuple(v[i]^v[i+8] for i in range(8))+tuple(v[i+8]^cv[i] for i in range(8))

def cv(out):return compress(*out)[:8]
def chunk(data,index):
    h=IV;blocks=[data[i:i+64] for i in range(0,len(data),64)] or [b'']
    for i,b in enumerate(blocks):
        out=(h,struct.unpack('<16I',b.ljust(64,b'\0')),index,len(b),(1 if i==0 else 0)|(2 if i==len(blocks)-1 else 0))
        h=cv(out)
    return out

def raw(data):
    chunks=[chunk(data[i:i+1024],i//1024) for i in range(0,len(data),1024)] or [chunk(b'',0)]
    def tree(parts):
        if len(parts)==1:return parts[0]
        split=1<<((len(parts)-1).bit_length()-1)
        return (IV,cv(tree(parts[:split]))+cv(tree(parts[split:])),0,64,4)
    h,words,_,length,flags=tree(chunks)
    return struct.pack('<16I',*compress(h,words,0,length,flags|8))[:32]
def digest(data):return raw(data).hex()
