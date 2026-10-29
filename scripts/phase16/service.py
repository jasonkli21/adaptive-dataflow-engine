#!/usr/bin/env python3
"""Bounded wire-observation/fault proxy in front of a real independent MinIO.
Never a storage implementation. Persists state bodies by SHA256, no auth headers.
"""
import argparse, hashlib, http.server, json, pathlib, socket, threading, time
import requests


def serve(backend,port,root):
    root=pathlib.Path(root);bodies=root/'wire-bodies';bodies.mkdir()
    lock=threading.Lock();state={'rule':None,'serial':0};trace=open(root/'wire.jsonl','a',buffering=1)
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version='HTTP/1.0'
        def log_message(self,*_):pass
        def request(self):
            size=int(self.headers.get('Content-Length','0'))
            if size>1048576:self.send_error(413);return
            body=self.rfile.read(size)
            if self.path=='/__control' and self.command=='POST':
                rule=json.loads(body or b'null')
                with lock:state['rule']=rule
                self.send_response(200);self.end_headers();self.wfile.write(b'{}');return
            with lock:
                state['serial']+=1;serial=state['serial'];rule=state['rule']
                if rule and rule.get('remaining',1)>0 and self.command==rule.get('method',self.command) and rule.get('contains','') in self.path:
                    rule=dict(rule);state['rule']['remaining']-=1
                else:rule=None
            action=rule['action'] if rule else None;start=time.monotonic_ns();data=b'';status=599
            def persist(raw):
                h=hashlib.sha256(raw).hexdigest();p=bodies/h
                if not p.exists():p.write_bytes(raw)
                return h
            try:
                if action=='delay':time.sleep(rule.get('delay_ms',2000)/1000)
                if action in ('500','503'):status,headers,data=int(action),{},b'<Error><Code>InternalError</Code></Error>'
                elif action=='redirect':status,headers,data=307,{'Location':'http://127.0.0.1:1/never'},b''
                elif action=='oversize':status,headers,data=500,{},b'x'*1048577
                else:
                    headers={k:v for k,v in self.headers.items() if k.lower() not in ('connection','content-length','transfer-encoding')}
                    session=requests.Session();session.trust_env=False
                    r=session.request(self.command,backend+self.path,headers=headers,data=body,timeout=10,allow_redirects=False)
                    status,headers,data=r.status_code,dict(r.headers),r.content
                if action=='corrupt' and data:data=bytes([data[0]^1])+data[1:]
                if action=='truncate' and data:data=data[:-1]
                if action=='drop-after':
                    self.connection.shutdown(socket.SHUT_RDWR);self.connection.close()
                else:
                    self.send_response(status)
                    for k,v in headers.items():
                        if k.lower() not in ('content-length','connection','transfer-encoding','server','date'):self.send_header(k,v)
                    length=headers.get('Content-Length',str(len(data))) if self.command=='HEAD' else str(len(data))
                    self.send_header('Content-Length',length);self.end_headers()
                    if self.command!='HEAD':self.wfile.write(data)
            except (requests.RequestException,OSError):status=599
            finally:
                with lock:
                    trace.write(json.dumps(dict(serial=serial,method=self.command,path=self.path,status=status,request_bytes=len(body),response_bytes=len(data),request_sha256=persist(body),response_sha256=persist(data),advertised_length=headers.get('Content-Length') if 'headers' in locals() else None,fault=action,conditional_create=self.headers.get('If-None-Match')=='*',signed=self.headers.get('Authorization','').startswith('AWS4-HMAC-SHA256'),elapsed_ns=time.monotonic_ns()-start),sort_keys=True)+'\n')
        do_GET=do_HEAD=do_PUT=do_POST=request
    server=http.server.ThreadingHTTPServer(('127.0.0.1',port),Handler);server.daemon_threads=True
    server.serve_forever()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--backend',required=True);p.add_argument('--port',type=int,required=True);p.add_argument('--root',required=True);a=p.parse_args();serve(a.backend,a.port,a.root)
