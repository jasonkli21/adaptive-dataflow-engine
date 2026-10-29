#!/usr/bin/env python3
"""Private loopback fault proxy for an independently running S3-compatible fixture.

Requires requests; run with the same pinned test venv as Moto. This is test-only,
never an ADE listener or a production S3/authentication/durability implementation.
Trace omits authorization, cookies, query secrets and request/response bodies.
"""
import argparse
import hashlib
import http.server
import json
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', required=True)
    parser.add_argument('--port', type=int, default=18914)
    parser.add_argument('--trace', required=True)
    args = parser.parse_args()
    backend = urllib.parse.urlsplit(args.backend)
    if backend.scheme != 'http' or backend.hostname != '127.0.0.1':
        raise ValueError('fixture backend must be explicit loopback HTTP')
    lock = threading.Lock()
    state = {'rule': None, 'serial': 0}
    trace = open(args.trace, 'a', encoding='utf-8', buffering=1)

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.0'
        def log_message(self, *_):
            pass
        def control(self):
            size = int(self.headers.get('Content-Length', '0'))
            if size > 4096:
                self.send_error(413)
                return
            rule = json.loads(self.rfile.read(size) or b'null')
            if rule is not None:
                if rule.get('action') not in ['500', '503', 'delay', 'truncate', 'corrupt', 'redirect', 'oversize-error']:
                    raise ValueError('unsupported bounded fixture fault')
                if not 1 <= rule.get('remaining', 1) <= 64 or not 0 <= rule.get('delay_ms', 0) <= 6000:
                    raise ValueError('fault cap')
            with lock:
                state['rule'] = rule
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{}')
        def request(self):
            if self.path == '/__control' and self.command == 'POST':
                self.control()
                return
            size = int(self.headers.get('Content-Length', '0'))
            if size > 1024 * 1024:
                self.send_error(413)
                return
            body = self.rfile.read(size)
            with lock:
                state['serial'] += 1
                serial = state['serial']
                rule = state['rule']
                if rule and rule.get('remaining', 1) > 0 and self.command == rule.get('method', self.command) and rule.get('contains', '') in self.path:
                    rule = dict(rule)
                    state['rule']['remaining'] -= 1
                else:
                    rule = None
            keys = []
            if self.command == 'POST' and '?delete' in self.path:
                try:
                    keys = [node.text for node in ET.fromstring(body).iter() if node.tag.rsplit('}', 1)[-1] == 'Key']
                except ET.ParseError:
                    pass
            start = time.monotonic_ns()
            action = rule['action'] if rule else None
            try:
                if action == 'delay':
                    time.sleep(rule.get('delay_ms', 100) / 1000)
                if action == 'redirect':
                    status, headers, data = 307, {'Location': f'http://127.0.0.1:{args.port}/never-follow'}, b''
                elif action == 'oversize-error':
                    status, headers, data = 500, {}, b'x' * (1024 * 1024 + 1024)
                elif action in ['500', '503']:
                    status, headers, data = int(action), {}, b'<Error><Code>InternalError</Code></Error>'
                else:
                    headers = {k: v for k, v in self.headers.items() if k.lower() not in ['connection', 'content-length', 'transfer-encoding']}
                    response = requests.request(self.command, args.backend + self.path, headers=headers, data=body, timeout=10, allow_redirects=False)
                    status, headers, data = response.status_code, dict(response.headers), response.content
                if action == 'truncate' and data:
                    data = data[:-1]
                if action == 'corrupt' and data:
                    data = bytes([data[0] ^ 1]) + data[1:]
                self.send_response(status)
                for key, value in headers.items():
                    if key.lower() not in ['content-length', 'connection', 'transfer-encoding', 'server', 'date']:
                        self.send_header(key, value)
                # HEAD advertises the upstream object's actual length.
                length = headers.get('Content-Length', str(len(data))) if self.command == 'HEAD' else str(len(data))
                self.send_header('Content-Length', length)
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(data)
            except (requests.RequestException, BrokenPipeError, ConnectionResetError):
                status = 599
                try:
                    self.send_error(503)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                data = b''
            finally:
                row = {'serial': serial, 'method': self.command, 'path': self.path,
                       'status': status, 'keys': keys, 'sent_body_bytes': len(body), 'received_body_bytes': len(data),
                       'body_sha256': hashlib.sha256(body).hexdigest(), 'fault': action,
                       'elapsed_ns': time.monotonic_ns() - start}
                with lock:
                    trace.write(json.dumps(row, sort_keys=True) + '\n')
        do_GET = do_HEAD = do_PUT = do_DELETE = do_POST = request

    server = http.server.ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    server.daemon_threads = True
    print(f'PHASE13_PROXY_READY 127.0.0.1:{args.port}', flush=True)
    server.serve_forever()

if __name__ == '__main__':
    main()
