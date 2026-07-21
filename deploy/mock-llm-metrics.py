#!/usr/bin/env python3
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=13000)
    parser.add_argument("--authorization", default="Bearer fixture-token")
    args = parser.parse_args()

    class Handler(BaseHTTPRequestHandler):
        scrapes = 0

        def do_GET(self):
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            if self.headers.get("Authorization") != args.authorization:
                self.send_response(401)
                self.end_headers()
                return
            Handler.scrapes += 1
            prompt = Handler.scrapes * 100
            generated = Handler.scrapes * 20
            body = (
                "sglang:num_running_reqs 2\n"
                "sglang:num_queue_reqs 1\n"
                f"sglang:prompt_tokens_total {prompt}\n"
                f"sglang:generation_tokens_total {generated}\n"
                "sglang:gen_throughput 12.5\n"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *_args):
            return

    class Server(ThreadingHTTPServer):
        allow_reuse_address = True

    Server(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
