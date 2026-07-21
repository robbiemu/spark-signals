#!/usr/bin/env python3
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=14318)
    parser.add_argument("--output", required=True)
    parser.add_argument("--expected-authorization")
    args = parser.parse_args()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            if (
                args.expected_authorization
                and self.headers.get("Authorization") != args.expected_authorization
            ):
                self.send_response(401)
                self.end_headers()
                return
            with open(args.output, "a", encoding="utf-8") as output:
                output.write(self.path + "\n")
            self.send_response(200)
            self.end_headers()

        def log_message(self, _format, *_args):
            return

    class Server(ThreadingHTTPServer):
        allow_reuse_address = True

    Server(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
