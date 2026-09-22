#!/usr/bin/env bash
# ClearLeaff Fleet Downsizing Studio Local Preview Server
PORT="${PORT:-8090}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HTML_FILE="/home/cleaff/.gemini/antigravity/brain/908a5403-3ee0-45a2-bb5c-84b0d09e4e6f/downsizing_studio.html"

echo "================================================================"
echo " ClearLeaff Rust Fleet Downsizing Studio - Live Preview Server"
echo "================================================================"
echo "Serving live interactive FinOps studio on: http://127.0.0.1:$PORT"
echo "Press Ctrl+C to stop."
echo "================================================================"

node -e "
const http = require('http');
const fs = require('fs');
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/html' });
  fs.createReadStream('$HTML_FILE').pipe(res);
});
server.listen($PORT, () => {
  console.log('Server running at http://127.0.0.1:$PORT/');
});
"

