#!/bin/bash
cd "$(dirname "$0")"
# Sleep briefly to ensure server starts, then open browser in background
(sleep 2 && open "http://127.0.0.1:5001") &
./venv/bin/python app.py
