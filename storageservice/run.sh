#!/bin/bash
redis-server --daemonize yes &
python3 cache.py &
python3 schedule.py &
python3 app.py