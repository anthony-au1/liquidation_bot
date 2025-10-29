#!/bin/bash

while true; do
  echo "---- $(date) ----"
  curl --connect-timeout 5 --max-time 280 http://localhost:3000/test_probe/10
  sleep 300
done