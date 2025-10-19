#!/bin/bash

while true; do
  echo "---- $(date) ----"
  curl -v http://localhost:3000/users
  sleep 60
done