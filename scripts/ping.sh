while true; do
  echo "---- $(date) ----"
  curl http://localhost:3000/test_probe/10
  sleep 300
done