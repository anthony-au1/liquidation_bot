//TODO
- what if we corrupted data during event or sync? what's next in this case
- we need to re-sync users as we might end up missing some events, so we need to re-sync periodically
- we need to crop our cache data for not active users, we can persist data in redis
- we need to docker our app
- we need stop it gracefully by persisting data in redis, only users 




1. fix panic
2. replace user in supply