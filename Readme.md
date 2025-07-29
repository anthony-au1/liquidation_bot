//TODO
- what if we corrupted data during event or sync? what's next in this case
- we need to re-sync users as we might end up missing some events, so we need to re-sync periodically
- we need to crop our cache data for not active users, we can persist data in redis
- we need to docker our app
- we need stop it gracefully by persisting data in redis, only users 


test
1. sync_user - DONE
2. init_user - DONE
3. get_user_data - DONE
4. contains - DONE
5. remove_user - DONE
6. subscribe - DONE
7. sync_collateral - DONE
8. sync_borrowed - DONE
9. sync_data - DONE
10. calc_hf - DONE
11. create_user
12. handle_event 
13. start
14. setup
15. listen_events
16. listen_price_update
17. liquidation_threshold_update
18. liquidation_threshold_update_handler
19. listen_sync
20. listen_sync_handler 
21. listen_hf_calc
22. listen_hf_calc_handler