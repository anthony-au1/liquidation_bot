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
11. supply - DONE
12. create_user - DONE
13. handle_event - DONE
14. start
15. setup - DONE
16. listen_events - DONE
17. listen_price_update - DONE
18. liquidation_threshold_update - DONE
19. liquidation_threshold_update_handler - DONE
20. listen_sync - DONE
21. listen_sync_handler - DONE
22. listen_hf_calc - DONE
23. listen_hf_calc_handler - DONE