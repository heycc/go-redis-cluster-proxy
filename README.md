# go-redis-cluster-proxy
redis-cluster proxy build on go

The github project [site](http://blog.heycc.net/go-redis-cluster-proxy)

## TODO
1. When slot migrated, update slot mapping
2. Reconfig connection pool when some slot is migrated to new added node, or when one node serves no slot, delete it from pool
3. Admin Util
  * migrating slots