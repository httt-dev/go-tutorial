
### Check rep slot in postgres
```sql
SELECT * FROM pg_replication_slots;
SELECT pg_drop_replication_slot('debezium_slot_name');
```