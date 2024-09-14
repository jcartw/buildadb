# buildadb
Build a simple database. Based on cstack's DB tutorial: https://cstack.github.io/db_tutorial/.

## B-Tree Insertion

- Start with 1 root/leaf node (page 0) and insert in sorted order up to the max allowable records (13) 
- Upon the insertion of the next record:
    - Create a new node (page 1) and write the right-half of records into it
      while inserting the new record in the correct location to maintain sorted order.
    - Create a new node (page 2) and copy records from root node (original left half of page 0) into it.
    - Re-initialize root node (page 0) as an internal node and set left and right child
      pointers, pages 2 and 1, respectively.

## Interesting findings

- Sequential insertions lead to 50% B+Tree "fill factor", whereas random insertion provides a roughly 70% "fill factor" (see https://stackoverflow.com/questions/73498429/btree-splitting-leads-to-leaf-nodes-with-less-capacity).
- Depending on the setting of INTERNAL_NODE_MAX_CELLS, the sequential inserts may be more or less performant than random inserts.

## Main Reference

- https://planetscale.com/blog/btrees-and-database-indexes
- https://use-the-index-luke.com/
- https://www.cybertec-postgresql.com/en/what-is-fillfactor-and-how-does-it-affect-postgresql-performance/
- https://maciejwalkowiak.com/blog/postgres-uuid-primary-key/
- https://stackoverflow.com/questions/73498429/btree-splitting-leads-to-leaf-nodes-with-less-capacity
