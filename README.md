# buildadb
Build a simple database

## B-Tree Insertion

- Start with 1 root/leaf node (page 0) and insert in sorted order up to the max allowable records (13) 
- Upon the insertion of the next record:
    - Create a new node (page 1) and write the right-half of records into it
      while inserting the new record in the correct location to maintain sorted order.
    - Create a new node (page 2) and copy records from root node (original left half of page 0) into it.
    - Re-initialize root node (page 0) as an internal node and set left and right child
      pointers, pages 2 and 1, respectively.


## Reference

- https://cstack.github.io/db_tutorial/
- https://use-the-index-luke.com/
