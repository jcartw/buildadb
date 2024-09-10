import copy
from typing import Union

# Contstants
LEAF_NODE_MAX_CELLS = 13
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

INTERNAL_NODE_MAX_KEYS = 510

class BtreeNode:
    def __init__(self, is_root = False):
        # common fields
        self._is_root = is_root
        self._parent_pointer = 0

    def is_root(self):
        return self._is_root

    def set_is_root(self, is_root: bool):
        self._is_root = is_root


#uint32_t get_node_max_key(void* node) {
#  switch (get_node_type(node)) {
#    case NODE_INTERNAL:
#      return *internal_node_key(node, *internal_node_num_keys(node) - 1);
#    case NODE_LEAF:
#      return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
#  }
#}

class BtreeNodeLeaf(BtreeNode):
    def __init__(self, is_root = False):
        super().__init__(is_root)
        self._num_cells = 0
        # preallocate cells array
        self._cell_list = [(0, {})] * LEAF_NODE_MAX_CELLS # (key, val)

    def copy(self):
        n = BtreeNodeLeaf()
        n._is_root = self._is_root
        n._parent_pointer = self._parent_pointer
        n._num_cells = self._num_cells
        n._cell_list = copy.deepcopy(self._cell_list)
        return n

    def get_type(self):
        return self._node_type

    def get_num_cells(self):
        return self._num_cells

    def get_cell(self, cell_num: int):
        return self._cell_list[cell_num]

    def get_key(self, cell_num: int):
        k, _ = self.get_cell(cell_num) # (key, val)
        return k

    def get_max_key(self) -> int:
        k, _ = self.get_cell(self._num_cells - 1)
        return k

    def set_cell(self, cell_num, cell):
        self._cell_list[cell_num] = cell

    def set_num_cells(self, num_cells: int):
        self._num_cells = num_cells

class BtreeNodeInternal(BtreeNode):
    def __init__(self, is_root = False):
        super().__init__(is_root)
        self._num_keys = 0
        self._right_child_pointer = 0
        # preallocate cells array
        self._cell_list = [(0, 0)] * INTERNAL_NODE_MAX_KEYS # (child pointer, key)

    def copy(self):
        n = BtreeNodeInternal()
        n._is_root = self._is_root
        n._parent_pointer = self._parent_pointer
        n._num_keys = self._num_keys
        n._right_child_pointer = self._right_child_pointer
        n._cell_list = copy.deepcopy(self._cell_list)
        return n

    def get_type(self):
        return self._node_type

    def get_num_keys(self):
        return self._num_keys

    def set_num_keys(self, num_keys: int):
        self._num_keys = num_keys

    def get_num_cells(self):
        return self._num_keys

    def get_cell(self, cell_num: int):
        return self._cell_list[cell_num]

    def set_cell(self, cell_num: int, cell):
        self._cell_list[cell_num] = cell

    def set_right_child_ptr(self, ptr: int):
        self._right_child_pointer = ptr

    def get_key(self, cell_num: int):
        _, k = self.get_cell(cell_num) # (child pointer, key)
        return k

    def get_max_key(self) -> int:
        _, k = self.get_cell(self._num_keys - 1)
        return k

    def get_child_ptr(self, child_num: int) -> int:
        if child_num > self._num_keys:
            msg = f"Tried to access child_num {child_num} > num_keys {self._num_keys}"
            raise Exception(msg)

        if child_num == self._num_keys:
            return self._right_child_pointer
        else:
            ptr, _ = self._cell_list[child_num]
            return ptr

class Pager:
    def __init__(self):
        self._next_page = 0
        self._node_map = {}

    def get_unused_page_num(self):
        out = self._next_page
        self._next_page += 1
        return out

    def get_page(self, page_num: int) -> Union[BtreeNodeLeaf,BtreeNodeInternal]:
        # cache-hit
        if page_num in self._node_map:
            return self._node_map[page_num]

        # cache-miss
        # create and return new node (default leaf)
        n = BtreeNodeLeaf(is_root=False)
        self._node_map[page_num] = n
        return n

    def set_page(self, page_num, node):
        self._node_map[page_num] = node

class Cursor:
    def __init__(self, btree, page_num):
        self._btree = btree
        self._page_num = page_num
        self._cell_num = 0
        self._end_of_table = False

    def get_cell_num(self):
        return self._cell_num

    def set_cell_num(self, cell_num: int):
        self._cell_num = cell_num

    def is_end_of_table(self):
        return self._end_of_table

    def set_end_of_table(self, end_of_table: bool):
        self._end_of_table = end_of_table

    def value(self):
        node: BtreeNodeLeaf = self._btree._pager.get_page(self._page_num)
        _, v = node.get_cell(cell_num=self._cell_num)
        return v

    def advance(self):
        node: BtreeNodeLeaf = self._btree._pager.get_page(self._page_num)
        self._cell_num += 1

        if self._cell_num >= node.get_num_cells():
            self._end_of_table = True

    def leaf_node_insert(self, key: int, val) -> None:
        node: BtreeNodeLeaf = self._btree._pager.get_page(self._page_num)
        num_cells = node.get_num_cells()

        if num_cells >= LEAF_NODE_MAX_CELLS:
            # Node full
            self.leaf_node_split_and_insert(key, val)
            return

        # make room for new cell
        if self._cell_num < num_cells:
            for i in range(num_cells, self._cell_num, -1):
                node.set_cell(i, node.get_cell(i - 1))

        # set new cell value
        node.set_cell(self._cell_num, (key, val))
        node.set_num_cells(num_cells + 1)

    def leaf_node_split_and_insert(self, key: int, val):
        #  Create a new node and move half the cells over.
        #  Insert the new value in one of the two nodes.
        #  Update parent or create a new parent.

        old_node: BtreeNodeLeaf = self._btree._pager.get_page(self._page_num)
        new_page_num: int = self._btree._pager.get_unused_page_num()
        new_node: BtreeNodeLeaf = self._btree._pager.get_page(new_page_num)

        #  All existing keys plus new key should be divided
        #  evenly between old (left) and new (right) nodes.
        #  Starting from the right, move each key to correct position.

        for i in range(LEAF_NODE_MAX_CELLS, -1, -1):
            destination_node = new_node if i > LEAF_NODE_LEFT_SPLIT_COUNT else old_node
            index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT

            if i == self._cell_num:
                destination_node.set_cell(cell_num=index_within_node, cell=(key, val))
            elif i > self._cell_num:
                destination_node.set_cell(cell_num=index_within_node, cell=old_node.get_cell(i - 1))
            else:
                destination_node.set_cell(cell_num=index_within_node, cell=old_node.get_cell(i))

            # update cell counts
            old_node.set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT)
            new_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT)

        if old_node.is_root():
            return self._btree.create_new_root(right_child_page_num=new_page_num)
        else:
            raise Exception("Need to implement updating parent after split")

class Btree:
    def __init__(self):
        self._pager = Pager()
        self._root_page_num = 0

        # init root node (leaf node)
        root_node = BtreeNodeLeaf(is_root=True)
        self._pager.set_page(self._root_page_num, root_node)

    def get_cursor(self, page_num) -> Cursor:
        return Cursor(btree=self, page_num=page_num)

    def get_start(self) -> Cursor:
        cursor = Cursor(btree=self, page_num=self._root_page_num)
        cursor.set_cell_num(0)

        root_node = self._pager.get_page(self._root_page_num)
        num_cells = root_node.get_num_cells()
        cursor.set_end_of_table(num_cells == 0)

        return cursor

    def execute_insert(self, key: int, val):

        # get root node
        node = self._pager.get_page(self._root_page_num)
        num_cells = node.get_num_cells()

        # find cursor for insert location
        cursor = self.table_find(key)

        # check for duplicate key
        if cursor.get_cell_num() < num_cells:
            key_at_index = node.get_key(cell_num=cursor.get_cell_num())
            if key_at_index == key:
                raise Exception(f"Cannot insert a duplicate key: {key}")

        # insert value at leaf node
        cursor.leaf_node_insert(key, val)

    def execute_select(self):
        cursor = self.get_start()
        while not cursor.is_end_of_table():
            print(cursor.value())
            cursor.advance()

    def table_find(self, key: int) -> Cursor:
        root_node = self._pager.get_page(self._root_page_num)
        if isinstance(root_node, BtreeNodeLeaf):
            return self.leaf_node_find(self._root_page_num, key)
        elif isinstance(root_node, BtreeNodeInternal):
            return self.internal_node_find(self._root_page_num, key)
        else:
            raise Exception(f"Unknown instance type for {root_node}")

    def leaf_node_find(self, page_num: int, key: int):
        node = self._pager.get_page(page_num)
        num_cells = node.get_num_cells()

        # get cursor
        cursor = self.get_cursor(page_num)

        # binary search
        min_index = 0
        one_past_max_index = num_cells
        while one_past_max_index != min_index:
            index = (min_index + one_past_max_index) // 2
            key_at_index = node.get_key(cell_num=index)
            if key == key_at_index:
                cursor.set_cell_num(index)
                return cursor
            if key < key_at_index:
                one_past_max_index = index
            else:
                min_index = index + 1

        cursor.set_cell_num(min_index)
        return cursor

    def internal_node_find(self, page_num: int, key: int) -> Cursor:
        node = self._pager.get_page(page_num=page_num)
        num_keys = node.get_num_keys()

        # Binary search to find index of child to search
        min_index = 0
        max_index = num_keys # there is one more child than key

        while min_index != max_index:
            index = (min_index + max_index) // 2
            key_to_right = node.get_key(cell_num=index)
            if key_to_right >= key:
                max_index = index
            else:
                min_index = index + 1

        child_page_num = node.get_child_ptr(child_num=min_index)
        child = self._pager.get_page(child_page_num)

        if isinstance(child, BtreeNodeLeaf):
            return self.leaf_node_find(child_page_num, key)
        elif isinstance(child, BtreeNodeInternal):
            return self.internal_node_find(child_page_num, key)
        else:
            raise Exception(f"Unknown instance type for {child}")

    def create_new_root(self, right_child_page_num: int):
        #  Handle splitting the root.
        #  Old root copied to new page, becomes left child.
        #  Address of right child passed in.
        #  Re-initialize root page to contain the new root node.
        #  New root node points to two children.

        # get current root
        root = self._pager.get_page(self._root_page_num)

        # Left child has data copied from old root
        left_child = root.copy()
        left_child.set_is_root(False)
        left_child_page_num = self._pager.get_unused_page_num()
        self._pager.set_page(left_child_page_num, left_child)

        # Root node is a new internal node with one key and two children
        root = BtreeNodeInternal(is_root=True)
        root.set_num_keys(1)
        left_child_max_key = left_child.get_max_key()
        root.set_cell(cell_num=0, cell=(left_child_page_num, left_child_max_key))
        root.set_right_child_ptr(right_child_page_num)
        self._pager.set_page(self._root_page_num, root)


if __name__ == "__main__":
    btree = Btree()

    data = []
    for i in range(5):
        val = {"id": i, "user": f"person{i}", "email": f"person{i}@example.com"}
        btree.execute_insert(key=i, val=val)

    print(btree)
    btree.execute_select()

    
