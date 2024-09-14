import copy
import random
from typing import Union

# Contstants
LEAF_NODE_MAX_CELLS = 13
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

INTERNAL_NODE_MAX_KEYS = 510

# Keep this small for testing
#INTERNAL_NODE_MAX_CELLS = 3
INTERNAL_NODE_MAX_CELLS = 500

# value for invalid page nums
INVALID_PAGE_NUM = -1

class BtreeNode:
    def __init__(self, is_root = False):
        # common fields
        self._is_root = is_root
        self._parent_ptr = 0

    def is_root(self):
        return self._is_root

    def set_is_root(self, is_root: bool):
        self._is_root = is_root

    def get_parent_ptr(self):
        return self._parent_ptr

    def set_parent_ptr(self, ptr):
        self._parent_ptr = ptr

class BtreeNodeLeaf(BtreeNode):
    def __init__(self, is_root = False):
        super().__init__(is_root)
        self._num_cells = 0
        self._next_leaf_ptr = 0 # 0 represents no sibling
        # preallocate cells array
        self._cell_list = [(0, {})] * LEAF_NODE_MAX_CELLS # (key, val)

    def copy(self):
        n = BtreeNodeLeaf()
        n._is_root = self._is_root
        n._parent_ptr = self._parent_ptr
        n._num_cells = self._num_cells
        n._next_leaf_ptr = self._next_leaf_ptr
        n._cell_list = copy.deepcopy(self._cell_list)
        return n

    def get_num_cells(self):
        return self._num_cells

    def set_num_cells(self, num_cells: int):
        self._num_cells = num_cells

    def get_next_leaf_ptr(self):
        return self._next_leaf_ptr

    def set_next_leaf_ptr(self, ptr: int):
        self._next_leaf_ptr = ptr

    def get_cell(self, cell_num: int):
        return self._cell_list[cell_num]

    def set_cell(self, cell_num, cell):
        self._cell_list[cell_num] = cell

    def get_key(self, cell_num: int):
        k, _ = self.get_cell(cell_num) # (key, val)
        return k

    def get_max_key_internal(self) -> int:
        k, _ = self.get_cell(self._num_cells - 1)
        return k
    


class BtreeNodeInternal(BtreeNode):
    def __init__(self, is_root = False):
        super().__init__(is_root)
        self._num_keys = 0
        self._right_child_pointer = INVALID_PAGE_NUM
        # preallocate cells array
        self._cell_list = [(0, 0)] * INTERNAL_NODE_MAX_KEYS # (child pointer, key)

    def copy(self):
        n = BtreeNodeInternal()
        n._is_root = self._is_root
        n._parent_ptr = self._parent_ptr
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

    def get_right_child_ptr(self):
        return self._right_child_pointer

    def set_right_child_ptr(self, ptr: int):
        self._right_child_pointer = ptr

    def get_key(self, cell_num: int):
        _, k = self.get_cell(cell_num) # (child pointer, key)
        return k

    def get_max_key_internal(self) -> int:
        _, k = self.get_cell(self._num_keys - 1)
        return k

    def get_child_ptr(self, child_num: int) -> int:
        if child_num > self._num_keys:
            msg = f"Tried to access child_num {child_num} > num_keys {self._num_keys}"
            raise Exception(msg)

        if child_num == self._num_keys:
            #return self._right_child_pointer
            right_child = self.get_right_child_ptr()
            if right_child == INVALID_PAGE_NUM:
                raise Exception("Tried to access right child of node, but was invalid page")
            return right_child
        else:
            child, _ = self._cell_list[child_num]
            if child == INVALID_PAGE_NUM:
                raise Exception("Tried to access child %d of node, but was invalid page")
            return child

    def find_child(self, key: int) -> int:
        # Return the index of the child which should contain
        # the given key.
        num_keys = self.get_num_keys()

        # Binary search
        min_index = 0
        max_index = num_keys # there is one more child than key
        while min_index != max_index:
            index = (min_index + max_index) // 2
            key_to_right = self.get_key(cell_num=index)
            if key_to_right >= key:
                max_index = index
            else:
                min_index = index + 1
        return min_index

    def update_key(self, old_key: int, new_key: int):
        old_child_index = self.find_child(old_key)
        child_ptr, _ = self.get_cell(old_child_index)
        self.set_cell(old_child_index, (child_ptr, new_key))

class Pager:
    def __init__(self):
        self._next_page = 1
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

    def get_node_max_key(self, node: Union[BtreeNodeLeaf,BtreeNodeInternal]) -> int:
        if isinstance(node, BtreeNodeLeaf):
            return node.get_max_key_internal()

        # need to traverse down RHS of tree for internal nodes
        right_child = self.get_page(node.get_right_child_ptr())
        return self.get_node_max_key(right_child)

class Cursor:
    def __init__(self, btree, page_num):
        self._btree = btree
        self._page_num = page_num
        self._cell_num = 0
        self._end_of_table = False

    def get_page_num(self):
        return self._page_num

    def set_page_num(self, page_num):
        self._page_num = page_num

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

        # Advance to next leaf node
        if self._cell_num >= node.get_num_cells():
            next_page_num = node.get_next_leaf_ptr()
            if next_page_num == 0:
                # This was the rightmost leaf
                self._end_of_table = True
            else:
                # move to next leaf and start at cell 0
                self.set_page_num(next_page_num)
                self.set_cell_num(0)

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
        self._btree._split_cnt_leaf_node += 1
        #  Create a new node and move half the cells over.
        #  Insert the new value in one of the two nodes.
        #  Update parent or create a new parent.

        old_node: BtreeNodeLeaf = self._btree._pager.get_page(self._page_num)
        old_max = self._btree._pager.get_node_max_key(old_node)
        new_page_num: int = self._btree._pager.get_unused_page_num()
        new_node: BtreeNodeLeaf = self._btree._pager.get_page(new_page_num)
        new_node.set_parent_ptr(old_node.get_parent_ptr())

        # Whenever we split a leaf node, update the sibling pointers. 
        # The old leaf’s sibling becomes the new leaf, and the new leaf’s 
        # sibling becomes whatever used to be the old leaf’s sibling.
        new_node.set_next_leaf_ptr(old_node.get_next_leaf_ptr())
        old_node.set_next_leaf_ptr(new_page_num)

        #  All existing keys plus new key should be divided
        #  evenly between old (left) and new (right) nodes.
        #  Starting from the right, move each key to correct position.

        for i in range(LEAF_NODE_MAX_CELLS, -1, -1):
            destination_node = new_node if i >= LEAF_NODE_LEFT_SPLIT_COUNT else old_node
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
            parent_page_num = old_node.get_parent_ptr()
            new_max = self._btree._pager.get_node_max_key(old_node)
            parent: BtreeNodeInternal = self._btree._pager.get_page(parent_page_num)
            parent.update_key(old_max, new_max)

            self._btree.internal_node_insert(parent_page_num, new_page_num)
            return 

class Btree:
    def __init__(self):
        self._pager = Pager()
        self._root_page_num = 0
        # split counts
        self._split_cnt_internal_node = 0
        self._split_cnt_leaf_node = 0
        self._split_cnt_root = 0
        # init root node (leaf node)
        root_node = BtreeNodeLeaf(is_root=True)
        self._pager.set_page(self._root_page_num, root_node)

    def print_split_counts(self):
        print(f"Split count (internal node): {self._split_cnt_internal_node}")
        print(f"Split count (leaf node): {self._split_cnt_leaf_node}")
        print(f"Split count (root): {self._split_cnt_root}")

    def get_cursor(self, page_num) -> Cursor:
        return Cursor(btree=self, page_num=page_num)

    def get_start(self) -> Cursor:
        cursor = self.table_find(0)
        node = self._pager.get_page(cursor.get_page_num())
        num_cells = node.get_num_cells()
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

        node: BtreeNodeInternal = self._pager.get_page(page_num=page_num)
        child_index = node.find_child(key)
        child_page_num = node.get_child_ptr(child_index)
        child = self._pager.get_page(page_num=child_page_num)

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
        self._split_cnt_root += 1

        # get current root
        root = self._pager.get_page(self._root_page_num)
        right_child = self._pager.get_page(right_child_page_num)

        if isinstance(root, BtreeNodeInternal):
            right_child = BtreeNodeInternal(is_root=False)
            self._pager.set_page(right_child_page_num, right_child)

        # Left child has data copied from old root
        left_child = root.copy()
        left_child.set_is_root(False)
        left_child_page_num = self._pager.get_unused_page_num()
        self._pager.set_page(left_child_page_num, left_child)

        if isinstance(left_child, BtreeNodeInternal):
            for i in range(0, left_child.get_num_keys()):
                ptr, _ = left_child.get_cell(i)
                child = self._pager.get_page(ptr)
                child.set_parent_ptr(left_child_page_num)
            child = self._pager.get_page(left_child.get_right_child_ptr())
            child.set_parent_ptr(left_child_page_num)

        # Root node is a new internal node with one key and two children
        root = BtreeNodeInternal(is_root=True)
        root.set_num_keys(1)
        left_child_max_key = self._pager.get_node_max_key(left_child)
        root.set_cell(cell_num=0, cell=(left_child_page_num, left_child_max_key))
        root.set_right_child_ptr(right_child_page_num)
        self._pager.set_page(self._root_page_num, root)
        left_child.set_parent_ptr(self._root_page_num)
        right_child.set_parent_ptr(self._root_page_num)

    def internal_node_insert(self, parent_page_num: int, child_page_num: int):

        #  Add a new child/key pair to parent that corresponds to child
        parent = self._pager.get_page(parent_page_num)
        child = self._pager.get_page(child_page_num)
        child_max_key = self._pager.get_node_max_key(child)

        index = parent.find_child(child_max_key)

        original_num_keys = parent.get_num_keys()

        if original_num_keys >= INTERNAL_NODE_MAX_CELLS:
            self.internal_node_split_and_insert(parent_page_num, child_page_num)
            return

        right_child_page_num = parent.get_right_child_ptr()

        # An internal node with a right child of INVALID_PAGE_NUM is empty
        if right_child_page_num == INVALID_PAGE_NUM:
            parent.set_right_child_ptr(child_page_num)
            return 

        right_child = self._pager.get_page(right_child_page_num)
        right_child_max_key = self._pager.get_node_max_key(right_child)

        # If we are already at the max number of cells for a node, we cannot increment
        # before splitting. Incrementing without inserting a new key/child pair
        # and immediately calling internal_node_split_and_insert has the effect
        # of creating a new key at (max_cells + 1) with an uninitialized value
        parent.set_num_keys(original_num_keys + 1)

        if (child_max_key > right_child_max_key):
            # replace right child
            parent.set_cell(original_num_keys, (right_child_page_num, right_child_max_key))
            parent.set_right_child_ptr(child_page_num)
        else:
            # Make room for the new cell
            for i in range(original_num_keys, index, -1):
                parent.set_cell(i, parent.get_cell(i - 1))
            parent.set_cell(index, (child_page_num, child_max_key))

    def internal_node_split_and_insert(self, parent_page_num: int, child_page_num: int) -> None:
        self._split_cnt_internal_node += 1

        old_page_num = parent_page_num
        old_node = self._pager.get_page(parent_page_num)
        old_max = self._pager.get_node_max_key(old_node)

        child = self._pager.get_page(child_page_num)
        child_max = self._pager.get_node_max_key(child)

        new_page_num = self._pager.get_unused_page_num()
            
        # Declaring a flag before updating pointers which
        # records whether this operation involves splitting the root -
        # if it does, we will insert our newly created node during
        # the step where the table's new root is created. If it does
        # not, we have to insert the newly created node into its parent
        # after the old node's keys have been transferred over. We are not
        # able to do this if the newly created node's parent is not a newly
        # initialized root node, because in that case its parent may have existing
        # keys aside from our old node which we are splitting. If that is true, we
        # need to find a place for our newly created node in its parent, and we
        # cannot insert it at the correct index if it does not yet have any keys
        splitting_root = old_node.is_root()

        if splitting_root:
            self.create_new_root(new_page_num)
            parent = self._pager.get_page(self._root_page_num)

            # If we are splitting the root, we need to update old_node to point
            # to the new root's left child, new_page_num will already point to
            # the new root's right child

            old_page_num, _ = parent.get_cell(0)
            old_node = self._pager.get_page(old_page_num)
        else:
            parent = self._pager.get_page(old_node.get_parent_ptr())
            new_node = BtreeNodeInternal(is_root=False)
            self._pager.set_page(new_page_num, new_node)

        old_num_keys = old_node.get_num_keys()
        cur_page_num = old_node.get_right_child_ptr()
        cur = self._pager.get_page(cur_page_num)

        # First put right child into new node and set right child of old node to invalid page number
        self.internal_node_insert(new_page_num, cur_page_num)
        cur.set_parent_ptr(new_page_num)
        old_node.set_right_child_ptr(INVALID_PAGE_NUM)

        # For each key until you get to the middle key, move the key and the child to the new node
        for i in range(INTERNAL_NODE_MAX_CELLS - 1, INTERNAL_NODE_MAX_CELLS // 2, -1):
            cur_page_num, _ = old_node.get_cell(i)
            cur = self._pager.get_page(cur_page_num)

            self.internal_node_insert(new_page_num, cur_page_num)
            cur.set_parent_ptr(new_page_num)

            old_num_keys -= 1
            old_node.set_num_keys(old_num_keys)

        # Set child before middle key, which is now the highest key, to be node's right child,
        # and decrement number of keys
        ptr, _ = old_node.get_cell(old_num_keys - 1)
        old_node.set_right_child_ptr(ptr)
        old_num_keys -= 1
        old_node.set_num_keys(old_num_keys)

        # Determine which of the two nodes after the split should contain the child to be inserted,
        # and insert the child
        max_after_split = self._pager.get_node_max_key(old_node)
        destination_page_num = old_page_num if child_max < max_after_split else new_page_num

        self.internal_node_insert(destination_page_num, child_page_num)
        child.set_parent_ptr(destination_page_num)
        parent.update_key(old_max, self._pager.get_node_max_key(old_node))

        if not splitting_root:
            self.internal_node_insert(old_node.get_parent_ptr(), new_page_num)
            new_node.set_parent_ptr(old_node.get_parent_ptr())

    def print(self, page_num: int = 0, indentation_level: int = 0):
        node = self._pager.get_page(page_num)

        if isinstance(node, BtreeNodeLeaf):
            num_keys = node.get_num_cells()
            indent = "  " * indentation_level
            print(f"{indent}- leaf (size {num_keys})")
            for i in range(num_keys):
                indent = "  " * (indentation_level + 1)
                print(f"{indent}- {node.get_key(i)}")
        elif isinstance(node, BtreeNodeInternal):
            num_keys = node.get_num_keys()
            indent = "  " * indentation_level
            print(f"{indent}- internal (size {num_keys})")
            if num_keys > 0:
                for i in range(num_keys):
                    child_page_num = node.get_child_ptr(child_num=i)
                    self.print(child_page_num, indentation_level + 1)
                    indent = "  " * (indentation_level + 1)
                    print(f"{indent}- key {node.get_key(i)}")
                child_page_num = node.get_right_child_ptr()
                self.print(child_page_num, indentation_level + 1)

