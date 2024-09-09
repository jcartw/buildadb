
from enum import Enum

# Contstants
LEAF_NODE_MAX_CELLS = 13
INTERNAL_NODE_MAX_KEYS = 510

class NodeType(Enum):
    NODE_INTERNAL = 1
    NODE_LEAF = 2

class BtreeNode:
    def __init__(self, is_root = False):
        # common fields
        self._is_root = is_root
        self._parent_pointer = 0

class BtreeNodeLeaf(BtreeNode):
    def __init__(self, is_root = False):
        super(is_root)
        self._node_type = NodeType.NODE_LEAF
        self._num_cells = 0
        self._cell_list = [{"key": 0, "value": 0}] * LEAF_NODE_MAX_CELLS

class BtreeNodeInternal(BtreeNode):
    def __init__(self, is_root = False):
        super(is_root)
        self._node_type = NodeType.NODE_INTERNAL
        self._num_keys = 0
        self._right_child_pointer = 0
        self._keys_list = [ 0 ] * INTERNAL_NODE_MAX_KEYS
        self._child_pointer_list = [ 0 ] * INTERNAL_NODE_MAX_KEYS


class Pager:
    def __init__(self):
        self._next_page = 0
        self._node_list = []

    def get_unused_page_num(self):
        out = self._next_page
        self._next_page += 1
        return out

class Btree:
    def __init__(self):
        pass