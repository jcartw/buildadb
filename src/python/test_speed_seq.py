
from time import perf_counter
from btree import Btree

btree = Btree()

N = 10 ** 5
data = []
for i in range(N):
    val = {"id": i, "user": f"person{i}", "email": f"person{i}@example.com"}
    data.append((i, val))

t1_start = perf_counter()
for key, val in data:
    btree.execute_insert(key, val)
t1_stop = perf_counter()

delta_t = round(t1_stop - t1_start, 3)
print(f"Elapsed time (N = {N}): {delta_t}")

# results: 2.294, 2.245, 2.268, 2.239, 2.231

