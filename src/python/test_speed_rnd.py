from random import randint
from time import perf_counter
from btree import Btree


btree = Btree()

N = 10 ** 5
data = []
for i in range(N):
    val = {"id": i, "user": f"person{i}", "email": f"person{i}@example.com"}
    data.append((i, val))

# shuffle input data
for i in reversed(range(len(data))):
    j = randint(0, i)
    data[i], data[j] = data[j], data[i]

t1_start = perf_counter()
for key, val in data:
    btree.execute_insert(key, val)
t1_stop = perf_counter()

delta_t = round(t1_stop - t1_start, 3)
print(f"Elapsed time (N = {N}): {delta_t}")

# results: 1.782, 1.777, 1.777, 1.799, 1.816
