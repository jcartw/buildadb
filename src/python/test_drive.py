
from btree import Btree

btree = Btree()

data = []
for i in range(100):
    val = {"id": i, "user": f"person{i}", "email": f"person{i}@example.com"}
    data.append((i, val))

# shuffle input data
# for i in reversed(range(len(data))):
#     j = random.randint(0, i)
#     data[i], data[j] = data[j], data[i]

for key, val in data:
    btree.execute_insert(key, val)

print("---------------")
#btree.execute_select()
print("---------------")
btree.print()
print("---------------")