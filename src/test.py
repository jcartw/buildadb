from subprocess import PIPE, Popen
from queue import Queue, Empty
from threading  import Thread
import json
import os
import time

# references:
# - https://stackoverflow.com/questions/77802033/c-program-and-subprocess
# - https://stackoverflow.com/questions/375427/a-non-blocking-read-on-a-subprocess-pipe-in-python

# clear test.db before each run of script
DB_FILENAME = "test.db"
def clear_db():
    if os.path.isfile(DB_FILENAME):
        os.remove(DB_FILENAME)
clear_db()


class StdoutQueue:
    def __init__(self, stdout):
        self.q = Queue()
        self.stopped = True
        self.stdout = stdout

    def stop(self):
        self.stopped = True

    def capture_lines(self):
        for line in iter(self.stdout.readline, b''):
            if len(line) > 0:
                self.q.put(line)
            if self.stopped:
                break

    def start(self):
        self.stopped = False
        t = Thread(target=self.capture_lines, args=())
        t.daemon = True
        t.start()

    def get_results(self):
        out = ""
        while self.q.qsize() > 0:
            out += self.q.get_nowait()
        return out


def run_script(commands):

    out = ""
    p = Popen([f"./db", DB_FILENAME], stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True)
    q = StdoutQueue(p.stdout)
    q.start()
    #SLEEP_TIME = 0.001
    SLEEP_TIME = 0.001 / 2 ** 4

    try:
        for command in commands:
            p.stdin.write(command + "\n")
            p.stdin.flush()
            time.sleep(SLEEP_TIME)
    finally:
        q.stop()
        time.sleep(SLEEP_TIME)
        p.kill()

    # get results
    time.sleep(0.1)
    out = q.get_results()

    return list(filter(lambda x: len(x) > 0, out.split("\n")))

def equal_results(a, b):
    return json.dumps(a) == json.dumps(b)

# ----------------------------------------------------- #

it = "inserts and retrieves a row"
status = "FAILED ❌"
clear_db()
result = run_script([
    'insert 1 user1 person1@example.com',
    'select',
    '.exit'
])
expectation = [
    "db > Executed.",
    "db > (1, user1, person1@example.com)",
    "Executed.",
    "db > "
]
if equal_results(result, expectation):
    status = "PASSED ✅"

print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "prints error message when table is full"
status = "FAILED ❌"
clear_db()
statements = []
for i in range(1401):
    num = str(i + 1)
    statements.append(f"insert {num} user{num} person{num}@example.com")
statements.append(".exit")

result = run_script(statements)
if equal_results(result[-2], "db > Error: Table full."):
    status = "PASSED ✅"
print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "allows inserting strings that are the maximum length"
status = "FAILED ❌"
clear_db()
long_username = "a" * 32
long_email = "a" * 255

result = run_script([
    f"insert 1 {long_username} {long_email}",
    'select',
    '.exit'
])
expectation = [
    "db > Executed.",
    f"db > (1, {long_username}, {long_email})",
    "Executed.",
    "db > "
]
if equal_results(result, expectation):
    status = "PASSED ✅"

print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "prints error message if strings are too long"
status = "FAILED ❌"
clear_db()
long_username = "a" * 33
long_email = "a" * 256

result = run_script([
    f"insert 1 {long_username} {long_email}",
    'select',
    '.exit'
])
expectation = [
    "db > String is too long.",
    "db > Executed.",
    "db > "
]
if equal_results(result, expectation):
    status = "PASSED ✅"
print(f"{it}: {status}")


# ----------------------------------------------------- #

it = "prints an error message if id is negative"
status = "FAILED ❌"
clear_db()

result = run_script([
    f"insert -1 cstack foo@bar.com",
    'select',
    '.exit'
])
expectation = [
    "db > ID must be positive.",
    "db > Executed.",
    "db > "
]
if equal_results(result, expectation):
    status = "PASSED ✅"
print(f"{it}: {status}")


# ----------------------------------------------------- #

it = "keeps data after closing connection"
status = "PASSED ✅"
clear_db()

result = run_script([
    f"insert 1 user1 person1@example.com",
    '.exit'
])
expectation = [
    "db > Executed.",
    "db > "
]
if not equal_results(result, expectation):
    status = "FAILED ❌"

result = run_script([
    f"select",
    '.exit'
])
expectation = [
    "db > (1, user1, person1@example.com)",
    "Executed.",
    "db > "
]
if not equal_results(result, expectation):
    status = "FAILED ❌"

print(f"{it}: {status}")


# ----------------------------------------------------- #

it = "prints constants"
status = "PASSED ✅"
clear_db()

result = run_script([
    ".constants",
    '.exit'
])
expectation = [
    "db > Constants:",
    "ROW_SIZE: 293",
    "COMMON_NODE_HEADER_SIZE: 6",
    "LEAF_NODE_HEADER_SIZE: 10",
    "LEAF_NODE_CELL_SIZE: 297",
    "LEAF_NODE_SPACE_FOR_CELLS: 4086",
    "LEAF_NODE_MAX_CELLS: 13",
    "db > "
]
if not equal_results(result, expectation):
    status = "FAILED ❌"

print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "allows printing out the structure of a one-node btree"
status = "PASSED ✅"
clear_db()

commands = []
for n in [3, 1, 2]:
    commands.append(f"insert {n} user{n} person{n}@example.com")
commands.append(".btree")
commands.append(".exit")

result = run_script(commands)
expectation = [
    "db > Executed.",
    "db > Executed.",
    "db > Executed.",
    "db > Tree:",
    "leaf (size 3)",
    "  - 0 : 1",
    "  - 1 : 2",
    "  - 2 : 3",
    "db > "
]
if not equal_results(result, expectation):
    status = "FAILED ❌"

print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "prints an error message if there is a duplicate id"
status = "FAILED ❌"
clear_db()

result = run_script([
    "insert 1 user1 person1@example.com",
    "insert 1 user1 person1@example.com",
    'select',
    '.exit'
])
expectation = [
    "db > Executed.",
    "db > Error: Duplicate key.",
    "db > (1, user1, person1@example.com)",
    "Executed.",
    "db > ",
]
if equal_results(result, expectation):
    status = "PASSED ✅"
print(f"{it}: {status}")

