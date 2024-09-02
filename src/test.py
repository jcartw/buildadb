from subprocess import PIPE, Popen
import json
import os

# references:
# - https://stackoverflow.com/questions/77802033/c-program-and-subprocess

# clear test.db before each run of script
DB_FILENAME = "test.db"
def clear_db():
    if os.path.isfile(DB_FILENAME):
        os.remove(DB_FILENAME)

clear_db()

def run_script(commands):

    out = ""
    p = Popen([f"./db", DB_FILENAME], stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True)
    try:
        for command in commands:
            p.stdin.write(command + "\n")
            p.stdin.flush()
            out += p.stdout.readline()
    finally:
        p.kill()

    return list(filter(lambda x: len(x) > 0, out.split("\n")))

def equal_results(a, b):
    return json.dumps(a) == json.dumps(b)

# ----------------------------------------------------- #

it = "inserts and retrieves a row"
status = "FAILED"
clear_db()
result = run_script([
    'insert 1 user1 person1@example.com',
    'select',
    '.exit'
])
expectation = [
    "db > Executed.",
    "db > (1, user1, person1@example.com)",
    "Executed."
]
if equal_results(result, expectation):
    status = "PASSED"

print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "prints error message when table is full"
status = "FAILED"
clear_db()
statements = []
for i in range(1401):
    num = str(i + 1)
    statements.append(f"insert {num} user{num} person{num}@example.com")
statements.append(".exit")

result = run_script(statements)
if equal_results(result[-2], "db > Error: Table full."):
    status = "PASSED"
print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "allows inserting strings that are the maximum length"
status = "FAILED"
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
    "Executed."
]
if equal_results(result, expectation):
    status = "PASSED"
print(f"{it}: {status}")

# ----------------------------------------------------- #

it = "prints error message if strings are too long"
status = "FAILED"
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
    status = "PASSED"
print(f"{it}: {status}")


# ----------------------------------------------------- #

it = "prints an error message if id is negative"
status = "FAILED"
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
    status = "PASSED"
print(f"{it}: {status}")


# ----------------------------------------------------- #

it = "keeps data after closing connection"
status = "PASSED"
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
    status = "FAILED"

result = run_script([
    f"select",
    '.exit'
])
expectation = [
    "db > (1, user1, person1@example.com)",
    "Executed."
]
if not equal_results(result, expectation):
    status = "FAILED"

print(f"{it}: {status}")

