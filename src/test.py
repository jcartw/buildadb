from subprocess import PIPE, Popen

# references:
# - https://stackoverflow.com/questions/77802033/c-program-and-subprocess

def run_script(commands):
    out = ""
    p = Popen(["./db"], stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True)
    try:
        for command in commands:
            p.stdin.write(command + "\n")
            p.stdin.flush()
            out += p.stdout.readline()
    finally:
        p.kill()

    return list(filter(lambda x: len(x) > 0, out.split("\n")))

#    result = run_script([
#      "insert 1 user1 person1@example.com",
#      "select",
#      ".exit",
#    ])

result = run_script([
    'insert 1 foo bar@example.com',
    'insert 2 greg greg@example.com',
    'select',
    '.exit'
])

print("RESULT")
print(result)
