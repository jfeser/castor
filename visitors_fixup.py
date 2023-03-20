import sys
import re

constrs = [
    "Name",
    "Int",
    "Fixed",
    "Date",
    "Bool",
    "String",
    "Null",
    "Unop",
    "Binop",
    "Count",
    "Row_number",
    "Sum",
    "Avg",
    "Min",
    "Max",
    "If",
    "First",
    "Exists",
    "Substring",
]

with open(sys.argv[1], "r") as f:
    text = f.read()

for constr in constrs:
    text = re.sub(f"([^`_]){constr} ", r"\1`" + constr + " ", text)
print(text)
