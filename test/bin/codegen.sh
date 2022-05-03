#!/bin/sh

name=$1
db=$2
params=$3
values=$4
compile_err=$(mktemp)

mkdir -p "$name.d"
../../bin/compile.exe $params -output-layout -o "$name.d" -db "$db" < "$name" 2> "$compile_err"
if [ $? -ne 0 ]; then
    cat "$compile_err"
fi
rm -f "$compile_err"

"$name.d/scanner.exe" -p "$name.d/data.bin" $values > "$name.result.output"
cp "$name.d/scanner.ll" "$name.ll.output"
cp "$name.d/scanner.ir" "$name.ir.output"
cp "$name.d/layout.txt" "$name.layout.output"
