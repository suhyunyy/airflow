#!/bin/bash

INPUT_FILE="dags/input_raw.csv"
OUTPUT_FILE="dags/output_process.csv"

# output_process.csv 파일이 없으면 생성
touch "$OUTPUT_FILE"

# 헤더 저장
head -n 1 "$INPUT_FILE" > "$OUTPUT_FILE"

# 필터링 (age >= 30)
awk -F, 'NR>1 && $3 >= 30 {print}' "$INPUT_FILE" >> "$OUTPUT_FILE"

echo "CSV Processing Completed: Saved to $OUTPUT_FILE"
