#!/bin/bash

# 파일 경로 설정
INPUT_FILE="dags/input_raw.csv"
OUTPUT_FILE="dags/output_process.csv"


echo "Input File Content ($INPUT_FILE):"
cat "$INPUT_FILE"

# input 파일을 output 파일로 덮어쓰기
cp "$INPUT_FILE" "$OUTPUT_FILE"
