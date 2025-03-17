#!/bin/bash

# 파일 경로 설정
INPUT_FILE="/opt/airflow/plugins/shell/input_raw.csv"
OUTPUT_FILE="/opt/airflow/plugins/shell/output_process.csv"

echo "Input File Content ($INPUT_FILE):"
cat "$INPUT_FILE"

# input 파일을 output 파일로 덮어쓰기
cp "$INPUT_FILE" "$OUTPUT_FILE"
