#!/bin/bash

# 파일 경로 설정
INPUT_FILE="dags/input_raw.csv"
OUTPUT_FILE="dags/output_process.csv"
LOG_FILE="dags/process.log"



# 입력 파일 존재 여부 확인
if [[ ! -f "$INPUT_FILE" ]]; then
    echo "ERROR: 입력 파일이 존재하지 않습니다: $INPUT_FILE" | tee -a "$LOG_FILE"
    exit 1
fi

# input 파일을 output 파일로 덮어쓰기
cp "$INPUT_FILE" "$OUTPUT_FILE"

# 실행 결과 로그 저장
echo "CSV Overwrite Completed: $INPUT_FILE → $OUTPUT_FILE" | tee -a "$LOG_FILE"