import pandas as pd
import random
import sqlite3  # 간단한 테스트용 DB

# Task 1: CSV 데이터 읽기
def download_data():
    df = pd.read_csv("dags/input_data.csv")
    print("데이터 로드 완료")
    return df

# Task 2: 데이터 처리 (old_column 삭제, age_group 추가)
def process_data(df):
    df = df.drop(columns=["old_column"])  # 필요 없는 컬럼 삭제
    df["age_group"] = df["age"].apply(lambda x: "Young" if x < 30 else "Adult")
    print("데이터 처리 완료")
    return df

# Task 3: 데이터 저장 (50% 확률로 실패)
def store_data(df):
    if random.choice([True, False]):
        raise ConnectionError("데이터베이스 연결 실패!")
    conn = sqlite3.connect("test_db.sqlite")  # 간단한 SQLite DB 사용
    df.to_sql("users", conn, if_exists="replace", index=False)
    conn.close()
    print("데이터 저장 완료")

# 실행 순서
try:
    data = download_data()  # 1️ 데이터 읽기
    processed_data = process_data(data)  # 2️ 데이터 변환
    store_data(processed_data)  # 3️ 데이터 저장
except Exception as e:
    print(f"오류 발생: {e}")
