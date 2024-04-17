from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import pandas as pd

# Khởi tạo phiên Spark 
spark = SparkSession.builder \
    .appName("collect_data") \
    .master("spark://192.168.1.11:7077") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "6") \
    .getOrCreate()

# Đọc dữ liệu từ tệp CSV
data = pd.read_csv("Diem_KyThiTotNghiepTHPT_2023.csv")
# Kết nối đến cụm Cassandra
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

NameTable = 'spark_add_data'
# Chọn keyspace hoặc tạo mới nếu chưa tồn tại
session.execute("CREATE KEYSPACE IF NOT EXISTS spark_add_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }")
session.set_keyspace(NameTable)

# Tạo bảng trong keyspace đã chọn
create_table_query = """
    CREATE TABLE IF NOT EXISTS students (
        student_id TEXT,
        mathematics FLOAT,
        literature FLOAT,
        foreign_language FLOAT,
        physics FLOAT,
        chemistry FLOAT,
        biology FLOAT,
        history FLOAT,
        geography FLOAT,
        civic_education FLOAT,
        foreign_language_code TEXT,
        PRIMARY KEY (student_id)
    )
"""
session.execute(create_table_query)

# Chèn dữ liệu từ DataFrame vào bảng Cassandra
for index, row in data.iterrows():
    insert_query = """
        INSERT INTO students (
            student_id, 
            mathematics, 
            literature, 
            foreign_language, 
            physics, 
            chemistry, 
            biology, 
            history, 
            geography, 
            civic_education, 
            foreign_language_code
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    session.execute(insert_query, (
        str(row['Student ID']),
        row['Mathematics'] if not pd.isnull(row['Mathematics']) else None,
        row['Literature'] if not pd.isnull(row['Literature']) else None,
        row['Foreign language'] if not pd.isnull(row['Foreign language']) else None,
        row['Physics'] if not pd.isnull(row['Physics']) else None,
        row['Chemistry'] if not pd.isnull(row['Chemistry']) else None,
        row['Biology'] if not pd.isnull(row['Biology']) else None,
        row['History'] if not pd.isnull(row['History']) else None,
        row['Geography'] if not pd.isnull(row['Geography']) else None,
        row['Civic education'] if not pd.isnull(row['Civic education']) else None,
        row['Foreign language code'] if not pd.isnull(row['Foreign language code']) else None
    ))

print("Dữ liệu đã được thêm vào cơ sở dữ liệu Cassandra thành công!")

# Đóng kết nối
session.shutdown()
cluster.shutdown()
spark.stop()
