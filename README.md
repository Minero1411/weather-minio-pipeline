# 🌤️ Weather Data Lakehouse Pipeline with MinIO & Streamlit
Các bước thực hiện:

Bước 1: Khởi động MinIO bằng Docker
-Chạy lệnh trong terminal (trong folder dự án): docker compose up -d


Bước 2: Thiết lập môi trường Python & Cấu hình
-Cài đặt thư viện bằng lệnh terminal (trong folder dự án): pip install -r requirements.txt


Bước 3: Thu thập dữ liệu thô vào MinIO (Tầng Bronze)
-Chạy lệnh: python src/1_ingest.py
(Kéo dữ liệu từ Open-Meteo API, tạo bucket weather-data, nạp stream raw JSON lên MinIO và tạo bản sao lưu tại thư mục backup/bronze/)


Bước 4: Xử lý dữ liệu ETL & Nén Parquet (Tầng Silver & Gold)
-Chạy lệnh: python src/2_etl.py
(Làm sạch dữ liệu, nén sang Parquet đẩy lên Silver Layer, trích xuất đặc trưng thời gian (Feature Engineering) nén Snappy đẩy lên Gold Layer.)


Bước 5: Tạo IAM, chạy lần lượt các lệnh sau:
-PowerShell(Terminal):
docker cp ./policies/ingestion-policy.json minio:/tmp/ingestion-policy.json
docker cp ./policies/analytics-policy.json minio:/tmp/analytics-policy.json

(-Khởi tạo bí danh kết nối (alias) quản trị viên nội bộ:)
docker exec -it minio mc alias set local http://localhost:9000 admin password

(-Tạo các chính sách IAM:)
docker exec -it minio mc admin policy create local IngestionPolicy /tmp/ingestion-policy.json
docker exec -it minio mc admin policy create local AnalyticsPolicy /tmp/analytics-policy.json
docker exec -it minio mc admin user add local ingestion_user IngestPass123
docker exec -it minio mc admin user add local analytics_user AnalyticsPass123
docker exec -it minio mc admin policy attach local IngestionPolicy --user ingestion_user
docker exec -it minio mc admin policy attach local AnalyticsPolicy --user analytics_user

##Kiểm tra xác nhận
-Lệnh: docker exec -it minio mc admin user list local
(Nếu ra đủ 2 dòng analytics_user và ingestion_user kèm 2 policy tương ứng và đều có chữ enabled là oke)

Bước 6: Khởi chạy Weather Dashboard (Streamlit)
-Chạy lệnh: python -m streamlit run src/3_dashboard.py
-Mở giao diện trực quan hóa thời tiết tại http://localhost:8501
-Dừng Streamlit bằng cách nhấn (Ctrl + C)

#Thực hiện các bài demo:
1. Kịch bản bị tấn công dữ liệu trên Minio hoặc bằng cách nào đó mất dữ liệu trên Minio
-Khôi phục bằng lệnh: docker run --rm -v "${PWD}/backup:/backup" --network weather-minio-pipeline_iceberg_net minio/mc cp --recursive /backup/ minio/weather-data/

2. Đo Column Projection (Parquet vs JSON): 
-Lệnh: python demo/demo_2.py

3. Kiểm thử bảo mật phân quyền IAM (Least Privilege): cấu hình policy bằng mc
-Lệnh: python demo/demo_3.py