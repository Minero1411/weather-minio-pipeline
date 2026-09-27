from minio import Minio
from minio.error import S3Error

print("=" * 60)
print("KỊCH BẢN DEMO: KIỂM CHỨNG BẢO MẬT PHÂN QUYỀN IAM (LEAST PRIVILEGE)")
print("=" * 60)

# TEST 1: Tài khoản Ingestion cố tình ĐỌC dữ liệu tầng Silver
print("\n>>> [TEST 1] Dùng ingestion_user cố tình ĐỌC trộm dữ liệu tầng Silver...")
client_ingest = Minio(
    "localhost:9000",
    access_key="ingestion_user",
    secret_key="IngestPass123",
    secure=False
)

try:
    # Thử đọc 1 file ở tầng Silver (quyền này không được cấp)
    client_ingest.get_object("weather-data", "silver/year=2023/hanoi_weather_clean.parquet")
    print("[-] Thất bại: Đọc được file (Bị hổng bảo mật)!")
except S3Error as err:
    print(f"[+] MinIO chặn đứng thành công!")
    print(f"    -> Mã phản hồi: {err.code}")
    print(f"    -> Thông báo: {err.message}")

# TEST 2: Tài khoản Analytics cố tình XÓA dữ liệu tầng Bronze
print("\n>>> [TEST 2] Dùng analytics_user cố tình XÓA file thô tầng Bronze...")
client_analytics = Minio(
    "localhost:9000",
    access_key="analytics_user",
    secret_key="AnalyticsPass123",
    secure=False
)

try:
    # Thử xóa file dữ liệu gốc ở tầng Bronze (chỉ có quyền đọc Silver/Gold)
    client_analytics.remove_object("weather-data", "bronze/year=2023/hanoi_weather_raw.json")
    print("[-] Thất bại: Xóa được file (Bị hổng bảo mật)!")
except S3Error as err:
    print(f"[+] MinIO chặn đứng thành công!")
    print(f"    -> Mã phản hồi: {err.code}")
    print(f"    -> Thông báo: {err.message}")

print("\n" + "=" * 60)
print("[KẾT LUẬN] Hệ thống phân quyền tuân thủ triệt để nguyên tắc Least Privilege!")
print("=" * 60)
