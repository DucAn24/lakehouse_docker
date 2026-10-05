from delta.tables import DeltaTable

from common.config import create_spark_session, GOLD_BUCKET
from common.tables import TABLES

# Cấu hình SparkSession với Delta Lake và S3
spark = create_spark_session("LakehouseVacuum", f"{GOLD_BUCKET}/")

# Tắt chế độ kiểm tra an toàn của Delta Lake để cho phép VACUUM với retention = 0.0 (xóa ngay lập tức)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")


print("=" * 80)
print("Bắt đầu dọn dẹp các tệp Delta Lake cũ (VACUUM)...")
print("=" * 80)

for bucket, tables in TABLES.items():
    for table, path in tables.items():
        try:
            print(f"Đang dọn dẹp {bucket}.{table} tại {path}...")
            deltaTable = DeltaTable.forPath(spark, path)
            # vacuum(0.0) sẽ dọn dẹp sạch toàn bộ các file parquet cũ không còn được tham chiếu
            deltaTable.vacuum(0.0)
            print(f"✓ Hoàn tất dọn dẹp {bucket}.{table}")
        except Exception as e:
            # Bỏ qua nếu bảng chưa tồn tại hoặc rỗng
            print(f"⚠ Bỏ qua {bucket}.{table}: {str(e)}")

print("=" * 80)
print("Hoàn tất quy trình dọn dẹp giải phóng ổ cứng!")
print("=" * 80)

spark.stop()
