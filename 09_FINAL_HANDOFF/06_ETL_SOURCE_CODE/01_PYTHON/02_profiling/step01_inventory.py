
from pathlib import Path
import pandas as pd

# ==========================================
# 1. KHAI BÁO ĐƯỜNG DẪN
# ==========================================

ROOT_DIR = Path.cwd()

RAW_DIR = ROOT_DIR / "01_RAW_DATA"

OUTPUT_DIR = (
    ROOT_DIR
    / "02_DATA_PROFILING"
    / "00_inventory"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

if not RAW_DIR.exists():
    raise FileNotFoundError(
        f"Không tìm thấy thư mục: {RAW_DIR}\n"
        "Hãy chạy script tại thư mục gốc dự án."
    )

# ==========================================
# 2. KIỂM KÊ TẤT CẢ FILE
# ==========================================

records = []

for file in RAW_DIR.rglob("*"):

    if not file.is_file():
        continue

    relative_path = file.relative_to(RAW_DIR)

    records.append({
        "File_Name": file.name,
        "Source_Folder": (
            relative_path.parts[0]
            if len(relative_path.parts) > 0
            else ""
        ),
        "Relative_Path": str(relative_path),
        "Extension": file.suffix.lower(),
        "Size_Bytes": file.stat().st_size,
        "Size_MB": round(
            file.stat().st_size / (1024 ** 2), 3
        ),
        "Modified_Time": pd.Timestamp(
            file.stat().st_mtime,
            unit="s"
        )
    })

inventory = pd.DataFrame(records)

if inventory.empty:
    print("Không tìm thấy file nào trong RAW_DATA.")
    raise SystemExit(0)

# ==========================================
# 3. THỐNG KÊ THEO NGUỒN
# ==========================================

summary_source = (
    inventory
    .groupby("Source_Folder")
    .agg(
        Total_Files=("File_Name", "count"),
        Total_Size_MB=("Size_MB", "sum")
    )
    .reset_index()
)

# ==========================================
# 4. THỐNG KÊ THEO ĐỊNH DẠNG
# ==========================================

summary_extension = (
    inventory
    .groupby("Extension")
    .agg(
        Total_Files=("File_Name", "count"),
        Total_Size_MB=("Size_MB", "sum")
    )
    .reset_index()
)

# ==========================================
# 5. XUẤT KẾT QUẢ
# ==========================================

inventory.to_csv(
    OUTPUT_DIR / "01_file_inventory.csv",
    index=False,
    encoding="utf-8-sig"
)

summary_source.to_csv(
    OUTPUT_DIR / "02_summary_by_source.csv",
    index=False,
    encoding="utf-8-sig"
)

summary_extension.to_csv(
    OUTPUT_DIR / "03_summary_by_extension.csv",
    index=False,
    encoding="utf-8-sig"
)

# ==========================================
# 6. HIỂN THỊ KẾT QUẢ
# ==========================================

print("\n========== DATA INVENTORY ==========")

print("\nTổng số file:", len(inventory))

print(
    "Tổng dung lượng:",
    round(inventory["Size_MB"].sum() / 1024, 3),
    "GB"
)

print("\n--- Summary by Source ---")
print(summary_source.to_string(index=False))

print("\n--- Summary by Extension ---")
print(summary_extension.to_string(index=False))

print("\n--- First 20 Files ---")
print(
    inventory[
        ["File_Name", "Source_Folder",
         "Extension", "Size_MB"]
    ].head(20).to_string(index=False)
)

print("\nCác báo cáo đã được lưu tại:")
print(OUTPUT_DIR)
