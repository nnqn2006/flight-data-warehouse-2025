# Flight DW 2025 – Validation Evidence Pack

## Mục tiêu
Bộ truy vấn này hỗ trợ thu thập bằng chứng về schema, quan hệ khóa ngoại và số lượng bản ghi của Flight Data Warehouse 2025. Đây là **bộ lệnh kiểm tra**, không phải kết quả kiểm định đã chạy. Chỉ đánh dấu PASS sau khi lưu output thực tế từ MySQL.

## Cấu trúc

- `01_RUN_NOW/01_schema_and_constraints.sql`: truy vấn metadata, danh sách bảng, cấu trúc cột và khóa ngoại. Chỉ đọc metadata, không quét dữ liệu fact/bridge.
- `01_RUN_NOW/02_dimension_counts.sql`: đếm 5 bảng dimension/mapping có quy mô nhỏ.
- `02_RUN_AFTER_ETL/03_fact_and_bridge_counts.sql`: đếm hai fact và bridge. **Chỉ chạy sau khi STEP 30E3 và STEP 30E4 hoàn thành, loader dừng và log xác nhận PASS.**
- `EVIDENCE_TRACKER.md`: checklist lưu bằng chứng và tên file đề xuất.

## Cách chạy

1. Mở MySQL Workbench và kết nối local MySQL.
2. Mở từng file SQL trong thư mục `01_RUN_NOW`, chạy từng file, kiểm tra kết quả rồi export kết quả hoặc chụp màn hình.
3. Lưu bằng chứng tại `09_FINAL_HANDOFF/04_DATABASE/04_VALIDATION/` và bản log/ảnh gốc tại `09_FINAL_HANDOFF/07_EVIDENCE/`.
4. Không chạy file trong `02_RUN_AFTER_ETL` khi Bridge loader còn hoạt động.
5. Không chạy nhiều câu `COUNT(*)` trên Bridge liên tiếp. Chỉ thực hiện một lần sau khi hoàn tất nạp và kiểm định.

## Kết quả kỳ vọng đã biết từ các bước ETL trước

| Bảng | Số dòng kỳ vọng/đã xác minh ở bước trước | Ghi chú |
|---|---:|---|
| `dim_airline` | 14 | Đã PASS ở STEP 30C |
| `dim_airport` | 86,335 | Đã PASS ở STEP 30C |
| `dim_date` | 370 | Đã PASS ở STEP 30B |
| `dim_time` | 1,442 | Đã PASS ở STEP 30B |
| `map_bts_airport` | 352 | Đã PASS ở STEP 30C |
| `fact_flight` | 7,001,619 | Đã PASS ở STEP 30E2 |
| `fact_airport_weather_hourly` | 3,083,520 | Đã PASS ở STEP 30E1 |
| `bridge_flight_weather_exposure` | 28,006,476 | Số dòng nguồn kỳ vọng; **chưa xem là số dòng thực tế đã xác minh** cho tới khi STEP 30E3/30E4 PASS |

## Nguyên tắc

- Không sửa RAW, STAGING hoặc CLEAN trong quá trình kiểm định.
- Không xóa hoặc chỉnh sửa checkpoint, journal hay log ETL.
- `INFORMATION_SCHEMA.TABLES.TABLE_ROWS` của InnoDB chỉ là ước lượng; dùng `COUNT(*)` khi cần số chính xác và chọn đúng thời điểm.
- Kết quả truy vấn metadata không thay thế được kiểm định dữ liệu thực tế.
