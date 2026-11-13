# Evidence Tracker – Flight DW 2025

> Điền trạng thái và tên file sau khi đã thực sự chạy kiểm tra. Không đánh dấu PASS chỉ dựa trên số kỳ vọng trong tài liệu.

| ID | Hạng mục | Lệnh/nguồn | Trạng thái ban đầu | Bằng chứng cần lưu | Vị trí lưu |
|---|---|---|---|---|---|
| V01 | Database/server context | `01_schema_and_constraints.sql` – phần 1 | Chờ chạy | `V01_mysql_server_context.txt` hoặc ảnh chụp | `04_DATABASE/04_VALIDATION` |
| V02 | Danh sách 8 bảng và engine | `01_schema_and_constraints.sql` – phần 2 | Chờ chạy | `V02_table_inventory.csv` | `04_DATABASE/04_VALIDATION` |
| V03 | Metadata 154 cột | `01_schema_and_constraints.sql` – phần 3 | Chờ chạy | `V03_column_inventory.csv` | `05_DATASETS/02_DATA_DICTIONARY` và bản minh chứng tại `04_DATABASE/04_VALIDATION` |
| V04 | FK: 15 constraint / 18 mapping | `01_schema_and_constraints.sql` – phần 4–6 | Chờ chạy | `V04_foreign_key_inventory.csv` | `04_DATABASE/04_VALIDATION` |
| V05 | Exact counts 5 dimension/mapping | `02_dimension_counts.sql` | Chờ chạy | `V05_dimension_counts.csv` | `04_DATABASE/04_VALIDATION` |
| V06 | Exact counts hai fact | `03_fact_and_bridge_counts.sql` | Chờ STEP 30E4 PASS | `V06_fact_counts.csv` | `04_DATABASE/04_VALIDATION` |
| V07 | Exact Bridge row count | `03_fact_and_bridge_counts.sql` | Chờ STEP 30E3 + 30E4 PASS | `V07_bridge_count.csv` | `04_DATABASE/04_VALIDATION` |
| V08 | STEP 30E3 partition completion | ETL log + checkpoint | Chưa xác minh trạng thái mới nhất | Log cuối, checkpoint snapshot sau khi loader dừng | `07_EVIDENCE/01_ETL_LOGS` và `07_EVIDENCE/02_VALIDATION` |
| V09 | STEP 30E4 final validation | ETL log | Chờ chạy/PASS | Log có kết luận PASS và các chỉ số kiểm định | `07_EVIDENCE/01_ETL_LOGS` |
| V10 | Screenshot schema/relationships | MySQL Workbench | Chờ chụp | `V10_schema_tables.png`, `V10_fk_relationships.png` | `07_EVIDENCE/03_SCREENSHOTS` |

## Quy tắc lưu log

- Sao chép log đã kết thúc; không sao chép hoặc thao tác lên file journal/checkpoint đang được loader cập nhật.
- Không đổi tên hoặc xóa file nguồn gốc nếu script ETL còn tham chiếu tới chúng.
- Khi chụp ảnh, để lộ tên database, tên bảng/cột và kết quả truy vấn; che thông tin xác thực nếu có.
- Không đưa mật khẩu MySQL, file `.env`, credential hoặc private key vào bộ bàn giao.
