# DATABASE GUIDE – FLIGHT DATA WAREHOUSE 2025

## 1. Mục đích

Tài liệu này hướng dẫn nhóm tiếp nhận cách kết nối, đọc cấu trúc và khai thác cơ sở dữ liệu `flight_dw_2025`. MySQL là nguồn dữ liệu DW trung tâm; Power BI kết nối để xây dựng báo cáo và dashboard.

## 2. Môi trường triển khai

| Thành phần | Giá trị |
|---|---|
| DBMS | MySQL Community Server 8.0.46 |
| Database / Schema | `flight_dw_2025` |
| Host | `127.0.0.1` |
| Port | `3306` |
| Storage engine | InnoDB |
| Character set | `utf8mb4` |
| Collation | `utf8mb4_0900_ai_ci` |

**Lưu ý bảo mật:** Không ghi mật khẩu MySQL vào tài liệu, mã nguồn, GitHub hoặc file bàn giao. Mỗi thành viên sử dụng tài khoản riêng hoặc thông tin xác thực được cấp qua kênh an toàn. Không mở cổng 3306 trực tiếp ra Internet.

## 3. Mô hình dữ liệu vật lý

CSDL có 8 bảng: 2 bảng Fact, 1 bảng Bridge, 4 Dimension và 1 bảng ánh xạ mã sân bay.

| Bảng | Grain / vai trò | Số dòng tham chiếu |
|---|---|---:|
| `fact_flight` | Một dòng cho một chuyến bay trong nguồn BTS | 7,001,619 |
| `fact_airport_weather_hourly` | Một dòng cho một sân bay tại một giờ UTC | 3,083,520 |
| `bridge_flight_weather_exposure` | Một dòng cho một FlightKey × ExposureType × SegmentSequence | 28,006,476 dòng dự kiến; cần xác nhận sau ETL và validation |
| `dim_airline` | Một dòng cho một hãng hàng không | 14 |
| `dim_airport` | Một dòng cho một sân bay OurAirports | 86,335 |
| `dim_date` | Một dòng cho một ngày trong phạm vi lịch | 370 |
| `dim_time` | Một dòng cho một time key, bao gồm 24:00 và Unknown | 1,442 |
| `map_bts_airport` | Bảng crosswalk mã sân bay BTS sang OurAirports | 352 |

### 3.1. Vai trò các bảng

- `fact_flight`: dữ liệu chuyến bay BTS, gồm lịch trình, thời gian thực tế, độ trễ, hủy chuyến và chuyển hướng.
- `fact_airport_weather_hourly`: dữ liệu thời tiết theo sân bay và giờ UTC từ Open-Meteo.
- `bridge_flight_weather_exposure`: liên kết chuyến bay với các mốc/phân đoạn thời tiết tương ứng.
- `dim_airline`, `dim_airport`, `dim_date`, `dim_time`: các chiều dùng để phân tích.
- `map_bts_airport`: ánh xạ mã sân bay BTS sang định danh sân bay OurAirports; đây là bảng mapping, không phải Dimension thông thường.

Mô hình thuộc dạng Fact Constellation / Galaxy Schema: các Fact dùng chung những Dimension liên quan.

## 4. Các quan hệ khóa ngoại chính

- `fact_flight` tham chiếu `dim_airline`, `dim_airport`, `dim_date`, `dim_time`.
- `fact_airport_weather_hourly` tham chiếu `dim_airport`, `dim_date`, `dim_time`.
- `bridge_flight_weather_exposure` tham chiếu `fact_flight`, `fact_airport_weather_hourly` và `dim_airport`.
- `map_bts_airport` tham chiếu `dim_airport` bằng khóa ghép.

Một số quan hệ sân bay dùng khóa ghép `AirportKey` + `OurAirportsID`. Khi join các bảng liên quan, cần dùng đầy đủ các cột khóa theo DDL, không tự ý chỉ join một phần khóa ghép.

DDL chính thức được lưu tại:

`04_DATABASE/01_DDL/flight_dw_2025_schema.sql`

Tổng số constraint khóa ngoại đã kiểm tra trong DDL: 15; tổng số column mappings: 18; trong đó có 3 quan hệ khóa ghép.

## 5. Quy tắc sử dụng dữ liệu quan trọng

1. **Timezone:** Thời gian BTS được ghi theo giờ địa phương của sân bay; dữ liệu thời tiết Open-Meteo được chuẩn hóa theo UTC. Không join trực tiếp hai timestamp nếu chưa chuyển đổi timezone.
2. **Ghép thời tiết:** Exposure được ghép với giờ thời tiết bằng cách floor xuống đầu giờ UTC; không dùng nearest-hour. `MinutesAfterWeatherHour` biểu diễn số phút từ đầu giờ, trong khoảng 0–59.
3. **WeatherKey NULL:** Trong Bridge, `WeatherKey IS NULL` biểu thị không có bản ghi thời tiết phù hợp hoặc trường hợp chưa phân giải; không tự thay bằng 0.
4. **Độ trễ:** `DepDelay` và `ArrDelay` là signed measures; giá trị âm thể hiện chuyến bay sớm. Không loại bỏ outlier tự động nếu chưa có quy tắc nghiệp vụ.
5. **Trường có điều kiện:** Các trường như `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`, `CancellationCode` có thể NULL theo logic nghiệp vụ; NULL không đồng nghĩa dữ liệu lỗi.
6. **Mã giờ 24:00:** Giữ sentinel 2400 và TimeKey 1441 theo thiết kế; Unknown time dùng key 0.
7. **Thời tiết:** `weather_code` là biến phân loại; hướng gió là biến góc tuần hoàn. Không tính trung bình số học trực tiếp cho hai trường này.
8. **SCD:** `dim_airline` và `dim_airport` đang áp dụng cách cập nhật kiểu Type 1; schema hiện tại không có EffectiveFrom / EffectiveTo / IsCurrent của SCD Type 2.
9. **Bridge:** Không chạy nhiều loader đồng thời. Không sửa/xóa checkpoint hoặc journal trong khi ETL còn hoạt động.

## 6. Kết nối bằng MySQL Workbench

1. Mở MySQL Workbench và tạo hoặc chọn connection tới `127.0.0.1`, port `3306`.
2. Chọn phương thức xác thực MySQL và nhập tài khoản/mật khẩu được cấp.
3. Mở schema `flight_dw_2025`.
4. Dùng DDL và Data Dictionary đi kèm để xem kiểu dữ liệu, khóa và ý nghĩa từng cột.

Không gửi file cấu hình có mật khẩu trong gói bàn giao.

## 7. Kết nối Power BI

1. Cài MySQL Connector/NET phù hợp với Power BI Desktop.
2. Chọn **Get Data → Database → MySQL database**.
3. Server: `127.0.0.1:3306`; Database: `flight_dw_2025`.
4. Chọn **Import** và xác thực bằng tài khoản MySQL.
5. Với dashboard tổng quan, ưu tiên `fact_flight` cùng các Dimension cần thiết.
6. Không import toàn bộ `bridge_flight_weather_exposure` (dự kiến khoảng 28 triệu dòng) vào PBIX. Tạo SQL view hoặc Gold aggregate theo mục tiêu phân tích trước khi đưa dữ liệu thời tiết vào Power BI.

Khi chuyển PBIX sang máy khác, người nhận có thể xem dữ liệu đã Import; để Refresh cần cài connector, có quyền truy cập MySQL và cấu hình lại nguồn/xác thực.

## 8. Kiểm tra nhanh schema

```sql
USE flight_dw_2025;

SELECT
    TABLE_NAME,
    TABLE_ROWS,
    ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'flight_dw_2025'
ORDER BY TABLE_NAME;

SELECT
    TABLE_NAME,
    COUNT(*) AS ColumnCount
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'flight_dw_2025'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;
```

`TABLE_ROWS` của InnoDB trong `information_schema.TABLES` có thể là số ước lượng. Dùng validation query chính thức hoặc `COUNT(*)` khi cần số chính xác và khi việc quét bảng không ảnh hưởng ETL đang chạy.

## 9. Backup và Restore

Chỉ tạo bản backup bàn giao cuối cùng sau khi Bridge ETL hoàn tất và STEP 30E4 / các kiểm định cuối cùng đạt PASS. Không backup bằng cách sao chép thư mục dữ liệu vật lý của MySQL.

Ví dụ lệnh backup (chạy trong PowerShell; nhập mật khẩu tại prompt, không ghi mật khẩu vào lệnh):

```powershell
$mysqldump = "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe"
$out = "D:\DATA_WAREHOUSE\09_FINAL_HANDOFF\04_DATABASE\03_DATABASE_BACKUP\flight_dw_2025_full.sql"
$dbUser = Read-Host "MySQL username"

& $mysqldump --host=127.0.0.1 --port=3306 --user=$dbUser --password --single-transaction --routines --triggers --events --databases flight_dw_2025 --result-file=$out
```

Restore trên một MySQL instance đã cài đặt bằng **Command Prompt (cmd.exe)** vì ký hiệu `<` dưới đây là input redirection của CMD:

```cmd
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" --host=127.0.0.1 --port=3306 --user=DB_USER --password < "D:\path\to\flight_dw_2025_full.sql"
```

Thay `DB_USER` bằng tài khoản được cấp. Nhập mật khẩu khi được yêu cầu; không đặt mật khẩu trực tiếp trong câu lệnh. Kiểm tra backup có kích thước hợp lý, không rỗng; sau restore cần chạy lại validation queries.

## 10. Tệp bàn giao liên quan

- `01_DDL/flight_dw_2025_schema.sql`: DDL tạo schema và ràng buộc.
- `02_SQL_VIEWS/`: các SQL view phục vụ truy vấn/Power BI (bổ sung sau khi chốt Gold layer).
- `03_DATABASE_BACKUP/`: full backup sau khi ETL và validation hoàn tất.
- `04_VALIDATION/`: SQL kiểm tra và kết quả validation.
- `05_DATABASE_GUIDE/`: tài liệu hướng dẫn này và hướng dẫn vận hành bổ sung.
- `05_DATASETS/02_DATA_DICTIONARY/DW_Data_Dictionary.xlsx`: từ điển dữ liệu 8 bảng và 154 cột.

---

**Trạng thái tài liệu:** Draft bàn giao. Số dòng Bridge và trạng thái toàn bộ pipeline phải được cập nhật từ log/checkpoint và kết quả STEP 30E4 thực tế trước khi phát hành bản cuối.
