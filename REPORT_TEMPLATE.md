# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyen Duy Dung  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  run 1/3 … 69.3s
  run 2/3 … 50.6s
  run 3/3 … 52.3s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536,3×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng sau mỗi lần pipeline chạy lại; sau ba lượt đạt 38.750 hàng thay vì 12.480. |
| **Nguyên nhân** | Model incremental không có `unique_key`, nên dbt ghi kiểu append/`INSERT`; ticket đã có bị ghi thêm thay vì cập nhật. CDC có bản ghi `op='u'`, nên cùng ticket có thể được xử lý lại. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: khai báo `unique_key='ticket_id'`, `incremental_strategy='merge'`. `dags/ai_training_pipeline.py`: đặt `catchup=False`, `max_active_runs=1` để giảm chạy bù/chồng run. |
| **Bằng chứng** | trước: 38.750 hàng · sau: 12.480 hàng · checksum 3 lượt: `8dd7c98653` giống nhau · không còn ticket lặp. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu 455 cặp ngày/customer ở các ngày quá khứ. |
| **P99 độ trễ đo được** | **2,7258 ngày** *(bắt buộc)* |
| **Lookback đã chọn** | 3 ngày — làm tròn lên từ P99 để bao phủ gần như toàn bộ event về muộn. |
| **Nguyên nhân** | Điều kiện incremental chỉ lấy `event_date > max(event_date)` trong target. Khi event cũ đến muộn, ngày sự kiện đã nhỏ hơn watermark nên không bao giờ được aggregate. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: tính lại cửa sổ `event_date >= max(event_date) - interval 3 day`; dùng `merge` với khoá ghép `['event_date', 'customer_id']` để các aggregate được tính lại thay thế kết quả cũ. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng · checksum 3 lượt: `3db448685c` giống nhau. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

P99 phản ánh đuôi trễ đại diện hơn `max`: dùng `max` sẽ tăng cửa sổ vì một ngoại lệ hiếm và làm quét/tính lại nhiều ngày ở **mọi** lượt chạy. Lookback 3 ngày chấp nhận phần đuôi cực hiếm vượt P99, đồng thời kiểm soát chi phí tính lại liên tục.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Sau 08-10, priority có nhiều `NULL` và còn các giá trị ngoài contract như `0`, `5`, `-1`; model downstream nhận sai feature. |
| **Nguyên nhân** | Source chuyển biểu diễn từ số sang nhãn chữ. `try_cast` cũ biến nhãn hợp lệ thành `NULL` nhưng lại nhận số ngoài miền; contract và kiểm thử miền giá trị chưa được bật. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | `1..4`: giữ nguyên; `urgent/high/medium/low`: map lần lượt `1/2/3/4`; `P1`, `unknown`, `0`, `5`, `-1`, rỗng, `NULL`: trả `NULL` và đưa quarantine. |
| **Cách khắc phục** | `normalize_priority.sql`: `CASE` chuẩn hoá đủ ba nhóm. `silver_tickets.sql`: lọc record không chuẩn hoá được **trước** `row_number`. `quarantine_tickets.sql`: lấy các record macro trả `NULL`. `schema.yml`: bật contract và test `not_null`, `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass · priority sạch, thuộc 1..4. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

Nên giữ Bronze là dữ liệu thô để truy vết/audit, rồi chuẩn hoá và quarantine ở Silver theo contract phục vụ downstream. Không để cả DAG fail vì chỉ 312 record lỗi không nên chặn dữ liệu hợp lệ của các bảng khác; quarantine tạo hàng đợi để điều tra và sửa nguồn.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A — Query dashboard chậm; B — consumer gặp sự cố giữa batch |
| **Nguyên nhân** | **A:** 5.000 Parquet file nhỏ không partition khiến truy vấn mở toàn bộ file; điều kiện `strftime(event_time, ...)` không sargable nên không thể tận dụng partition/statistics. **B:** consumer commit offset trước khi ghi; chết sau commit làm batch chưa ghi bị bỏ qua khi restart (at-most-once). |
| **Cách khắc phục** | **A:** compact theo partition `event_date`, sắp xếp `customer_name, event_time`, row group 1.000; đọc bằng `hive_partitioning` và filter ngày theo range/date. **B:** ghi batch trước, commit offset sau; dùng `event_id` primary key và `ON CONFLICT DO UPDATE` để replay cập nhật nội dung mới, thay vì `DO NOTHING` bỏ qua nội dung đã thay đổi. |
| **Bằng chứng** | **A:** rows scanned 5.000.000 → 9.324 (**536,3×**) · file 5.000 → 14 · hash `4379e4c5d9f3` không đổi. **B:** baseline 20.000 event · crash lô 7, offset 3.000 · restart ghi 17.000 message · cuối cùng 20.000 hàng / 20.000 event_id; không mất, không trùng, C == A ✓. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain, natural key và SQL materialization của mọi incremental model trước khi chạy lại/backfill. |
| 2 | Đo phân bố độ trễ ingestion và chọn lookback có căn cứ định lượng; bảo đảm ghi lại là idempotent. |
| 3 | Tách raw data khỏi contract-serving data; nhận diện schema evolution khác với dữ liệu lỗi và theo dõi quarantine. |
