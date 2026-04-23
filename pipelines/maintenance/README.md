# Iceberg Maintenance And Optimization

Tài liệu này mô tả maintenance hiện tại cho bảng `catalog_iceberg.bus_silver.bus_way_point`
và các hướng tối ưu phù hợp với workload hiện có trong project.

## Maintenance hiện tại

File thực thi: [iceberg_maintenance.py](/D:/Projects/mp-252/pipelines/maintenance/iceberg_maintenance.py)

Bảng đang được bảo trì:

```text
catalog_iceberg.bus_silver.bus_way_point
```

Script hiện tại làm các bước sau theo đúng thứ tự:

1. `rewrite_data_files`
   Gộp các file dữ liệu nhỏ thành file lớn hơn.
   Cấu hình hiện tại:
   - `min-file-size-bytes = 3MB`
   - `target-file-size-bytes = 64MB`
   - `max-file-size-bytes = 128MB`

2. `rewrite_manifests`
   Gom và sắp xếp lại manifest metadata để Spark planning query nhanh hơn.

3. `expire_snapshots`
   Xóa snapshot cũ, hiện giữ lại `3` snapshot gần nhất.

4. `remove_orphan_files`
   Xóa file không còn được snapshot nào tham chiếu, với ngưỡng cũ hơn `10` ngày.

5. Benchmark trước và sau maintenance
   Script chạy các query benchmark trước và sau bảo trì để so sánh thời gian thực thi.

## Vì sao thứ tự này hợp lý

- `rewrite_data_files` nên chạy đầu tiên vì small files thường là nguyên nhân lớn nhất làm scan chậm.
- `rewrite_manifests` nên chạy sau khi data layout đã thay đổi để metadata phản ánh layout mới.
- `expire_snapshots` nên chạy sau các bước rewrite để dọn bớt metadata cũ.
- `remove_orphan_files` nên chạy cuối để tránh xóa file vẫn còn có thể được tham chiếu bởi snapshot chưa expire.

## Trạng thái bảng hiện tại trong pipeline

File tạo bảng silver: [silver_buswaypoint.py](/D:/Projects/mp-252/pipelines/silver/silver_buswaypoint.py)

Thiết kế hiện tại:
- Bảng `bus_way_point` được `PARTITIONED BY (date)`
- Có các cột quan trọng cho truy vấn: `route_id`, `route_no`, `vehicle`, `timestamp`, `date`, `hour`

Điều này cho thấy maintenance hiện tại là đúng hướng vì:
- Query theo ngày sẽ được hưởng lợi từ partition pruning
- Query theo route hoặc timestamp vẫn còn phụ thuộc khá nhiều vào data layout trong từng partition

## Các hướng tối ưu tiếp theo nên làm

### 1. Tối ưu benchmark theo workload thật

Hiện benchmark đang dùng 3 query tổng quát:
- `COUNT(*)`
- tổng hợp 7 ngày gần nhất
- tổng hợp `route_id, date`

Nên bổ sung benchmark sát nghiệp vụ hơn, ví dụ:

```sql
SELECT route_id, hour, COUNT(*) AS row_count
FROM catalog_iceberg.bus_silver.bus_way_point
WHERE date = current_date()
GROUP BY route_id, hour
ORDER BY route_id, hour
```

Query này sát hơn với dashboard theo ngày/khung giờ.

Ví dụ khác:

```sql
SELECT vehicle, MAX(timestamp) AS latest_seen
FROM catalog_iceberg.bus_silver.bus_way_point
WHERE date >= date_sub(current_date(), 3)
GROUP BY vehicle
```

Query này hợp với use case theo dõi trạng thái xe gần thời gian hiện tại.

### 2. Tối ưu write path để giảm small files từ đầu

Maintenance giúp sửa hậu quả, nhưng tối ưu write path mới là cách hiệu quả lâu dài.

Với `bus_way_point`, có thể cân nhắc:
- `repartition("date")` hoặc `repartition("date", "route_id")` trước khi ghi nếu ingest đang quá phân mảnh
- giảm số lượng micro-batch hoặc commit quá nhỏ
- kiểm soát số file ghi ra theo mỗi lần append

Ví dụ ý tưởng:

```python
df_clean.repartition("date").writeTo("catalog_iceberg.bus_silver.bus_way_point").append()
```

Không phải lúc nào cũng nên dùng ngay, nhưng đây là hướng nên thử nếu mỗi partition ngày đang sinh quá nhiều file nhỏ.

### 3. Tối ưu partition spec nếu workload đổi

Hiện tại bảng partition theo `date`, đây là lựa chọn hợp lý cho analytics theo ngày.

Khi nào cần xem lại:
- nếu phần lớn query lọc theo `hour` trong ngày
- nếu query tập trung mạnh vào một nhóm `route_id`
- nếu mỗi ngày dữ liệu quá lớn và một partition ngày vẫn còn quá rộng

Ví dụ hướng thay đổi:
- giữ `date` là partition chính
- kết hợp tối ưu write/sort theo `route_id`, `timestamp`

Thường không nên partition trực tiếp theo `route_id` nếu cardinality quá cao vì dễ tạo quá nhiều partition nhỏ.

### 4. Tối ưu sort/data clustering trong từng partition

Vì bảng hiện chỉ partition theo `date`, query bên trong một ngày vẫn có thể phải scan khá nhiều file.

Một hướng tốt là giữ dữ liệu trong partition được “gần nhau” theo:
- `route_id`
- `timestamp`
- hoặc `vehicle, timestamp`

Ví dụ workload:
- truy vấn lịch sử một tuyến xe theo ngày
- truy vấn timeline của một xe

Khi đó việc rewrite hoặc write theo sort order gần với các cột trên thường giúp giảm scan thực tế.

### 5. Điều chỉnh target file size theo workload

Hiện tại `target-file-size-bytes = 64MB`.

Gợi ý:
- giữ `64MB` nếu workload có nhiều query vừa và nhỏ
- thử `128MB` nếu workload thiên về scan lớn, batch analytics, aggregate nặng

Ví dụ:
- dashboard nhẹ, nhiều query nhỏ: `64MB` thường ổn
- nightly analytics hoặc query gộp dài ngày: `128MB` có thể tốt hơn

### 6. Điều chỉnh snapshot retention theo nhu cầu vận hành

Hiện tại script giữ `3` snapshot gần nhất.

Gợi ý:
- dev/test: `3` là hợp lý
- production cần rollback hoặc audit: tăng lên `5-10`
- nếu cần time travel theo ngày, nên chuyển retention sang policy theo thời gian thay vì chỉ theo số lượng

## Ví dụ kế hoạch tối ưu thực tế cho project này

### Giai đoạn 1

- Giữ nguyên partition theo `date`
- Chạy maintenance định kỳ
- Dùng benchmark trước/sau để xác nhận hiệu quả

### Giai đoạn 2

- Đo số lượng file theo từng `date`
- Nếu thấy quá nhiều small files, tối ưu write path ở pipeline silver
- Bổ sung benchmark theo `route_id`, `vehicle`, `hour`

### Giai đoạn 3

- Thử tăng `target-file-size-bytes` từ `64MB` lên `128MB`
- So sánh benchmark với cùng một tập query
- Chỉ giữ thay đổi nếu kết quả tốt hơn ổn định

## Kỳ vọng cải thiện hiệu năng

Các bước maintenance thường cải thiện ở 2 phần:
- thời gian planning query
- thời gian scan dữ liệu

Ví dụ thường gặp:
- trước maintenance: nhiều nghìn file nhỏ trong partition ngày
- sau `rewrite_data_files`: số file giảm mạnh
- sau `rewrite_manifests`: Spark lập kế hoạch nhanh hơn

Kết quả dễ thấy nhất thường là các query:
- aggregate theo `date`
- aggregate theo `route_id, date`
- lọc dữ liệu vài ngày gần nhất

## Khuyến nghị ngắn gọn

- Tiếp tục giữ maintenance hiện tại vì hướng đi là đúng
- Ưu tiên tối ưu write path nếu benchmark cho thấy small files quay lại nhanh
- Đừng đổi partition spec quá sớm; hãy benchmark trước bằng query thật
- Dùng benchmark của dashboard/nghiệp vụ để đánh giá, không chỉ dựa vào `COUNT(*)`
