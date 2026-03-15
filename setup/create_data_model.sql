-- ============================================================
-- Mô hình dữ liệu xe buýt TP.HCM
-- Star Schema: Fact (bus_way_points) + Dimensions + Bridge
-- ============================================================

USE catalog_iceberg;
CREATE DATABASE IF NOT EXISTS schema_iceberg;
USE schema_iceberg;

-- ============================================================
-- DIMENSION: vehicle_route_mapping
-- Ánh xạ xe → tuyến đường
-- Quan hệ: bus_way_points.vehicle → vehicle_route_mapping.vehicle (N:1)
-- ============================================================
CREATE TABLE IF NOT EXISTS vehicle_route_mapping (
    vehicle       VARCHAR NOT NULL,  -- SHA-256 hash định danh xe
    route_id      INT     NOT NULL,  -- ID tuyến (nội bộ hệ thống)
    route_no      VARCHAR NOT NULL   -- Số tuyến hiển thị (vd: 88, 30, 163V)
);

-- ============================================================
-- DIMENSION: bus_stops
-- Danh sách trạm dừng xe buýt TP.HCM
-- Mỗi trạm phục vụ nhiều tuyến (cột routes chứa danh sách csv)
-- ============================================================
CREATE TABLE IF NOT EXISTS bus_stops (
    stop_id              INT     NOT NULL,  -- ID trạm dừng
    code                 VARCHAR,           -- Mã trạm (vd: Q1 031, BX 06)
    stop_name            VARCHAR,           -- Tên trạm
    type                 VARCHAR,           -- Loại: Nhà chờ, Trụ dừng, Bến xe, Ô sơn, Biển treo
    zone                 VARCHAR,           -- Quận/Huyện (vd: Quận 1, Quận 5)
    ward                 VARCHAR,           -- Phường/Xã
    address              VARCHAR,           -- Địa chỉ số nhà
    street               VARCHAR,           -- Tên đường
    support_disability   VARCHAR,           -- Hỗ trợ người khuyết tật (Có/NULL)
    status               VARCHAR,           -- Trạng thái khai thác
    lng                  DOUBLE  NOT NULL,  -- Kinh độ (longitude)
    lat                  DOUBLE  NOT NULL,  -- Vĩ độ (latitude)
    search               VARCHAR,           -- Chuỗi tìm kiếm viết tắt
    routes               VARCHAR            -- Danh sách tuyến phục vụ (csv, vd: "01, 03, 19, 45")
);

-- ============================================================
-- BRIDGE TABLE: route_stops
-- Liên kết N-N giữa tuyến và trạm dừng
-- Được tạo bằng cách explode cột routes từ bus_stops
-- Quan hệ: vehicle_route_mapping.route_no → route_stops.route_no (1:N)
--           bus_stops.stop_id → route_stops.stop_id (1:N)
-- ============================================================
CREATE TABLE IF NOT EXISTS route_stops (
    route_no    VARCHAR NOT NULL,  -- Số tuyến (join key với vehicle_route_mapping)
    stop_id     INT     NOT NULL   -- ID trạm (join key với bus_stops)
);

-- ============================================================
-- FACT TABLE: bus_way_points
-- Dữ liệu GPS xe buýt theo thời gian thực
-- Mỗi bản ghi = 1 vị trí GPS của 1 xe tại 1 thời điểm
-- Quan hệ: bus_way_points.vehicle → vehicle_route_mapping.vehicle (N:1)
-- ============================================================
CREATE TABLE IF NOT EXISTS bus_way_points (
    msg_type    VARCHAR,            -- Loại message (MsgType_BusWayPoint)
    vehicle     VARCHAR NOT NULL,   -- SHA-256 hash định danh xe (FK → vehicle_route_mapping)
    driver      VARCHAR,            -- SHA-256 hash định danh tài xế
    speed       FLOAT,              -- Tốc độ (km/h)
    datetime    INT     NOT NULL,   -- Unix epoch timestamp (giây)
    x           DOUBLE,             -- Kinh độ (longitude)
    y           DOUBLE,             -- Vĩ độ (latitude)
    z           FLOAT,              -- Cao độ
    heading     FLOAT,              -- Hướng di chuyển (độ)
    ignition    BOOLEAN,            -- Trạng thái khởi động
    aircon      BOOLEAN,            -- Điều hòa bật/tắt
    door_up     BOOLEAN,            -- Cửa trên mở/đóng
    door_down   BOOLEAN,            -- Cửa dưới mở/đóng
    sos         BOOLEAN,            -- Tín hiệu SOS
    working     BOOLEAN,            -- Xe đang hoạt động
    analog1     FLOAT,              -- Tín hiệu analog 1
    analog2     FLOAT,              -- Tín hiệu analog 2
    timestamp   TIMESTAMP,          -- Derived: from_unixtime(datetime)
    date        DATE,               -- Derived: partition key
    load_at     TIMESTAMP           -- Thời điểm nạp dữ liệu
)
PARTITIONED BY (date);

-- ============================================================
-- QUAN HỆ GIỮA CÁC BẢNG (mô tả logic, không enforce FK trong Iceberg)
--
-- ┌─────────────────────┐       ┌──────────────────────────┐
-- │   bus_way_points    │       │  vehicle_route_mapping   │
-- │   (FACT TABLE)      │       │  (DIMENSION)             │
-- │                     │  N:1  │                          │
-- │  vehicle ───────────┼──────▶│  vehicle                 │
-- │  x, y (GPS coords)  │       │  route_id                │
-- │  speed, datetime    │       │  route_no ──────┐        │
-- │  ignition, aircon...│       └─────────────────┼────────┘
-- └─────────────────────┘                         │
--                                                 │ 1:N
--                                    ┌────────────▼────────┐
--                                    │    route_stops      │
--                                    │    (BRIDGE TABLE)   │
--                                    │                     │
--                                    │  route_no           │
--                                    │  stop_id ──────┐    │
--                                    └────────────────┼────┘
--                                                     │ N:1
--                                       ┌─────────────▼────────┐
--                                       │     bus_stops        │
--                                       │     (DIMENSION)      │
--                                       │                      │
--                                       │  stop_id             │
--                                       │  stop_name, zone     │
--                                       │  lng, lat (coords)   │
--                                       │  routes (csv list)   │
--                                       └──────────────────────┘
--
-- JOIN PATHS:
--   1. GPS → Tuyến:  bus_way_points JOIN vehicle_route_mapping ON vehicle
--   2. Tuyến → Trạm: vehicle_route_mapping JOIN route_stops ON route_no
--                     route_stops JOIN bus_stops ON stop_id
--   3. GPS → Trạm:   Spatial join (Haversine distance) trên cùng tuyến
-- ============================================================

-- ============================================================
-- POPULATE BRIDGE TABLE từ bus_stops.routes
-- ============================================================
INSERT INTO route_stops (route_no, stop_id)
SELECT DISTINCT
    TRIM(route_no_exploded) AS route_no,
    stop_id
FROM bus_stops
CROSS JOIN UNNEST(SPLIT(routes, ',')) AS t(route_no_exploded)
WHERE TRIM(route_no_exploded) != '';

-- ============================================================
-- SAMPLE QUERIES
-- ============================================================

-- 1. Xem waypoints kèm thông tin tuyến
-- SELECT wp.vehicle, wp.x, wp.y, wp.speed, wp.timestamp,
--        vr.route_id, vr.route_no
-- FROM bus_way_points wp
-- JOIN vehicle_route_mapping vr ON wp.vehicle = vr.vehicle;

-- 2. Đếm số trạm theo tuyến (cho các tuyến có xe hoạt động)
-- SELECT rs.route_no, COUNT(DISTINCT rs.stop_id) AS num_stops
-- FROM route_stops rs
-- JOIN vehicle_route_mapping vr ON rs.route_no = vr.route_no
-- GROUP BY rs.route_no
-- ORDER BY num_stops DESC;

-- 3. Thống kê tốc độ theo tuyến
-- SELECT vr.route_no,
--        COUNT(*)           AS num_records,
--        AVG(wp.speed)      AS avg_speed,
--        MAX(wp.speed)      AS max_speed,
--        STDDEV(wp.speed)   AS std_speed
-- FROM bus_way_points wp
-- JOIN vehicle_route_mapping vr ON wp.vehicle = vr.vehicle
-- GROUP BY vr.route_no
-- ORDER BY num_records DESC;

-- 4. Tìm trạm trên cùng tuyến với xe (để spatial join)
-- SELECT wp.vehicle, wp.x, wp.y, wp.timestamp,
--        bs.stop_name, bs.lng, bs.lat,
--        vr.route_no
-- FROM bus_way_points wp
-- JOIN vehicle_route_mapping vr ON wp.vehicle = vr.vehicle
-- JOIN route_stops rs ON vr.route_no = rs.route_no
-- JOIN bus_stops bs ON rs.stop_id = bs.stop_id;
