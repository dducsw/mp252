"""
testbr Layer Table Definitions for
=======================================
Tạo các bảng Iceberg cho từng message chính trong.proto
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("testbrTableDefinitions") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.table-default.format-version", "2") \
    .config("spark.sql.defaultCatalog", "iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

namespace = "testbr"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

tables = [
    # WayPoint
    ("waypoint", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.waypoint (
            vehicle STRING,
            driver STRING,
            speed FLOAT,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            z FLOAT,
            heading FLOAT,
            ignition BOOLEAN,
            door BOOLEAN,
            aircon BOOLEAN,
            maxvalidspeed DOUBLE,
            vss FLOAT,
            location STRING,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # BusWayPoint
    ("buswaypoint", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.buswaypoint (
            vehicle STRING,
            driver STRING,
            speed FLOAT,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            z FLOAT,
            heading FLOAT,
            ignition BOOLEAN,
            aircon BOOLEAN,
            door_up BOOLEAN,
            door_down BOOLEAN,
            sos BOOLEAN,
            working BOOLEAN,
            analog1 FLOAT,
            analog2 FLOAT,
            load_at TIMESTAMP
        ) USING iceberg 
        PARTITIONED BY (day(datetime))
    """),
    # StudentCheckInPoint
    ("studentcheckin", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.studentcheckin (
            vehicle STRING,
            studentcode STRING,
            speed FLOAT,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            z FLOAT,
            heading FLOAT,
            working BOOLEAN,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # RegVehicle
    ("regvehicle", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.regvehicle (
            vehicle STRING,
            vehicleType INT,
            driver STRING,
            company STRING,
            deviceModelNo INT,
            deviceModel STRING,
            deviceId STRING,
            sim STRING,
            datetime TIMESTAMP,
            vin STRING,
            load_at TIMESTAMP
        ) USING iceberg
    """),
    # RegDriver
    ("regdriver", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.regdriver (
            driver STRING,
            name STRING,
            datetimeIssue TIMESTAMP,
            datetimeExpire TIMESTAMP,
            regPlace STRING,
            license STRING,
            load_at TIMESTAMP
        ) USING iceberg
    """),
    # RegCompany
    ("regcompany", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.regcompany (
            company STRING,
            name STRING,
            address STRING,
            tel STRING,
            load_at TIMESTAMP
        ) USING iceberg
    """),
    # BusTicketPoint
    ("busticketpoint", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.busticketpoint (
            vehicle STRING,
            routecode STRING,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            stationname STRING,
            tickettype INT,
            fare DOUBLE,
            series STRING,
            customercode STRING,
            universitycode STRING,
            companycode STRING,
            iscash BOOLEAN,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # BusTicketStartEndPoint
    ("busticketstartend", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.busticketstartend (
            vehicle STRING,
            routecode STRING,
            datetime TIMESTAMP,
            x DOUBLE,
            y DOUBLE,
            stationname STRING,
            tripno INT,
            pointtype INT,
            ticket_total INT,
            ticket_series STRING,
            ticket_type INT,
            load_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (day(datetime))
    """),
    # BusWayPointBatch
    ("buswaypointbatch", """
        CREATE TABLE IF NOT EXISTS iceberg.testbr.buswaypointbatch (
            vehicle STRING,
            driver STRING,
            load_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
]

for name, stmt in tables:
    print(f"- Creating table: {name}")
    spark.sql(stmt)
    print(f"* Table {name} created!")

print("\n# testbr layer tables created successfully!")

spark.stop()