$ docker run --name bo-mysql -p 3306:3306 -v C:/mysql-data:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=Abc12345 -d mysql:8.0.42

CREATE DATABASE bo_src CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE bo_dst CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE test_table_src ( id INT AUTO_INCREMENT PRIMARY KEY, col_int INT, col_bigint BIGINT, col_decimal DECIMAL(10, 2), col_float FLOAT, col_double DOUBLE, col_date DATE, col_datetime DATETIME, col_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, col_time TIME, col_char CHAR(10), col_varchar VARCHAR(255), col_text TEXT, col_blob BLOB, col_boolean BOOLEAN, col_enum ENUM('small', 'medium', 'large'), col_json JSON );

CREATE TABLE test_table_dst ( id INT AUTO_INCREMENT PRIMARY KEY, col_int INT, col_bigint BIGINT, col_decimal DECIMAL(10, 2), col_float FLOAT, col_double DOUBLE, col_date DATE, col_datetime DATETIME, col_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, col_time TIME, col_char CHAR(10), col_varchar VARCHAR(255), col_text TEXT, col_blob BLOB, col_boolean BOOLEAN, col_enum ENUM('small', 'medium', 'large'), col_json JSON );

-- Create database if not exists CREATE DATABASE IF NOT EXISTS test_database; USE test_database;

-- Create table with various data types CREATE TABLE test_data ( id INT AUTO_INCREMENT PRIMARY KEY, tiny_int_col TINYINT, small_int_col SMALLINT, medium_int_col MEDIUMINT, int_col INT, big_int_col BIGINT, float_col FLOAT(10, 2), double_col DOUBLE(15, 5), decimal_col DECIMAL(20, 10), date_col DATE, time_col TIME, datetime_col DATETIME, timestamp_col TIMESTAMP, year_col YEAR, char_col CHAR(10), varchar_col VARCHAR(100), binary_col BINARY(20), varbinary_col VARBINARY(100), tinyblob_col TINYBLOB, blob_col BLOB, mediumblob_col MEDIUMBLOB, longblob_col LONGBLOB, tinytext_col TINYTEXT, text_col TEXT, mediumtext_col MEDIUMTEXT, longtext_col LONGTEXT, enum_col ENUM('small', 'medium', 'large', 'x-large'), set_col SET('red', 'green', 'blue', 'yellow'), json_col JSON, bool_col BOOLEAN, bit_col BIT(8), geometry_col GEOMETRY, point_col POINT, linestring_col LINESTRING, polygon_col POLYGON, multipoint_col MULTIPOINT, multilinestring_col MULTILINESTRING, multipolygon_col MULTIPOLYGON, geometrycollection_col GEOMETRYCOLLECTION, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ) ENGINE=InnoDB;

Tao data
INSERT INTO test_data ( tiny_int_col, small_int_col, medium_int_col, int_col, big_int_col, float_col, double_col, decimal_col, date_col, time_col, datetime_col, timestamp_col, year_col, char_col, varchar_col, binary_col, varbinary_col, tinytext_col, text_col, mediumtext_col, longtext_col, enum_col, set_col, bool_col, bit_col ) SELECT FLOOR(RAND() * 127) - 64, FLOOR(RAND() * 32767) - 16384, FLOOR(RAND() * 8388607) - 4194304, FLOOR(RAND() * 2147483647) - 1073741824, FLOOR(RAND() * 9223372036854775807) - 4611686018427387904, RAND() * 10000, RAND() * 100000, RAND() * 1000000, DATE_ADD('2000-01-01', INTERVAL FLOOR(RAND() * 8000) DAY), SEC_TO_TIME(FLOOR(RAND() * 86400)), DATE_ADD('2000-01-01 00:00:00', INTERVAL FLOOR(RAND() * 800086400) SECOND), DATE_ADD('2000-01-01 00:00:00', INTERVAL FLOOR(RAND() * 800086400) SECOND), 2000 + FLOOR(RAND() * 30), CONCAT('C', FLOOR(RAND() * 1000)), CONCAT('Variable text ', FLOOR(RAND() * 1000000)), UNHEX(MD5(RAND())), UNHEX(MD5(RAND())), CONCAT('Tiny text ', FLOOR(RAND() * 1000)), CONCAT('Regular text ', FLOOR(RAND() * 1000000)), CONCAT(REPEAT('Medium text ', 100), FLOOR(RAND() * 1000000)), CONCAT(REPEAT('Long text ', 1000), FLOOR(RAND() * 1000000)), ELT(1 + FLOOR(RAND() * 4), 'small', 'medium', 'large', 'x-large'), CASE WHEN RAND() < 0.25 THEN 'red' WHEN RAND() < 0.5 THEN 'green,blue' WHEN RAND() < 0.75 THEN 'yellow,red,blue' ELSE 'red,green,blue,yellow' END, RAND() > 0.5, FLOOR(RAND() * 256) FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t5, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t6 LIMIT 100000;



## increase placeholder 

show variables like 'max_allowed_packet';

nano /etc/my.cnf

[mysqld]
max_allowed_packet=512MB

sudo systemctl restart mysqld