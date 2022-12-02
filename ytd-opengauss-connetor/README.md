```CREATE TABLE mysql_binlog (
    id INT NOT NULL,
    name STRING,
    description STRING,
    PRIMARY KEY (id) NOT ENFORCED +
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'flinkcdc',
    'password' = '123456',
    'database-name' = 'test',
    'table-name' = 'test_cdc'
);`

`CREATE TABLE test_cdc_sink (
     id INT NOT NULL,
     name STRING,
     description STRING,
     PRIMARY KEY (id) NOT ENFORCED 
    ) WITH (
     'connector' = 'opengauss',
//  'driver' = 'com.gbasedbt.jdbc.Driver',
    'url' = 'jdbc:opengauss://172.27.71.161:26000/postgres',
    'username' = 'jacky',
    'password' = '123456',
    'table-name' = ' t1 '
);`

    insert into test_cdc_sink select * from mysql_binlog;