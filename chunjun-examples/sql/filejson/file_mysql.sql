CREATE TABLE source (
                        id bigint,
                        name varchar,
                        age varchar
) WITH (
      'connector' = 'file-json-x',
      'path' = 'C:\Users\52710\IdeaProjects\flinkx\data\data01.csv',
      'format' = 'csv'
      );

CREATE TABLE sink (
                      id bigint,
                      name varchar,
                      age varchar
) WITH (
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://cdhnode40:3306/test',
      'table-name' = 'flink_target',
      'username' = 'root',
      'password' = '123456'
      );

INSERT INTO sink
SELECT id,name,age
FROM source;
