### create docker network
docker network create default_network

### airflow
docker-compose up -d --build

### spark
docker-compose up -d

### kafka
docker-compose up -d

### postgresql
docker run --name postgres-ds9 --network=default_network --hostname postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres

### mysql
docker run --name mysql-ds9 --network=default_network --hostname mysql -e MYSQL_ROOT_PASSWORD=Ayamgoreng23 -p 3307:3306 -d mysql

### masuk container
docker exec -it nama_container bash

spark-submit --jars /usr/local/spark/resources/mysql-connector-j-8.0.31.jar --name example_job /usr/local/spark/app/csv_to_mysql.py

spark-submit --jars /usr/local/spark/resources/mysql-connector-j-8.0.31.jar,/usr/local/spark/resources/postgresql-42.5.1.jar --name example_job /usr/local/spark/app/mysql_to_postgres.py

cd/