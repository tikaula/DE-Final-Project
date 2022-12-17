from pyspark.sql import SparkSession

# Initiate spark
if __name__=="__main__":
    spark = SparkSession.builder.appName('FinalProject') \
    .config('spark.jars','/usr/local/spark/resources/mysql-connector-j-8.0.31','/usr/local/spark/resources/postgres-42.5.1').getOrCreate()

#read data from mysql
    application_test= spark.read.format('jdbc').option(
        url='jdbc:mysql://host.docker.internal:3307/final_project',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='home_credit_default_risk_application_test',
        user='root',
        password='Ayamgoreng23').mode('overwrite').save()

    application_train=spark.read.format('jdbc').option(
        url='jdbc:mysql://host.docker.internal:3307/final_project',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='home_credit_default_risk_application_train',
        user='root',
        password='Ayamgoreng23').mode('overwrite').save()

#load to postgresql
    application_test.write.format('jdbc').option(
        url='jdbc:postgresql://host.docker.internal:5432/final_project',
        driver='org.postgresql.Driver',
        dbtable='home_credit_default_risk_application_test',
        user='postgres',
        password='password').mode('overwrite').save()

    application_train.write.format('jdbc').option(
        url='jdbc:postgresql://host.docker.internal:5432/final_project',
        driver='org.postgresql.Driver',
        dbtable='home_credit_default_risk_application_train',
        user='postgres',
        password='password').mode('overwrite').save()
