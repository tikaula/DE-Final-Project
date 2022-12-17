from pyspark.sql import SparkSession

# Initiate spark
if __name__=="__main__":
    spark = SparkSession.builder.appName('FinalProject') \
    .config('spark.jars','/usr/local/spark/resources/mysql-connector-j-8.0.31') \
    .getOrCreate()

#read data
    application_test = spark.read.csv('/usr/local/spark/resources/application_test.csv', \
        inferSchema=True, header=True)

    application_train = spark.read.csv('/usr/local/spark/resources/application_train.csv', \
        inferSchema=True, header=True)

#load to mysql
    application_test.write.format('jdbc').option(
        url='jdbc:mysql://host.docker.internal:3306/final_project',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='home_credit_default_risk_application_test',
        user='root',
        password='Ayamgoreng23').mode('overwrite').save()

    application_train.write.format('jdbc').option(
        url='jdbc:mysql://host.docker.internal:3306/final_project',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='home_credit_default_risk_application_train',
        user='root',
        password='Ayamgoreng23').mode('overwrite').save()
