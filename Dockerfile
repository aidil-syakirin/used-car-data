FROM tabulario/spark-iceberg

# Add necessary Hadoop Azure JARs
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.2/hadoop-azure-3.3.2.jar -P /opt/spark/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.2/hadoop-azure-datalake-3.3.2.jar -P /opt/spark/jars/
