FROM apache/spark:3.5.3-python3
USER root
# delta's Python API also ships inside the delta-spark jar; --no-deps avoids a second copy of pyspark
RUN pip install --no-cache-dir --no-deps delta-spark==3.2.1 && pip install --no-cache-dir psycopg2-binary==2.9.10
COPY logic.py stream_job.py /opt/skystream/
WORKDIR /opt/skystream
CMD ["/opt/spark/bin/spark-submit", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.1", \
     "--driver-memory", "1g", \
     "/opt/skystream/stream_job.py"]
