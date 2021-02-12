FROM bde2020/spark-base:3.0.1-hadoop3.2

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/run_pipeline.py

ONBUILD COPY requirements.txt /app/
ONBUILD RUN cd /app \
      && pip3 install -r requirements.txt

COPY submit.sh /

CMD ["/bin/bash", "/submit.sh"]
