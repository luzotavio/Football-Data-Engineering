FROM apache/airflow:2.7.2

RUN pip install --no-cache-dir \
    beautifulsoup4 \
    pandas \
    requests \
    lxml \
    apache-airflow-providers-microsoft-azure \
    geopy \
    adlfs