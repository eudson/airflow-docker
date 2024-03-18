FROM apache/airflow:2.8.0

COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
RUN pip freeze
# Set the platform to linux/arm64 for M1 compatibility
CMD ["--platform", "linux/arm64"]