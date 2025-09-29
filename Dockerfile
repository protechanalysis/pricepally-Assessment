FROM apache/airflow:3.0.4

USER airflow

# Copy requirements.txt
COPY requirement.txt /requirement.txt

# Install dependencies as airflow user
RUN pip install --no-cache-dir -r /requirement.txt