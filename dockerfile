FROM apache/airflow:2.10.3

# copy requirements.txt file to container

COPY requirements.txt /requirements.txt


# upgrade pip
RUN pip install --upgrade pip

# Install Libraries
RUN pip install --no-cache-dir -r /requirements.txt