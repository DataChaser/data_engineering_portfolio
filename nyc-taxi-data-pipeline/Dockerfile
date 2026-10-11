FROM astrocrpublic.azurecr.io/runtime:3.2-1

# Copy entire project into container so DAG tasks can import from src/
COPY . /usr/local/airflow/project

# Install project dependencies inside the container
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt