FROM astrocrpublic.azurecr.io/runtime:3.2-1

# Copy entire project into the container
COPY . /opt/airflow/project

# Install project's dependencies
COPY requirements.txt /tmp/project_requirements.txt
RUN pip install --no-cache-dir -r /tmp/project_requirements.txt