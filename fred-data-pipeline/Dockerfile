# base image -- Astro Runtime 3.2 which maps to Airflow 3.x
# this is a pre-built Docker image by Astronomer that includes Airflow and all its dependencies pre-installed
FROM astrocrpublic.azurecr.io/runtime:3.2-1

# mount the entire project into the container at this path
# this gives Airflow access to src/, dbt_project/, mapping/ etc.
COPY . /usr/local/airflow/project

# install project Python dependencies inside the container
# copied to /tmp first as a best practice -- keeps it separate from project files
COPY requirements.txt /tmp/project_requirements.txt
RUN pip install --no-cache-dir -r /tmp/project_requirements.txt