FROM apache/airflow:2.1.0
# add your dependencies here:
# RUN pip install package1 package2....
ENV AIRFLOW_VERSION=2.1.0
RUN PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
RUN CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip3 install "apache-airflow[apache.spark]==${AIRFLOW_VERSION}"
