FROM apache/airflow:3.1.0
USER root
# you cannot run pip install --proxy since uv pip doesnt support it 
# RUN export HTTPS_PROXY='http://192.168.70.86:10802' 
RUN apt-get update 
# i need curl in order to install my custom installer.sh script under the resources/assets 
# if you do not need to inject proxy inside the script in order to get uv you can just install it online! (not in iran)
RUN apt-get install -y --no-install-recommends curl ca-certificates
RUN mkdir -p /codebase/resources/logs
COPY ./resources/assets/installer.sh installer.sh
RUN sh installer.sh && rm installer.sh
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /codebase
RUN uv venv --python 3.12
ENV PATH="/codebase/.venv/bin:$PATH"
ENV VIRTUAL_ENV="/codebase/.venv"
RUN unset HTTPS_PROXY

COPY requirements.txt /codebase/requirements.txt
COPY pyproject.toml pyproject.toml
COPY README.md README.md
RUN uv pip install -r requirements.txt
ADD infrastructure/airflow ./infrastructure/airflow
ADD lake ./lake
ADD resources ./resources
RUN uv pip install -e .
RUN chown airflow:root -R /codebase
ENV PATH="/codebase/.venv/bin/:$PATH"
# from this point on im initiating a regular airflow base image (this part must re-run everytime you change the source code in ./lake/*)
RUN export AIRFLOW_UID=$(id -u)

USER ${AIRFLOW_UID}

ENV AIRFLOW_UID=${AIRFLOW_UID}
ENV AIRFLOW_GID=0
ENV PYTHONPATH=/codebase

# Create directories for Airflow (they will be mapped to infrastructure/airflow/* under our local environment)
RUN mkdir -p /codebase/infrastructure/airflow/{logs,dags,plugins,config}

# Check system resources and prepare Airflow's environment (this script is running on airflow dockker compose base image)
RUN /bin/bash -c ' \
    one_meg=1048576 && \
    mem_available=$(( $(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg )) && \
    cpus_available=$(grep -cE "cpu[0-9]+" /proc/stat) && \
    disk_available=$(df / | tail -1 | awk "{print \$4}") && \
    warning_resources="false" && \
    if (( mem_available < 4000 )); then \
        echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m" && \
        echo "At least 4GB of memory required. You have $(numfmt --to iec $(( mem_available * one_meg )))" && \
        warning_resources="true"; \
    fi && \
    if (( cpus_available < 2 )); then \
        echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m" && \
        echo "At least 2 CPUs recommended. You have ${cpus_available}" && \
        warning_resources="true"; \
    fi && \
    if (( disk_available < one_meg * 10 )); then \
        echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m" && \
        echo "At least 10 GBs recommended. You have $(numfmt --to iec $(( disk_available * 1024 )))" && \
        warning_resources="true"; \
    fi && \
    if [[ "${warning_resources}" == "true" ]]; then \
        echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"; \
    fi && \
    echo "Airflow version:" && \
    /entrypoint airflow version && \
    echo "Running airflow config list to create default config file if missing." && \
    /entrypoint airflow config list >/dev/null && \
    echo "Change ownership of files in /codebase/infrastructure/airflow to ${AIRFLOW_UID}:0" && \
    chown -R "${AIRFLOW_UID}:0" /codebase/infrastructure/airflow \
'


CMD ["/bin/bash"]
