FROM quay.io/astronomer/astro-runtime:12.6.0

# Create Python virtual environment for dbt
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && deactivate

# Install ODBC driver for SQL Server
USER root
RUN apt-get update && \
    apt-get install -y gnupg2 curl unixodbc unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install pyodbc in the main environment
RUN pip install --no-cache-dir pyodbc

USER astro