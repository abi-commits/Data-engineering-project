FROM python:3.8-slim

# Add the PostgreSQL repository and install the client tools
RUN apt-get update && \
    apt-get install -y wget gnupg2 && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y postgresql-client-16 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY elt_script.py .

CMD ["python", "elt_script.py"]

