FROM python:3.8

RUN apt-get update && apt-get install -y wget gnupg2 && \
    wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && apt-get install -y postgresql-client-17

COPY elt_script.py .
CMD ["python", "elt_script.py"]
