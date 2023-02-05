FROM python:3.9
FROM python:3.9.1

# We need to install wget to download the csv file
RUN apt-get install wget
# psycopg2 is a postgres db adapter for python: sqlalchemy needs it
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest-zones.py ingest-zones.py 

ENTRYPOINT [ "python", "ingest-zones.py" ]
