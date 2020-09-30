FROM python:3.7-slim

WORKDIR /app
COPY . /app

RUN apt-get update
RUN apt-get -y upgrade

# This is only for debugging in dev mode. Uncomment in production
# RUN apt-get install -y vim

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD python index_sharechat.py
# CMD tail -f /dev/null