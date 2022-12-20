FROM python:3.10-slim

WORKDIR /app
RUN pip install boto3 Pillow requests sanic ydb
COPY ./script.py .
CMD [ "python", "script.py" ]
