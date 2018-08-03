FROM  python:3.6.5-slim-stretch

COPY . /app

RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /app

VOLUME ["/app"]

EXPOSE 5080

CMD ["gunicorn", "--reload", "-b", "0.0.0.0:5080", "api:app"]
