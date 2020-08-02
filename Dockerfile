FROM python:3.8.3-buster

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

WORKDIR /app
CMD ["python", "collector.py"]
