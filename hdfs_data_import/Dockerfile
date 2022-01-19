#FROM bash:4.4
FROM python:3.7-slim

COPY . /
RUN pip install --no-cache-dir -r requirements.txt

#CMD ["bash", "/import_csv.sh"]
CMD ["python", "-u", "/import_csv.py"]
