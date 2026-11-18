FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py /app/
COPY models/ /app/models/

CMD ["python", "-u", "generator.py"]