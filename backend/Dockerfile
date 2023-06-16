FROM python:3.8.9-slim-buster
RUN apt-get update  
RUN apt-get upgrade -y
#RUN apt-get install -y gcc gdal-bin python-gdal python3-gdal

RUN mkdir -p /app
COPY ./ /app
WORKDIR /app
RUN ls -a
RUN pip3 install -U pip
RUN pip3 install -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]