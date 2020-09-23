FROM python:3.8
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apt-get update
RUN apt-get install vim -y
RUN pip3 install -r requirements.txt
COPY . .
CMD ["python","parking_realtime_collectdata.py"]