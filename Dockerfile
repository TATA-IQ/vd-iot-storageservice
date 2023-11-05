FROM python:3.9
RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6  -y
Run apt-get install redis -y
RUN pip install mysql-connector-python
RUN pip install pandas
RUN pip install opencv-python
RUN pip install requests
RUN pip install imutils
RUN pip install pyyaml
RUN pip install scipy
Run pip install numpy
Run pip install minio
Run pip install pymongo
Run pip install pillow
RUN pip install kafka-python==2.0.2
Run pip install redis==4.6.0
Run pip install shared-memory-dict==0.7.2
RUN pip install protobuf==3.20.*
copy storageservice/ /app
WORKDIR /app
CMD chmod +x run.sh
CMD ./run.sh
#CMD ["run.sh"]
#CMD ["/bin/bash","-c","python3 cache.py;python3 streaming.py"]