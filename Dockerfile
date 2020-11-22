FROM openkbs/jdk-mvn-py3-x11
ENV GOOGLE_APPLICATION_CREDENTIALS "CloudComputing-91f724764b5e.json"
COPY . /usr/src/myapp
COPY CloudComputing-91f724764b5e.json CloudComputing-91f724764b5e.json
WORKDIR /usr/src/myapp
CMD java -jar SearchEngine.jar 

