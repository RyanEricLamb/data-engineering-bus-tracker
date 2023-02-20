FROM  prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --no-cache-dir

#Conflict in gts-realtime-bindings protobuf version
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
