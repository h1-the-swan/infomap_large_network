FROM ubuntu:20.04

RUN mkdir /RelaxMap

# ENV SPARK_SUBMIT_OPTS="--illegal-access=permit -Dio.netty.tryReflectionSetAccessible=true"
# ENV SPARK_SUBMIT_OPTS="-Dio.netty.tryReflectionSetAccessible=true"
# ENV SPARK_MASTER_OPTS="-Dio.netty.tryReflectionSetAccessible=true"
# ENV SPARK_EXECUTOR_OPTS="-Dio.netty.tryReflectionSetAccessible=true"


RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -yq \
    libgomp1 \
    python3-pip \
    openjdk-8-jdk \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY --from=h1theswan/relaxmap /RelaxMap/ompRelaxmap /RelaxMap

COPY requirements.txt /

RUN pip3 --no-cache-dir install -r /requirements.txt

COPY spark_infomap_subclusters.py .
COPY run_subprocesses.py .

VOLUME /data

ENV CACHE_DIR="/data/cache"

# ENTRYPOINT [ "/RelaxMap/ompRelaxmap" ]
# ENTRYPOINT [ "python3", "spark_infomap_subclusters.py" ]
ENTRYPOINT [ "python3", "run_subprocesses.py" ]
CMD [ "--help" ]