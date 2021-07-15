FROM ubuntu:20.04

RUN mkdir /RelaxMap

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -yq \
    libgomp1 \
    python3-pip \
    default-jdk \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY --from=h1theswan/relaxmap /RelaxMap/ompRelaxmap /RelaxMap

COPY requirements.txt /

RUN pip3 --no-cache-dir install -r /requirements.txt

COPY spark_infomap_subclusters.py .

VOLUME /data

# ENTRYPOINT [ "/RelaxMap/ompRelaxmap" ]
ENTRYPOINT [ "python3", "spark_infomap_subclusters.py" ]
CMD [ "--help" ]