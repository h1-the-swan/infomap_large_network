FROM ubuntu:20.04

RUN mkdir /RelaxMap

RUN apt-get update && apt-get install -y \
    libgomp1 \
    python3-pip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY --from=h1theswan/relaxmap /RelaxMap/ompRelaxmap /RelaxMap

WORKDIR /RelaxMap
RUN pip3 --no-cache-dir install python-dotenv humanfriendly pandas pyspark infomap

ENTRYPOINT [ "/RelaxMap/ompRelaxmap" ]
CMD [ "--help" ]