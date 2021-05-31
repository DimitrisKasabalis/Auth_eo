FROM osgeo/gdal:ubuntu-small-3.2.3

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN apt-get update -y; \
    apt-get install -y --no-install-recommends \
    python3-pip


ADD docker-entrypoint.sh docker-entrypoint
ADD requirements.txt /requirements.txt
RUN chmod +x docker-entrypoint
RUN mkdir -p /local_files_root
ADD mproj /src

RUN pip install -r /requirements.txt

ENTRYPOINT ["/docker-entrypoint"]
CMD ["worker"]
