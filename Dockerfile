FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.0
LABEL name="eodash catalog generator" \
    vendor="EOX IT Services GmbH <https://eox.at>" \
    license="MIT Copyright (C) 2025 EOX IT Services GmbH <https://eox.at>" \
    type="eodash catalog"

USER root
WORKDIR /opt/eodash_catalog

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    python3-pip


# install python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

RUN  apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# install the package itself
COPY . .
RUN pip install --break-system-packages .

# test if was installed
RUN eodash_catalog --help

CMD ["eodash_catalog"]

LABEL version="0.3.23"
