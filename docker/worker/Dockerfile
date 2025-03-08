FROM ubuntu:20.04

MAINTAINER Terradue S.r.l
USER root
ENV PIP_DEFAULT_TIMEOUT=100
ENV LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8 \
    SHELL=/bin/bash \
    PATH=/srv/conda/bin:$PATH \
    DEBIAN_FRONTEND=noninteractive \
    NB_USER=jovyan \
    NB_UID=${UID:-1002} \
    NB_GID=${GID:-1002} \
    APP_BASE=/srv

ENV USER=${NB_USER} \
    HOME=/home/${NB_USER} \
    CONDA_DIR=${APP_BASE}/conda

RUN groupadd --gid ${NB_GID} ${NB_USER}                                                                                             && \
    useradd --comment "Default user" --create-home --gid ${NB_GID} --no-log-init --shell /bin/bash --uid ${NB_UID} ${NB_USER}       && \
    apt-get -qq update                                                                                                              && \
    apt-get -qq install --yes apt-utils                                                                                             && \
    apt-get -qq install --yes --no-install-recommends ttf-dejavu wget make g++ sudo vim less unzip tree file  locales > /dev/null   && \
    apt-get -qq purge                                                                                                               && \
    apt-get -qq clean                                                                                                               && \
    rm -rf /var/lib/apt/lists/*                                                                                                     && \
    echo "LC_ALL=en_US.UTF-8" >> /etc/environment                                                                                   && \
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen                                                                                     && \
    echo "LANG=en_US.UTF-8" > /etc/locale.conf                                                                                      && \
    locale-gen en_US.UTF-8

# conda installation via miniforge
ADD docker/worker/install-miniforge.bash /install-miniforge.bash

RUN chmod 755 /install-miniforge.bash
RUN bash install-miniforge.bash
RUN rm -f /install-*.bash
RUN chown -R $NB_USER:$NB_GID ${HOME}

USER ${NB_USER}

ENV PATH=${CONDA_DIR}/envs/env_snap/snap/bin:${CONDA_DIR}/envs/env_snap/bin:${CONDA_DIR}/bin:$PATH \
    KERNEL_PYTHON_PREFIX=${CONDA_DIR}/envs/env_snap  \
    PREFIX=${CONDA_DIR}/envs/env_snap

ADD environment.yml /tmp/environment.yml

RUN mamba env create -f /tmp/environment.yml
RUN mamba install -y libspatialite -n env_snap
RUN mamba clean --all -f -y

WORKDIR ${HOME}

##### add pip
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt

##### 08/08/2021 add nc tools
USER root
RUN apt-get -qq update && \
    apt-get -qq install --yes --no-install-recommends nco

#### 28/09/2021 add s06p06 libs
USER root
RUN pip install tables rasterio==1.1.8


#### 29/01/2022 add pymodis
USER root
RUN pip install pymodis

#### 14/04/2022
# disable Disable Check for Updates
# https://senbox.atlassian.net/wiki/spaces/SNAP/pages/447119383/Disable+Check+for+Updates

ENV SNAP_HOME="/srv/conda/envs/env_snap/snap"
ENV SNAP_PROPERTY_FILE="${SNAP_HOME}/etc/snap.properties"
ENV SNAP_CONF_FILE="${SNAP_HOME}/etc/snap.conf"

RUN echo snap.versionCheck.interval=NEVER >>  ${SNAP_PROPERTY_FILE}

##### END
ENV PROJ_LIB="/srv/conda/envs/env_snap/share/proj"
USER ${NB_USER}
