FROM python:3.11-bullseye
MAINTAINER info@vizzuality.com

ENV NAME nexgddp
ENV USER nexgddp

RUN apt-get update && apt-get install -yq \
    gcc \
    bash \
    git \
    build-essential \
    libcurl4-openssl-dev \
    libffi-dev \
    libjpeg-dev \
    libpq-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    make \
    libpng-dev \
    cmake \
    ffmpeg \
    libsm6 \
    libxext6


RUN groupadd $USER && useradd -g $USER $USER -s /bin/bash

RUN mkdir -p /opt/$NAME
RUN cd /opt/$NAME

RUN pip install --upgrade pip
RUN pip install gunicorn gevent numpy

WORKDIR /opt/$NAME

COPY ./$NAME /opt/$NAME/$NAME
COPY ./microservice /opt/$NAME/microservice
COPY tox.ini /opt/$NAME/tox.ini
COPY requirements.txt /opt/$NAME/requirements.txt
COPY requirements_dev.txt /opt/$NAME/requirements_dev.txt
COPY entrypoint.sh /opt/$NAME/entrypoint.sh
COPY main.py /opt/$NAME/main.py
COPY test.py /opt/$NAME/test.py
COPY tests /opt/$NAME/tests
COPY gunicorn.py /opt/$NAME/gunicorn.py

RUN pip install -r requirements.txt
RUN pip install -r requirements_dev.txt

RUN chown -R $USER:$USER /opt/$NAME

# Tell Docker we are going to use this ports
EXPOSE 3078
USER $USER

# Launch script
ENTRYPOINT ["./entrypoint.sh"]
