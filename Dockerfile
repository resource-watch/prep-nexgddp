FROM python:3.6.2-stretch
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
    libgdal-dev \
    gdal-bin \
    make \
    libpng-dev \
    cmake

ENV CPLUS_INCLUDE_PATH /usr/include/gdal
ENV C_INCLUDE_PATH /usr/include/gdal

RUN groupadd $USER && useradd -g $USER $USER -s /bin/bash

RUN easy_install pip && pip install --upgrade pip
RUN pip install virtualenv gunicorn gevent numpy
RUN pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==2.1.3

RUN mkdir -p /opt/$NAME
RUN cd /opt/$NAME && virtualenv venv && /bin/bash -c "source venv/bin/activate"

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

RUN cd /opt/$NAME && pip install -r requirements.txt
RUN cd /opt/$NAME && pip install -r requirements_dev.txt

RUN chown -R $USER:$USER /opt/$NAME

# Tell Docker we are going to use this ports
EXPOSE 3078
USER $USER

# Launch script
ENTRYPOINT ["./entrypoint.sh"]
