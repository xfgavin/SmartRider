FROM python:3.7.9-slim-buster

ENV DEBIAN_FRONTEND noninteractive
WORKDIR /usr/src/app

ADD requirements.txt /tmp
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ADD installer.sh /tmp

RUN /tmp/installer.sh

CMD [ "/usr/src/app/smartrider_app.py" ]
ENV DEBIAN_FRONTEND teletype
