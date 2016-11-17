FROM nodesource/xenial
MAINTAINER Brandon B. Jozsa <bjozsa@jinkit.com>

##################################################
# Set environment variables                      #
##################################################

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm

##################################################
# Install tools                                  #
##################################################

RUN apt-get update && \
    apt-get install -y \
    apt-utils \
    apt-transport-https \
    locales \
    curl \
    wget \
    git \
    python \
    build-essential \
    make \
    g++ \
    libavahi-compat-libdnssd-dev \
    libkrb5-dev \
    net-tools 

##################################################
# Install homebridge                             #
##################################################

RUN npm install -g homebridge \
    homebridge-openhab \
    homebridge-ninjablock-temperature \
    homebridge-ninjablock-humidity \
    homebridge-ninjablock-alarmstatedevice \
    homebridge-people \
    homebridge-ifttt --unsafe-perm

##################################################
# Start                                          #
##################################################

USER root
RUN mkdir -p /var/run/dbus

ADD image/run.sh /root/run.sh

EXPOSE 5353 51826
CMD ["/root/run.sh"]
