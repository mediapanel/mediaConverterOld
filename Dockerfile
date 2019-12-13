FROM debian:buster

# Requirements
RUN apt-get update && \
    apt-get install -y python2 python-pip default-libmysqlclient-dev \
                       imagemagick ffmpeg mediainfo poppler-utils && \
    pip install requests pymediainfo mysqlclient youtube-dl

# Install mediaConverter

RUN mkdir /app
ADD mediaConverter.py /app/mediaConverter.py
RUN chmod u+x /app/mediaConverter.py

CMD /app/mediaConverter.py
