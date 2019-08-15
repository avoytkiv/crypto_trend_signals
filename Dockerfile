FROM python:3.7

WORKDIR /usr/src/app

RUN apt-get install tzdata
ENV TZ Europe/Kiev
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz &&\
    tar xzvf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib*

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN mkdir -p ./data

# Bundle app source
COPY . .
CMD [ "python", "main.py" ]