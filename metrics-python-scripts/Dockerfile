
   
# INSTALL PYTHON IMAGE
FROM aptplatforms/oraclelinux-python

FROM ubuntu:latest

# INSTALL TOOLS
RUN mkdir -p /code

ADD ./tpscrm/requirements.txt /code

WORKDIR /code

# INSTALL INSTANTCLIENT AND DEPENDENCIES
RUN pip install -r requirements.txt

COPY ./tpscrm/prod_product.py ./
COPY config.ini ./
COPY ./tpscrm/run.sh ./
RUN apt-get install -y dos2unix
RUN dos2unix run.sh
RUN chmod a+x run.sh

CMD ["./run.sh"]
