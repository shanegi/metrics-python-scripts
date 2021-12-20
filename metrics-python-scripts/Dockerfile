
   
# INSTALL PYTHON IMAGE
FROM aptplatforms/oraclelinux-python

# INSTALL TOOLS
RUN mkdir -p /code

ADD ./tpscrm/requirements.txt /code

WORKDIR /code

# INSTALL INSTANTCLIENT AND DEPENDENCIES
RUN pip install -r requirements.txt

COPY ./tpscrm/prod_product.py ./
COPY config.ini ./
COPY ./tpscrm/run.sh ./

RUN chmod a+x run.sh

CMD ["./run.sh"]
