FROM continuumio/miniconda3
RUN apt-get update && apt-get install -y build-essential redis-tools

RUN conda install -c bioconda mysqlclient
RUN conda install -c conda-forge celery requests
RUN conda install -c conda-forge pyomo pyomo.extras ipopt

COPY requirements.txt /brain/requirements.txt
RUN pip install -r /brain/requirements.txt
COPY . /brain
WORKDIR /brain

ENTRYPOINT ["/opt/conda/bin/python"]