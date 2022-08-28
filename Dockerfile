ARG PYTHON_VERSION=3.10.6-slim

FROM python:${PYTHON_VERSION}

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    python3-dev \
    python3-setuptools \
    python3-wheel

WORKDIR /miserytest

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . /miserytest

CMD python /miserytest/miseryv2.py