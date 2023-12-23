# Copyright (c) 2023 Nam Seob Seo
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

FROM  docker.io/apache/spark:3.4.2-scala2.12-java11-python3-r-ubuntu

USER root

COPY ./packages/requirements.txt /workspace/packages/requirements.txt
RUN pip install -r /workspace/packages/requirements.txt
RUN mkdir /workspace/app
