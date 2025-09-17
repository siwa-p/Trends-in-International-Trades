FROM mcr.microsoft.com/vscode/devcontainers/python:3.12-bullseye

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl openjdk-11-jdk && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*


# Set JAVA_HOME so PySpark can find Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --upgrade pip

COPY requirements.txt /tmp/pip-tmp/


RUN pip install --no-cache-dir -r /tmp/pip-tmp/requirements.txt


RUN rm -rf /tmp/pip-tmp


USER vscode

WORKDIR /workspace