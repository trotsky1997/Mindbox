ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
ARG PIP_INDEX_URL=https://pypi.org/simple/
ARG PIP_TRUSTED_HOST=pypi.org
ARG EXTRA_PIP=""
ARG PREWARM_MODULES=""
ARG POOL_SIZE="16"
WORKDIR /
RUN pip install --no-cache-dir --index-url ${PIP_INDEX_URL} --trusted-host ${PIP_TRUSTED_HOST} \
        protobuf ${EXTRA_PIP}
ENV WORKER_PREWARM_MODULES=${PREWARM_MODULES}
ENV WORKER_POOL_SIZE=${POOL_SIZE}
COPY worker-rust /usr/local/bin/worker-rust
COPY inspect_pb2.py /inspect_pb2.py
ENV PYTHONPATH="/"
ENV LD_LIBRARY_PATH="/usr/local/lib"
CMD ["/usr/local/bin/worker-rust"]
