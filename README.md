# pyspark_substrait

## Pre-requisites

Create conda environment

```bash
conda env create -f environment.yaml
```

```bash

```

```bash
cd pyspark_substrait
git submodule sync --recursive && git submodule update --init --recursive
cd substrait-python && \
    chmod u+x gen_proto.sh && ./gen_proto.sh && \
    pip install .
```

## Developer Mode

```bash
cd pyspark_substrait
pip install -e .
```