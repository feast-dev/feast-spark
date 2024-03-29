#!/usr/bin/env bash

CURRENT_PATH=$PWD
DESTINATION=${DESTINATION:-$CURRENT_PATH}

# 1. Create libraries (dependencies) package
if [[ -f "$DESTINATION/libs.tar.gz" ]]; then
    echo "$DESTINATION/libs.tar.gz exists."
else
  tmp_dir=$(mktemp -d)
  pip3 install -t ${tmp_dir}/libs great-expectations==0.13.2 pyarrow==2.0.0 Jinja2==3.0.3
  cd $tmp_dir && tar -czf libs.tar.gz libs/ && mv libs.tar.gz $DESTINATION/libs.tar.gz
fi

# 2. Pickle python udf
cd $CURRENT_PATH
pip3 install great-expectations==0.13.2 setuptools pyspark==3.1.3 Jinja2==3.0.3 pyarrow==2.0.0
python3 udf.py $DESTINATION/udf.pickle
