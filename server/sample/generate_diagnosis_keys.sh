#!/bin/sh

export CONFIG_PATH=$HOME/en-calibration-server/server/sample/config.json

cd $HOME/en-calibration-server/server
python3 generate_diagnosis_keys.py
