#!/bin/bash

spark-submit --master spark://ip-10-0-0-6:7077 --files config.txt,forex.keys,cycles.txt,src/arbitrage.py src/main.py
