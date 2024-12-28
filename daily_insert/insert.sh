#!/bin/bash
eval "$(/Users/ZMCodi/miniforge3/bin/conda shell.bash hook)"
conda activate daily_insert
python /Users/ZMCodi/git/personal/stock-db/daily_insert/insert.py