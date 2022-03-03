#! /usr/bin/bash 

source /home/sean/code/venv_py3810/bin/activate
export PYTHONPATH=/home/sean/code/cn_ex_sync
if [ -z "$STY" ]; then exec screen -dm -S screenName /bin/bash "$0"; fi
python /home/sean/code/cn_ex_sync/sina/start.py

