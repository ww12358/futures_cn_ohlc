#! /usr/bin/bash
source /home/sean/code/venv_py3800/bin/activate
export PYTHONPATH=/home/sean/code/cn_ex_sync

python /home/sean/code/cn_ex_sync/sina/sync_sina_M5_origin.py --major && python /home/sean/code/cn_ex_sync/vw/mono.py --freq M5 --major

