#! /usr/bin/bash
source /home/sean/code/venv_py3800/bin/activate
export PYTHONPATH=/home/sean/code/cn_ex_sync

python /home/sean/code/cn_ex_sync/sina/sinaM5origin_transform.py --all --freq 1h && \
python /home/sean/code/cn_ex_sync/vw/mono.py --freq H1 --symbol ALL