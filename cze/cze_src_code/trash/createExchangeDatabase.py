# coding: utf-8
import h5py
from cze.include import cze_symbols

f = h5py.File('../data/cze.hdf5', 'w')

months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

for j in cze_symbols:
    grp = f.create_group(j)
    grp_d = grp.create_group("D")
    for i in months:
        if i < 10:
            subgrp = grp_d.create_group('_0'+str(i))
        else:
            subgrp = grp_d.create_group('_'+str(i))

f.flush()
f.close()