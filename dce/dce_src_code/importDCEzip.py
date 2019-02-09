# -*- coding:utf-8 -*-
import ntpath
import os
import zipfile
import re

dir_name = "D:\\Desktop\\dce_excels"
target_dir = "D:\\Desktop\\dce_excels\\dce"
extension = ".zip"

for item in os.listdir(dir_name):
    if item.endswith(extension):
        file_name = os.path.abspath(item)
        file_path = os.path.join(dir_name, item)
        base_name = os.path.basename(item)
        symbol = re.search('2006(.*).zip', base_name).group(1)
        target_name = symbol + "_" + "2006" + ".csv"
        target_path = os.path.join(target_dir, symbol)
        print 'file_path %s' % file_path
        print 'target_path %s' % target_path

        # file_name = "D:\\Desktop\\dce_excels\\2014a.zip"
        # target_path = 'D:\\Desktop\\dce_excels\\dce\\a\\'
        # f_nm = "a_2014.csv"
        try:
            zf = zipfile.ZipFile(file_path)
            infos = zf.infolist()

            for info in infos:
                info.filename = target_name
                zf.extract(info, target_path)
                print 'target_name  %s' % target_name
            print 'Done...'
            zf.close()
        except:
            print "error"