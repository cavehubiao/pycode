#coding=utf-8
# File : uion.py
# Author: drogba
# Created Time: Sat Aug  6 15:20:18 2016
#=========================================================================

import os
import threading
import logging
import logging.handlers

LOG_FILE = "uion.log"
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024 * 1024 * 1024, backupCount = 5)
fmt = "[%(levelname)s]%(asctime)s - %(message)s"
formatter  = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('uion')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logger.info("start log.......")


def insert_dict(uin_dict, visit_set, li):

    uin, ymd, step, ym = li[0], li[1], int(li[2]), li[1][:6]
    if (uin + ymd) in visit_set:
        return 
    visit_set.add(uin + ymd)
    if not uin_dict.has_key(uin):
        uin_dict[uin] = {ym : [step, step, 1]}
    elif not uin_dict[uin].has_key(ym):
        uin_dict[uin][ym] = [step, step, 1]
    else:
        uin_dict[uin][ym][0] += step
        uin_dict[uin][ym][1] = max(uin_dict[uin][ym][1], step)
        uin_dict[uin][ym][2] += 1

def proc_data():
    parent_dir = os.getcwd()
    #data99_dir = os.path.join(parent_dir, "data99")
    #data99_files = map(lambda x : os.path.join(data99_dir, x), os.listdir(data99_dir))
    data99map = {}
    month_data_dirs = []

    #for f in data99_files:
    #    data99map[os.path.basename(f)] = open(f, 'r')

    for i in range(10):
        mdir = os.path.join(os.path.split(parent_dir)[0], "monthdata/data" + str(i))
        month_data_dirs.append(mdir)
        if not os.path.exists(mdir):
            os.makedirs(mdir)

    for i in range(10):
        data_dir = os.path.join(parent_dir, "data" + str(i)) 
        unknows = map(lambda x : os.path.join(data_dir, x), os.listdir(data_dir))
        bfiles = filter(lambda x : os.path.isfile(x), unknows)
        
        for f in bfiles:
            uin_dict = {}
            visit_set = set()

            fb = os.path.basename(f)
            wfile_path = os.path.join(month_data_dirs[i], fb)
	    if os.path.exists(wfile_path):
		continue

            for line in open(f, "r"):
                li = line.split('\t')
                if li[0] == "fuin":
                    continue
                insert_dict(uin_dict, visit_set, li)
            
            if os.path.getsize(f) < 1000 * 1000 * 100 and data99map.has_key(fb):
                for line in data99map[fb]:
                    li = line.split('\t')
                    if li[0] == "fuin":
                        continue
                    insert_dict(uin_dict, visit_set, li)
            
            wfd = open(wfile_path, "a+")
            
            wfd.write("uin\tmonth\ttotal\tmax\tcnt\n")
            for kuin in uin_dict:
                for kmonth in uin_dict[kuin]:
                    vtotal, vmax, vcnt = uin_dict[kuin][kmonth][0], uin_dict[kuin][kmonth][1], uin_dict[kuin][kmonth][2]
                    wstr = "%s\t%s\t%d\t%d\t%d\n" % (kuin, kmonth, vtotal, vmax, vcnt)
                    wfd.write(wstr)
            wfd.close()
            logger.info("%s - completed", (f))


if __name__ == "__main__":
    proc_data()

