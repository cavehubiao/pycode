import os
import ceasylog
def run():

    ceasylog.clog.int()
    dir_list = ["data201604_05/workday", "data201606_07/workday", "data201608/workday", "data201609_11/workday", "data201612/workday", "data_before20160401/workday"]
    li = [map(lambda y : os.path.join(x, y), os.listdir(os.path.join(dir_list[2], x))) for x in os.listdir(dir_list[2])]
    fli = []
    for x in li:
        fli += li
    print fli, len(fli)

    wpre = "workdata_all"
    for x in range(10):
        adir = os.path.join(wpre, "data" + str(x))
        if not os.path.exists(adir):
            os.makedirs(adir)

    for x in fli:
        table = dict()
        for y in dir_list:
            real_file = os.path.join(y, x)
            if not os.path.exists(real_file):
                ceasylog.clog.info("%s %s skipped" % (y, x))
                continue
            ceasylog.clog.info("%s start" % real_file)
            for line in open(real_file, 'r'):
                li = line.split('\t')
                uin, workday, workstep, freeday, freestep = int(li[0]), int(li[1]), int(li[2]), int(li[3]), int(li[4])
                if table.has_key(uin):
                    table[uin][0] += workday
                    table[uin][1] += workstep
                    table[uin][2] += freeday
                    table[uin][3] += freestep
                else:
                    table[uin] = [workday, workstep, freeday, freestep]
            ceasylog.clog.info("%s end" % real_file)

        wf = os.path.join(wpre, x)
        wfd = open(wf, 'w')
        for k in table:
            table[k].insert(0, k)
            wfd.write('\t'.join(table[k]) + '\n')
        ceasylog.clog.info("%s done" % x)


if __name__ == "__main__":
    run()
