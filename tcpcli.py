from gevent import monkey;monkey.patch_all();
from multiprocessing import Process, cpu_count, Queue, JoinableQueue
import socket
import sys
import os
import struct
import json
import time
import gevent
import multiprocessing
from gevent.pool import Pool
d = {}

#def tcpcli(ip, port, headlen):
def tcpcli(*args):
    [ip, port, headlen, str] = args[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_addr = (ip, port)
    r = sock.connect(server_addr)
    #print 'connect'
    buff_size = 1024 * 1024
    data = ""
    sock.sendall(str)
    #print 'sendall'
    while True:
        tdata = sock.recv(buff_size)
        #print 'recv'
        #if len(tdata) == 0:
        #    break;
        data += tdata;
        break
    '''
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(str, (ip, port))
    data, addr = sock.recvfrom(1024)
    '''
    sock.close()
    #print 'close'
    parsed = json.loads(data[headlen:])
    if parsed["code"] not in d:
        d[parsed["code"]] = 0
    d[parsed["code"]] += 1
    #print json.dumps(parsed, indent = 4, sort_keys = True)

def udpcli(*args):
    [ip, port, headlen, str] = args[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        with gevent.Timeout(1) as tm:
            sock.sendto(str, (ip, port))
            data, addr = sock.recvfrom(1024)
            sock.close()
            parsed = json.loads(data[headlen:])
            if parsed["code"] not in d:
                d[parsed["code"]] = 0
            d[parsed["code"]] += 1
    except:
        if -4 not in d:
            d[-4] = 0
        d[-4] += 1

def run_proc(ip, port, headlen, str):
    reqs = [[ip, port, headlen, str]] * 20000
    ts = time.time()
    print 'start', ts
    pool = Pool(200)
    pool.map(udpcli, reqs)
    #jobs = [gevent.spawn(tcpcli, param) for param in reqs]
    #gevent.joinall(jobs, timeout = 20)
    print d
    te = time.time()
    print 'end', time.time(), 'time use', te - ts


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    if len(sys.argv) < 2:
        print "cmd ip port"
        exit()
    port = int(sys.argv[1])
    ip = "10.239.206.190"
    headlen = 0
    if len(sys.argv) > 2:
        ip = sys.argv[2]
    if len(sys.argv) > 3:
        headlen = int(sys.argv[3])

    msg = raw_input()
    form = ">I%ds" % len(msg)
    print form
    str = struct.pack(form, len(msg), msg)
    #timer = gevent.Timeout.start_new(1)
    pl = []
    for i in range(1):
        p = multiprocessing.Process(target = run_proc, args = (ip, port, headlen, str))
        p.start()
        pl.append(p)
    for p in pl:
        p.join()
