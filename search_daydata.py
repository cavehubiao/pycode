import sys, getopt

def useage():
    print '''useage:
    python this.py --month=201701 --out=./out.txt --in=./in.txt
             '''
    sys.exit()


def search(month, infile, outfile):
    pass

def go():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:', ['month=', 'out=', 'in='])
        for a, b in opts:
            if a in ('-h',):
                useage()
            if a in ('month',):
                month = b
            if a in ('out', ):
                outfile = b
            if a in ('in', ):
                infile = b
        print 'month %s, infile %s, outfile %s' % (month, outfile, infile)
        search(month, infile, outfile)
    except:
        useage()

if __name__ == '__main__':
    go()
