
import dax
import ezlog

import os
import re
import sys

ezlog.set_level(ezlog.INFO, dax.__name__)


mydir = os.path.dirname(sys.argv[0])
xtclist = os.path.join(mydir, 'p10009.xtclist.test')


re_gen = re.compile(r'%(sep)sresults-([0-9]+)' % {'sep':os.sep})

def read_path(path):
    r,c = dax.read_cannonical_traj(path)
    m = re_gen.search(path)

    if not m:
        raise ValueError, 'Cannot parse generation from %s' % path

    g = int(m.group(1))

    return r,c,g


proj = dax.Project('tests', 'lcls', 'fah', 10009)
proj.load_file(read_path, xtclist)

proj.write_dax()
