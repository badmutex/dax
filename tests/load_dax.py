
import dax
import ezlog

import os
import re
import sys

ezlog.set_level(ezlog.INFO, dax.__name__)


mydir = os.path.dirname(sys.argv[0])
xtclist = os.path.join(mydir, 'lcls.10009')

proj = dax.Project('tests', 'lcls', 'fah', 10009)
proj.load_dax()

