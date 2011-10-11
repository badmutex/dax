

import ezlog
import ezpool

import numpy as np

import os
import re
import glob
import shutil
import fnmatch
import itertools
import functools

_logger = ezlog.setup(__name__)



_re_run   = re.compile(r'%(sep)sRUN([0-9]+)'    % {'sep':os.sep})
_re_clone = re.compile(r'%(sep)sCLONE([0-9]+)'  % {'sep':os.sep})
_re_gen   = re.compile(r'%(sep)sGEN([0-9]+)'    % {'sep':os.sep})



def cannonical_project_name(group, platform, projid):
    return '%s.%s.%s' % (group, platform, projid)


def read_cannonical_traj(path):
    """
    Get the RUN,CLONE identifiers for a trajectory from a path
    """

    m_run = _re_run.search(path)
    if not m_run:
        raise ValueError, 'Could not parse the RUN from %s' % path

    m_clone = _re_clone.search(path)
    if not m_clone:
        raise ValueError, 'Could not parse the CLONE from %s' % path

    rc = m_run.group(1), m_clone.group(1)
    return tuple(map(int, rc))


def read_cannonical(path):
    """
    Gets the RUN,CLONE,GEN values from a path in the cannonical format

    @param (string)
    @return (3-tuple (int,int,int)): (run, clone, gen)

    @raise ValueError: if *path* cannot be parsed to determin the run,clone,gen values
    """

    run, clone = read_cannonical_traj(path)

    m_gen = _re_gen.search(path)
    if not m_gen:
        raise ValueError, 'Could not parse the GEN from %s' % path

    gen = int(m_gen.group(1))
    return run, clone, gen



def cannonical_traj(run, clone):
    return os.path.join('RUN%04d' % run, 'CLONE%04d' % clone)


def cannonical(run, clone, gen):
    """
    Get the cannonical name for a work unit

    @param run (int)
    @param clone (int)
    @param gen (int)

    @return (string)
    """

    return os.path.join(cannonical_traj(run, clone), 'GEN%04d' % gen)


def cannonical_gen(gen):
    """
    Get the cannonical location for the generation.
    Wraps the *location()* function

    @param (Generation)
    @return (string)
    """

    return cannonical(gen.run, gen.clone, gen.gen)


def sanitize(path):
    return os.path.abspath(os.path.expanduser(path))



class SymlinkMissing (Exception) : pass
class OriginalMissing (Exception): pass
class DuplicateException (Exception): pass




class Location(object):

    def resolve(self):
        """
        Returns a path to the local copy of file
        """
        raise NotImplementedError


    def _parse_url(self):
        raise NotImplementedError


    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, exc_type, exc_value, traceback):
        raise NotImplementedError


    def _debug(self):
        _logger.debug('Location: location = %s' % self)


    @classmethod
    def location(cls, url):

        _logger.debug('Location.location: parsing url %s' % url)

        if url.startswith('chirp'):
            return Chirp(url)
        elif url.startswith('file'):
            return Local(url)
        else:
            raise ValueError, 'Cannot parse url %s' % url

    @classmethod
    def load_url(cls, path):
        """
        Given a Location file read and return the url from it
        @param path (string): path to a dax.Project file
        @return (string): the url contained in *path*
        """

        with open(path) as fd:
            return fd.read().strip()

    @classmethod
    def from_file(cls, path):
        """
        Given a Location file return the corresponding Location object
        @param path (string)
        @return (Location)
        """
        url = Location.load_url(path)
        loc = Location.location(url)
        return loc


    def to_file(self, path, force=False):
        """
        Writes the url of this Location to file designated by *path*
        @param path (string)
        @param force=False (boolean): overwrite *path* if it already exists
        """

        def write():
            with open(path, 'w') as fd:
                fd.write(self.url)

        if os.path.exists(path) and force:
            _logger.warning('Location %s already exists, overwritting' % path)
            write()
        elif os.path.exists(path) and not force:
            _logger.warning('Location %s already exists, skipping' % path)
        else: # not os.path.exist(path):
            _logger.debug('Location %s being written' % path)
            write()


def read_filelist(path, kind='local', **kws):
    """
    Reads a list of files for a project and returns a generator over Locations

    @param path (string): the path containing the project files (one per line)
    @param kind=local (string enum: local|chirp)
    @params **kws: if *kind* is 'chirp', then 'host' (string) is required and 'port' (int) is optional
    @return (generator over Locations)
    @raise ValueError if the proper Location type cannot be determined from the parameters
    """

    ### define the line handlers
    def local_handler(line):
        url = 'file://%s' % line
        return Local(url)

    def chirp_handler(line, host=None, port=None):
        url = 'chirp://%(host)s%(port)s%(path)s' % {
            'host' : host, 'port' : (':%d' % port) if port else '', 'path' : line}
        return Chirp(url)

    ### pick the correct handler
    if kind == 'local':
        handler = local_handler
    elif kind == 'chirp' and 'host' in kws:
        handler = chirp_handler
    else:
        raise ValueError, 'Cannot determin proper Location: kind=%s and **kws=%s' % (kind, kws)


    ### handle the filelist
    with open(path) as fd:
        for line in itertools.imap(str.strip, fd):
            yield handler(line, **kws)



class Local(Location):
    def __init__(self, url, **kws):
        self.url = url
        self.name = None

        self._parse_url()

        Location.__init__(self)

    def _parse_url(self):

        ### remove the 'file://'
        ix = self.url.find('://') + 3
        self.name = self.url[ix:]
        self._debug()


    def __str__(self):
        return 'Local(url=%r,name=%r)' % (self.url, self.name)

    def resolve(self):
        return self.name

    def __enter__(self):
        return self.resolve()

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Chirp(Location):

    def __init__(self, url):

        self.url    = url
        self.host   = 'localhost'
        self.port   = None
        self.remote = None
        self.name   = None

        self._parse_url()

        Location.__init__(self)

    def _get_local_name(self):
        name = self.remote.replace(os.sep, '!')
        return os.path.join('/tmp', name)

    def _parse_url(self):

        ### remove 'chirp://'
        ix = self.url.find('://') + 3
        url = self.url[ix:]

        self._debug()

        ### find the port if it is present
        ix_port = url.find(':')
        if ix_port >= 0:
            ix_host_end   = ix_port

            port          = url[ix_port+1:]
            ix_slash      = port.find('/')
            port          = port[:ix_slash]
            port          = int(port)
            self.port = port

            self._debug()

        ### setup for parsing out the host
        else:
            ix_host_end   = url.find('/')

        ### find the hostname
        host          = url[:ix_host_end]
        self.host = host

        self._debug()

        ### find data location
        ix_data = url.find('/')
        data = url[ix_data:]
        self.remote = data

        self._debug()


    def __str__(self):
        return 'Chirp(%r,host=%r,port=%r,remote=%r,name=%r)' % (self.url, self.host, self.port, self.remote, self.name)

    def resolve(self):

        self.name = self._get_local_name()
        if os.path.exists(self.name):
            _logger.debug('Chirp: %s is cached' % self.name)
            return self.name
        else:

            cmd = "chirp_get %(host)s%(port)s '%(remote)s' '%(local)s'" % {
                'host'   : self.host,
                'port'   : (':%d' % self.port) if self.port else '',
                'remote' : self.remote,
                'local'  : self.name      }

            _logger.debug('Chirp: Getting remote:%s -> local:%s' % (self.remote, self.name))
            _logger.debug('Chirp: Executing: %s' % cmd)

            exitcode = os.system(cmd)
            if not exitcode == 0:
                raise IOError, 'Command %s failed with exit code %s' % (cmd, exitcode)

            return self.name


    def __enter__(self):
        return self.resolve()

    def __exit__(self, exc_type, exc_value, traceback):
        _logger.debug('Chirp: unlinking local file %s' % self.name)
        os.unlink(self.name)




class Generation(object):

    def __init__(self, run, clone, gen, locations=[]):

        self.run             = run
        self.clone           = clone
        self.gen             = gen

        ## dict of file name -> Location
        self._names          = dict()
        for loc in locations:
            name = os.path.basename(loc.url)
            self._names[name] = loc


    def load_dax(self, gendir):
        """
        Load the files for this generation in the givin directory
        *gendir* should be a cannonical path to a generation's directory
        An example value for *gendir* is 'RUN0123/CLONE4567/GEN8910'

        @param gendir (string)
        """

        genfiles = os.listdir(gendir)
        for name in genfiles:
            path = os.path.join(gendir, name)
            loc = Location.from_file(path)
            self._names[name] = loc


    def write_dax(self, trajdir, force=False):

        location = cannonical_gen(self)
        gendir   = os.path.join(trajdir, location)

        _logger.debug('Writing Generation (%d,%d,%d) files %s to %s' % (self.run,self.clone,self.gen,tuple(self._names),gendir))

        if not os.path.exists(gendir):
            _logger.debug('Creating %s' % gendir)
            os.makedirs(gendir)

        for name, location in self._names.iteritems():
            path = os.path.join(gendir, name)
            location.to_file(path, force=force)


    def add(self, location):
        """
        Add the Location given by *location* to this generation

        @param location (Location): the location of a file

        @raise DuplicateException: if the location is already tracked
        """

        name = os.path.basename(location.url)
        if name in self._names:
            raise DuplicateException, location

        self._names[name] = location



    def location(self, root, pattern):
        """
        Get the path to a file matching *pattern*

        @param root (string)
        @param pattern (string): a pattern similar to shell wildcards acceptable to the python fnmatch modules to match on the basename of one of this generation's files
        @return (string or None): the Location if it matches otherwise None

        @raise ValueError: if the *pattern* matchs more than one files matched
        """

        bases   = self._names.keys()
        matches = fnmatch.filter(bases, pattern)

        if len(matches) == 1:
            name = matches[0]
            return self._names[name]
        elif len(matches) < 1:
            raise ValueError, 'Pattern %s failed to match any of %s' % (pattern, bases)
        else:
            raise ValueError, 'Pattern %s matched too many of %s' % (pattern, bases)




class Trajectory(object):

    def __init__(self, run, clone):
        self.run = run
        self.clone = clone
        self._generations = dict()


    def load_dax(self, root):
        """
        Load the trajectory 
        """

        _logger.info('Loading Trajectory (%d,%d) from %s' % (self.run, self.clone, root))

        gendirs = glob.iglob(os.path.join(root, 'GEN*'))

        for dirpath in gendirs:
            r,c,g = read_cannonical(dirpath)
            gen   = self.generation(g, create=True)
            gen.load_dax(dirpath)



    def write_dax(self, prefix, force=False):

        location = cannonical_traj(self.run, self.clone)
        trajdir  = os.path.join(prefix, location)

        _logger.info('Writing Trajectory (%d,%d) with %d gens to %s' % (self.run,self.clone,self.num_generations(),trajdir))

        if not os.path.exists(trajdir):
            _logger.debug('Creating %s' % trajdir)
            os.makedirs(trajdir)

        for gen in self.generations():
            gen.write_dax(prefix, force=force)


    def generation(self, gen, create=False):
        """
        Get the Generation for *gen*.
        If it does not exist and *create* is True, create then return it, otherwise a ValueError will be raised
        """

        if gen not in self._generations and not create:
            raise ValueError, 'Generation (%d,%d,%d) does not exist' % (self.run, self.clone, gen)

        elif gen not in self._generations and create:
            self._generations[gen] = Generation(self.run, self.clone, gen)

        return self._generations[gen]


    def num_generations(self):
        """
        @return (int): the number of generations in this Trajectory
        """
        return len(self._generations)

    def generations(self):
        for k in self._generations.keys():
            yield self._generations[k]


    def add(self, gen, location):
        """
        Add *location* to this trajectory's generation

        @param gen (int)
        @param location (Location)
        """

        if gen not in self._generations:
            self._generations[gen] = Generation(self.run, self.clone, gen)

        self._generations[gen].add(location)


class Project(object):

    def __init__(self, prefix, group, platform, projid):

        self._prefix   = prefix
        self._group    = group
        self._platform = platform
        self._projid   = projid

        ## sets the self._name and self._root based on the prefix, group, platform, and projid fields
        self._update_name_root()

        self._data     = dict()


    def _update_name_root(self):
        self._name = cannonical_project_name(self._group, self._platform, self._projid)
        self._root = os.path.join(self._prefix, self._name)

    def set_group(self, group):
        self._group = group
        self._update_name_root()

    def set_platform(self, platform):
        self._platform = platform
        self._update_name_root()

    def set_projid(self, projid):
        self._projid = projid
        self._update_name_root()

    def prefix(self)   : return self._prefix
    def group(self)    : return self._group
    def platform(self) : return self._platform
    def projid(self)   : return self._projid
    def name(self)     : return self._name
    def root(self)     : return self._root


    def add(self, run, clone, gen, location):
        """
        Add the *location* to the project

        @param run (int)
        @param clone (int)
        @param gen (int)
        @param path (Location)

        @raise ValueError: if *path* is not a file
        """

        _logger.debug('Adding (%d,%d,%d) %s' % (run, clone, gen, location))

        traj = self.trajectory(run, clone, create=True)
        traj.add(gen, location)


    def trajectory(self, run, clone, create=False):

        _logger.debug('Grabbing Trajectory (%d,%d) with create=%s' % (run, clone, create))

        if run not in self._data and not create:
            raise ValueError, 'RUN %s not known' % run
        elif run not in self._data and create:
            self._data[run] = dict()

        if clone not in self._data[run] and not create:
            raise ValueError, 'CLONE %s not known' % clone
        elif clone not in self._data[run] and create:
            self._data[run][clone] = Trajectory(run, clone)

        if run not in self._data and clone not in self._data[run] and not create:
            raise RuntimeError, 'Trajectory (%d,%d) not known and create=%s' % (run,clone,create)

        return self._data[run][clone]

    def trajectories(self):
        for r in self._data.iterkeys():
            for c in self._data[r].iterkeys():
                yield self._data[r][c]


    def locations(self, pattern, files=False):

        for traj in self.trajectories():
            for gen in traj.generations():

                try:
                    loc = gen.location(self._root, pattern)
                except ValueError, e:
                    _logger.error('Could not get location for (%d,%d,%d): %s' % (gen.run, gen.clone, gen.gen, e))
                    continue

                if loc and not files: yield loc
                elif loc and files:
                    prefix = self._root
                    path   = cannonical(gen.run, gen.clone, gen.gen)
                    name   = os.path.basename(loc.url)
                    yield os.path.join(prefix, path, name)
                else: _logger.warn('Project: pattern %s did not match any file for generation (%d,%d,%d)' % \
                                       (pattern, gen.run, gen.clone, gen.gen))


    def generation(self, run, clone, gen, create=False):
        return self.trajectory(run, clone, create=create).generation(gen, create=create)

    def load_locations(self, fn, locations):
        """
        Load a project from the files listed in *path* using *fn* to figure out the RUN, CLONE, and GEN are.

        @param fn (string -> (int,int,int)): a 1-ary function from a string to a 3-tuple of ints.
                                             *fn* accespts an entry url from *locations* and
                                             returns the (run, clone, gen) for that entry
        @param locations (iterable over Locations)
        """

        _logger.info('Loading data from %s using %s' % (locations, fn))

        for loc in locations:
            r,c,g = fn(loc.url)
            self.add(r,c,g, loc)

                
    def load_dax(self):

        root = self.root()

        pattern = os.path.join(root, 'RUN*', 'CLONE*')

        _logger.info('Loading files from: %s' % pattern)

        for dirpath in glob.iglob(pattern):
            r,c = read_cannonical_traj(dirpath)
            traj = self.trajectory(r, c, create=True)
            traj.load_dax(dirpath)

    def write_dax(self, force=False):

        if not os.path.exists(self._root):
            _logger.debug('Creating %s' % self._root)

        _logger.info('Writing project to %s' % self._root)

        for traj in self.trajectories():
            traj.write_dax(self._root, force=force)



def _test_location_context():
    url = 'http://foo.bar'
    url = 'file:///pscratch/csweet1/lcls/data/lcls.fah.10009'
    url = 'chirp://lclsstor01.crc.nd.edu:9094/lcls/fah/data/PROJ10009/RUN0/CLONE0/results-000.tar.bz2'
    with Location.location(url) as name:
        print 'Hello local file:', name

def _test_read_filelist():
    filelist = 'tests/p10009.xtclist.test2'
    local_locations = read_filelist(filelist, kind='local')
    chirp_locations = read_filelist(filelist, kind='chirp', host='localhost', port=9887)

    print 'Local locations:\n\t',
    print '\n\t'.join(map(str,local_locations))

    print 'Chirp locations:\n\t',
    print '\n\t'.join(map(str,chirp_locations))


def _test_load_write_project():
    filelist = 'tests/p10009.xtclist.test2'
    local_locations = read_filelist(filelist, kind='local')
    chirp_locations = read_filelist(filelist, kind='chirp', host='localhost', port=9887)

    def read_path(path):
        re_gen = re.compile(r'%(sep)sresults-([0-9]+)' % {'sep':os.sep})
        r,c = read_cannonical_traj(path)
        m = re_gen.search(path)

        if not m:
            raise ValueError, 'Cannot parse generation from %s' % path

        g = int(m.group(1))

        return r,c,g

    proj = Project('tests', 'lcls', 'fah', 10009)
    proj.load_locations(read_path, chirp_locations)
    proj.write_dax()

def _test():
    proj = Project('tests', 'lcls', 'fah', 10009)
    proj.load_dax()
    locs = proj.locations('*.xtc', files=False)
    for l in locs:
        print l



if __name__ == '__main__':
    ezlog.set_level(ezlog.DEBUG, __name__)
    _test()
