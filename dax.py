

import ezlog
import ezpool

import numpy as np

import os
import re
import glob
import shutil
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




class Generation(object):

    def __init__(self, run, clone, gen, files=[]):

        self.run             = run
        self.clone           = clone
        self.gen             = gen

        self._original_files = set(itertools.imap(os.path.abspath,  files))
        self._names          = set(itertools.imap(os.path.basename, files))


    def load_dax(self, gendir):
        """
        Load the files for this generation in the givin directory
        *gendir* should be a cannonical path to a generation's directory
        An example value for *gendir* is 'RUN0123/CLONE4567/GEN8910'

        @param gendir (string)
        """

        genfiles  = map(functools.partial(os.path.join, gendir), os.listdir(gendir))
        bases     = itertools.imap(os.path.basename, genfiles)
        originals = itertools.imap(os.readlink, genfiles)

        _logger.debug('Loading Generation (%d,%d,%d) files %s from %s' % (self.run,self.clone,self.gen,tuple(genfiles),gendir))

        for base, original in itertools.izip(bases, originals):
            self._names.add(base)
            self._original_files.add(original)


    def write_dax(self, trajdir, force=False):

        location = cannonical_gen(self)
        gendir   = os.path.join(trajdir, location)

        _logger.debug('Writing Generation (%d,%d,%d) files %s to %s' % (self.run,self.clone,self.gen,tuple(self._names),gendir))

        if not os.path.exists(gendir):
            _logger.debug('Creating %s' % gendir)
            os.makedirs(gendir)

        self.symlink(gendir, force=force)


    def add(self, path):
        """
        Add the file given by *path* to this generation

        @param path (string): the location of a file

        @raise ValueError: if *path* is not a file
        @raise OriginalMissing: if *path* does not exist
        """

        abspath = os.path.abspath(path)

        if not os.path.exists(abspath):
            raise OriginalMissing, abspath

        if not os.path.isfile(abspath):
            raise ValueError, '%s is not a file' % abspath


        base = os.path.basename(abspath)

        if base in self._names:
            _logger.warning('Already tracking %s' % abspath)
        else:
            self._original_files.add(abspath)
            self._names.add(base)


    def targets(self, root):
        """
        Get the source and target for the files of this generation

        @param root (string): the root directory path
        @return (2-tuple (string, string)): the path to the actual file and it's name under *root*
        """

        for original in self._original_files:
            location = cannonical_gen(self)
            target   = os.path.join(root, os.path.basename(original))
            yield sanitize(original), sanitize(target)


    def symlink(self, root, force=False):
        """
        Symlink the files of the generation under *root*.
        If a target already exists skip it (default) or overwrite it (when *force* is True)

        @param root (string)
        @prarm force=False (Boolean)
        """

        for src, target in self.targets(root):
            loc = os.path.dirname(target)

            ## the target may exist and we either skip it or overwrite it
            if os.path.exists(target) and not force:
                _logger.warning('%s -> %s already exists, skipping' % (target, os.readlink(target)))
                continue
            elif os.path.exists(target) and force:
                _logger.warning('%s -> %s already exists, overwriting' % (target, os.readlink(target)))
                os.remove(target)
            else: pass # this is ok

            _logger.debug('Linking %s -> %s' % (src, target))
            os.symlink(src, target)



    def lookup(self, root, basename, unlink=False):
        """
        Get the path to the name of the file given by *basename*.
        Default: get the file under *root* if unlink if False
        Otherwise: follow the symlink to the original file

        @param root (string)
        @param basename (string)
        @param unlink=False (boolean)
        @return (string)

        @raise SymlinkMissing if the symlink does not exist
        @raise OriginalMissing if the original does not exist and *unlink* is True
        """

        
        symlink = os.path.join(sanitize(root), cannonical_gen(self), basename)

        if not os.path.exists(symlink):
            raise SymlinkMissing, symlink

        if not unlink:
            return symlink

        original = os.readlink(symlink)
        if not os.path.exists(original):
            raise OriginalMissing, original

        return original

        


    def get_file(self, root, pattern, unlink=False):
        """
        Get the path to a file matching *pattern*

        @param root (string)
        @param pattern (string): a python regular expression to match on the basename of one of this generation's files
        @param unlink=False (boolean)
        @return (string): the location of the file

        @raise ValueError: if the *pattern* fails to match or more than one files matched
        @raise SymlinkMissing: if the path does not exist under *root*
        @raise OriginalMissing: if *unlink* is True and the original file does not exist
        """

        bases    = map(os.path.basename, self._files)
        regex    = re.compile(patter)
        matches  = itertools.imap(regex.match, bases)
        notNones = filter(None, matches)

        if len(notNones) == 1:
            base = notNones[0]
            return self.lookup(root, base, unlink=unlink)
        elif len(notNones) < 1:
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


    def add(self, gen, path):
        """
        Add *path* to this trajectory's generation

        @param gen (int)
        @param path (string)
        """

        if gen not in self._generations:
            self._generations[gen] = Generation(self.run, self.clone, gen)

        self._generations[gen].add(path)


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


    def add(self, run, clone, gen, path):
        """
        Add the file *path* to the project

        @param run (int)
        @param clone (int)
        @param gen (int)
        @param path (string)

        @raise ValueError: if *path* is not a file
        """

        _logger.debug('Adding (%d,%d,%d) file %s' % (run, clone, gen, path))

        # if run not in self._data:
        #     _logger.debug('Initializing RUN %s' % run)
        #     self._data[run] = dict()

        # if clone not in self._data[run]:
        #     _logger.debug('Initializing trajectory (%d, %d)' % (run, clone))
        #     self._data[run][clone] = Trajectory(run, clone)

        traj = self.trajectory(run, clone, create=True)
        traj.add(gen, path)


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


    def generation(self, run, clone, gen, create=False):
        return self.trajectory(run, clone, create=create).generation(gen, create=create)

    def load_file(self, fn, path):
        """
        Load a project from the files listed in *path* using *fn* to figure out the RUN, CLONE, and GEN are.

        @param fn (string -> (int,int,int)): a 1-ary function from a string to a 3-tuple of ints.
                                             *fn* accespts an entry from *path* and
                                             returns the (run, clone, gen) for that entry
        @param path (string): a file containing (one per line) the files that make up this project
        """

        _logger.info('Loading data from %s using %s' % (path, fn))

        with open(path) as fd:
            for line in itertools.imap(str.strip, fd):
                r,c,g = fn(line)
                self.add(r,c,g, line)
                
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
