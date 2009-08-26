""" Build tool that finds dependencies automatically for any language.

fabricate is a build tool that finds dependencies automatically for any
language. It's small and just works. No hidden stuff behind your back. It was
inspired by Bill McCloskey's make replacement, memoize, but fabricate works on
Windows as well as Linux.

Read more about how to use it and how it works on the project page:
    http://code.google.com/p/fabricate/

Like memoize, fabricate is released under a "New BSD license". fabricate is
copyright (c) 2009 Brush Technology. Full text of the license is here:
    http://code.google.com/p/fabricate/wiki/License

"""

# so you can do "from fabricate import *" to simplify your build script
__all__ = ['ExecutionError', 'shell', 'md5_hasher', 'mtime_hasher', 'Builder',
           'setup', 'run', 'autoclean', 'memoize', 'outofdate', 'main']

# fabricate version number
__version__ = '1.05'

# if version of .deps file has changed, we know to not use it
deps_version = 1

import atexit
import optparse
import os
import platform
import re
import stat
import subprocess
import sys
import tempfile
import time

# So we can use md5func in old and new versions of Python without warnings
try:
    import hashlib
    md5func = hashlib.md5
except ImportError:
    import md5
    md5func = md5.new

# Use json, or pickle on older Python versions if simplejson not installed
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import cPickle
        # needed to ignore the indent= argument for pickle's dump()
        class PickleJson:
            def load(self, f):
                return cPickle.load(f)
            def dump(self, obj, f, indent=None, sort_keys=None):
                return cPickle.dump(obj, f)
        json = PickleJson()

def printerr(message):
    """ Print given message to stderr with a line feed. """
    print >>sys.stderr, message

class ExecutionError(Exception):
    pass

def shell(command, input=None, silent=True):
    """ Run given shell command and return its output as a string.
        - input='string' to pass standard input into the process.
        - input=None (default) to use parent's stdin (keyboard)
        - silent=False to use parent's stdout (i.e. print output
          as-it-comes instead of returning it)
    """
    if input:
        stdin = subprocess.PIPE
    else:
        stdin = None
    if silent:
        stdout = subprocess.PIPE
    else:
        stdout = None
    proc = subprocess.Popen(command, shell=True, stdin=stdin, stdout=stdout,
                            stderr=subprocess.STDOUT)
    if input:
        proc.stdin.write(input)
    output = ''
    if silent:
        output = proc.stdout.read()
    status = proc.wait()
    if status:
        raise ExecutionError('Command %r terminated with exit status %d'
                             % (command.split(' ')[0], status), output, status)
    if silent:
        return output

def access_file(filename):
    """ Access (read a byte from) file to try to update its access time. """
    f = open(filename)
    data = f.read(1)
    f.close()

def file_has_atimes(filename):
    """ Return True if the given filesystem supports access time updates for
        this file. The atime resolution must be at least one day (as it is on
        FAT filesystems). """

    resolution = 24*60*60           # in seconds (worst-case resolution)
    stat = os.stat(filename)
    os.utime(filename, (stat.st_atime-resolution, stat.st_mtime))

    previous = os.stat(filename).st_atime
    access_file(filename)
    return os.stat(filename).st_atime > previous

def has_atimes(paths):
    """ Return True if a file created in each path supports fast atimes.
        Note: for speed, this only tests files created at the top directory
        of each path. A safe assumption in most build environments.
        In the unusual case that any sub-directories are mounted
        on alternate file systems that don't support atimes, the build may
        fail to identify a dependency """

    for path in paths:
        handle, filename = tempfile.mkstemp(dir=path)
        try:
            try:
                f = os.fdopen(handle, 'wb')
            except:
                os.close(handle)
                raise
            try:
                f.write('x')    # need a byte in the file for access test
            finally:
                f.close()
            if not file_has_atimes(filename):
                return False
        finally:
            os.remove(filename)
    return True

def has_strace():
    """ Return True if this system has strace. """
    if platform.system() == 'Windows':
        # even if windows has strace, it's probably a dodgy cygwin one
        return False
    try:
        subprocess.Popen('strace', stderr=subprocess.PIPE)
        return True
    except OSError:
        return False

def _file_times(path, depth, ignoreprefix='.'):
    """ Helper function for file_times().
        Return a dict of file times, recursing directories that don't
        start with ignoreprefix """

    names = os.listdir(path)
    times = {}
    for name in names:
        if ignoreprefix and name.startswith(ignoreprefix):
            continue
        fullname = os.path.join(path, name)
        st = os.stat(fullname)
        if stat.S_ISDIR(st.st_mode):
            if depth > 1:
                times.update(_file_times(fullname, depth-1, ignoreprefix))
        elif stat.S_ISREG(st.st_mode):
            times[fullname] = st.st_atime, st.st_mtime
    return times

def file_times(paths, depth=100, ignoreprefix='.'):
    """ Return a dict of "filepath: (atime, mtime)" entries for each file in
        given paths list. "filepath" is the absolute path, "atime" is the
        access time, "mtime" the modification time.
        Recurse directories that don't start with ignoreprefix """

    times = {}
    for path in paths:
        times.update(_file_times(os.path.abspath(path), depth, ignoreprefix))
    return times

def md5_hasher(filename):
    """ Return MD5 hash of given filename, or None if file doesn't exist. """
    try:
        f = open(filename, 'rb')
        try:
            return md5func(f.read()).hexdigest()
        finally:
            f.close()
    except IOError:
        return None

def mtime_hasher(filename):
    """ Return modification time of file, or None if file doesn't exist. """
    try:
        st = os.stat(filename)
        return repr(st.st_mtime)
    except (IOError, OSError):
        return None

def shrink_path(filename):
    """ Try to shrink a filename for display (remove the leading path if the
        file is relative to the current working directory). """
    cwd = os.getcwd()
    prefix = os.path.commonprefix([cwd, filename])
    if prefix:
        filename = filename[len(prefix)+1:]
    return filename

class Builder(object):
    """ The Builder.

        You can subclass this and override the "runner" function to do what you
        want. For an example, see:
            http://code.google.com/p/fabricate/wiki/HowtoSubclassBuilder

        "runner" is the function used to run commands and generate
        dependencies. It must take a command line string as its argument, and
        return a tuple of (deps, outputs), where deps is a list of abspath'd
        dependency files and outputs a list of abspath'd output files. It
        defaults to a function that just calls smart_runner, which uses
        strace_runner or atimes_runner as it can, automatically.
    """

    def __init__(self, dirs=None, dirdepth=100, ignoreprefix='.',
                 hasher=md5_hasher, depsname='.deps', quiet=False):
        """ Initialise a Builder with the given options.

        "dirs" is a list of paths to look for dependencies (or outputs) in
            if using the strace or atimes runners.
        "dirdepth" is the depth to recurse into the paths in "dirs" (default
            essentially means infinitely). Set to 1 to just look at the
            immediate paths in "dirs" and not recurse at all. This can be
            useful to speed up the atimes_runner if you're building in a large
            tree and you don't care about all of the subdirectories.
        "ignoreprefix" prevents recursion into directories that start with
            prefix.  It defaults to '.' to ignore svn directories.
            Change it to '_svn' if you use _svn hidden directories.
        "hasher" is a function which returns a string which changes when
            the contents of its filename argument changes, or None on error.
            Default is md5_hasher, but can also be mtime_hasher.
        "depsname" is the name of the JSON dependency file to load/save.
        "quiet" set to True tells the builder to not display the commands being
            executed (or other non-error output).
        """
        if dirs is None:
            dirs = ['.']
        self.dirs = [os.path.abspath(path) for path in dirs]
        self.dirdepth = dirdepth
        self.ignoreprefix = ignoreprefix
        self.depsname = depsname
        self.hasher = hasher
        self.quiet = quiet
        self.checking = False

    def echo(self, message):
        """ Print message, but only if builder is not in quiet mode. """
        if not self.quiet:
            print message

    def echo_command(self, command):
        """ Show a command being executed. """
        self.echo(command)

    def echo_delete(self, filename, error=None):
        """ Show a file being deleted. For subclassing Builder and overriding
            this function, the exception is passed in if an OSError occurs
            while deleting a file. """
        if error is None:
            self.echo('deleting %s' % shrink_path(filename))

    def run(self, command, runner=None):
        """ Run given shell command, but only if its dependencies or outputs
            have changed or don't exist. Override default runner if given. """
        if not self.outofdate(command):
            return

        # if just checking up-to-date-ness, set flag and do nothing more
        self.outofdate_flag = True
        if self.checking:
            return

        # use runner to run command and collect dependencies
        self.echo_command(command)
        if runner is None:
            runner = self.runner
        deps, outputs = runner(command)
        if deps is not None or outputs is not None:
            deps_dict = {}
            # hash the dependency inputs and outputs
            for dep in deps:
                hash = self.hasher(dep)
                if hash is not None:
                    deps_dict[dep] = "input-" + hash
            for output in outputs:
                hash = self.hasher(output)
                if hash is not None:
                    deps_dict[output] = "output-" + hash
            self.deps[command] = deps_dict

    def memoize(self, command):
        """ Run given shell command as per run(), but return the status code
            instead of raising an exception if there's an error. """
        try:
            self.run(command)
            return 0
        except ExecutionError, exc:
            message, data, status = exc
            return status

    def outofdate(self, command):
        """ Return True if given command is out of date. Command can either be
            a callable build function or a command line string. """
        if callable(command):
            # command is a build function
            self.checking = True
            self.outofdate_flag = False
            command()
            self.checking = False
            return self.outofdate_flag
        else:
            # command is a command line string
            if command in self.deps:
                for dep, oldhash in self.deps[command].items():
                    assert oldhash.startswith('input-') or \
                           oldhash.startswith('output-'), \
                        "%s file corrupt, do a clean!" % self.depsname
                    oldhash = oldhash.split('-', 1)[1]
                    # make sure this dependency or output hasn't changed
                    newhash = self.hasher(dep)
                    if newhash is None or newhash != oldhash:
                        break
                else:
                    # all dependencies are unchanged
                    return False
            # command has never been run, or one of the dependencies didn't
            # exist or had changed
            return True

    def autoclean(self):
        """ Automatically delete all outputs of this build as well as the .deps
            file. """
        # first build a list of all the outputs from the .deps file
        outputs = []
        for command, deps in self.deps.items():
            outputs.extend(dep for dep, hash in deps.items()
                           if hash.startswith('output-'))
        outputs.append(os.path.abspath(self.depsname))
        self._deps = None
        for output in outputs:
            try:
                os.remove(output)
            except OSError, e:
                self.echo_delete(output, e)
            else:
                self.echo_delete(output)

    @property
    def deps(self):
        """ Lazy load .deps file so that instantiating a Builder is "safe". """
        if not hasattr(self, '_deps') or self._deps is None:
            self.read_deps()
            atexit.register(self.write_deps)
        return self._deps

    def read_deps(self):
        """ Read dependency JSON file into deps object. """
        try:
            f = open(self.depsname)
            try:
                self._deps = json.load(f)
                # make sure the version is correct
                if self._deps.get('.deps_version', 0) != deps_version:
                    printerr('Bad %s dependency file version! Rebuilding.'
                             % self.depsname)
                    self._deps = {}
                self._deps.pop('.deps_version', None)
            finally:
                f.close()
        except IOError:
            self._deps = {}

    def write_deps(self):
        """ Write out deps object into JSON dependency file. """
        if self._deps is None:
            return                      # we've cleaned so nothing to save
        self.deps['.deps_version'] = deps_version
        f = open(self.depsname, 'w')
        try:
            json.dump(self.deps, f, indent=4, sort_keys=True)
        finally:
            f.close()
            self._deps.pop('.deps_version', None)

    def runner(self, command):
        """ The default command runner. Override this in a subclass if you want
            to write your own auto-dependency runner."""
        return self.smart_runner(command)

    def smart_runner(self, command):
        """ Smart command runner that uses strace if it can, otherwise
            access times if available, otherwise always builds. """
        if not hasattr(self, '_smart_runner'):
            if has_strace():
                self._smart_runner = self.strace_runner
            elif has_atimes(self.dirs):
                self._smart_runner = self.atimes_runner
            else:
                self._smart_runner = self.always_runner
        return self._smart_runner(command)

    def _utime(self, filename, atime, mtime):
        """ Call os.utime but ignore permission errors """
        try:
            st = os.utime(filename, (atime, mtime))
        except OSError, e:
            # ignore permission errors -- we can't build with files
            # that we can't access anyway
            if e.errno != 1:
                raise

    def _age_atimes(self, filetimes, age):
        """ Age files' atimes to be at least age old. Only adjust if the given
            filetimes dict says it isn't that old, and return a new dict of
            filetimes with the ages adjusted. """
        adjusted = {}
        now = time.time()
        for filename, entry in filetimes.iteritems():
            if now - entry[0] < age:
                entry = entry[0] - age, entry[1]
                st = self._utime(filename, entry[0], entry[1])
            adjusted[filename] = entry
        return adjusted

    # *** Note: tree walking time can be halved by caching afters for the next
    # command's befores.
    # We can also save lots of utime-ing by not restoring original atimes until
    # after the final build step (because currently we're restoring atimes just
    # to age them again for the next command.)

    def atimes_runner(self, command):
        """ Run command and return its dependencies and outputs, using before
            and after access times to determine dependencies. """
        originals = file_times(self.dirs, self.dirdepth, self.ignoreprefix)
        befores = self._age_atimes(originals, 24*60*60)
        shell(command, silent=False)
        afters = file_times(self.dirs, self.dirdepth, self.ignoreprefix)
        deps = []
        outputs = []
        for name in afters:
            if name in befores:
                # file in both befores+afters, add to outputs if mtime changed
                if afters[name][1] > befores[name][1]:
                    outputs.append(name)
                elif afters[name][0] > befores[name][0]:
                    # otherwise add to deps if atime changed
                    deps.append(name)
            else:
                # file created (in afters but not befores), add as output
                outputs.append(name)

        # Restore atimes of files we didn't access: not for any functional
        # reason -- it's just to preserve the access time for the user's info
        for name in deps:
            originals.pop(name)
        for name in originals:
            original = originals[name]
            if original != afters.get(name, None):
                self._utime(name, original[0], original[1])

        return deps, outputs

    def _is_relevant(self, fullname):
        """ Return True if file is in the dependency search directories. """
        for path in self.dirs:
            if fullname.startswith(path):
                rest = fullname[len(path):]
                # files in dirs starting with ignoreprefix are not relevant
                if os.sep+self.ignoreprefix in os.sep+os.path.dirname(rest):
                    continue
                # files deeper than dirdepth are not relevant
                if rest.count(os.sep) > self.dirdepth:
                    continue
                return True
        return False

    def _do_strace(self, ecmd, outfile, outname):
        """ Run strace on given (escaped) command, sending output to file.
            Return (status code, list of dependencies, list of outputs). """
        calls = 'open,stat64,execve,exit_group,chdir,mkdir,rename'
        shell('strace -f -o %s -e trace=%s /bin/sh -c "%s"' %
              (outname, calls, ecmd), silent=False)

        cwd = os.getcwd()
        status = 0
        deps = set()
        outputs = set()
        for line in outfile:
            is_output = False
            open_match = re.match(r'.*open\("([^"]*)", ([^,)]*)', line)
            stat64_match = re.match(r'.*stat64\("([^"]*)", .*', line)
            execve_match = re.match(r'.*execve\("([^"]*)", .*', line)
            mkdir_match = re.match(r'.*mkdir\("([^"]*)", .*', line)
            rename_match = re.match(r'.*rename\("[^"]*", "([^"]*)"\)', line)

            kill_match = re.match(r'.*killed by.*', line)
            if kill_match:
                return None, None, None

            match = None
            if open_match:
                match = open_match
                mode = match.group(2)
                if 'O_WRONLY' in mode or 'O_RDWR' in mode:
                    # it's an output file if opened for writing
                    is_output = True
            elif stat64_match:
                match = stat64_match
            elif execve_match:
                match = execve_match
            elif mkdir_match:
                match = mkdir_match
            elif rename_match:
                match = rename_match
                # the destination of a rename is an output file
                is_output = True
            if match:
                name = os.path.normpath(os.path.join(cwd, match.group(1)))
                if self._is_relevant(name) and (os.path.isfile(name) or
                   os.path.isdir(name) or not os.path.lexists(name)):
                    if is_output:
                        outputs.add(name)
                    else:
                        deps.add(name)

            match = re.match(r'.*chdir\("([^"]*)"\)', line)
            if match:
                cwd = os.path.normpath(os.path.join(cwd, match.group(1)))

            match = re.match(r'.*exit_group\((.*)\).*', line)
            if match:
                status = int(match.group(1))

        return status, list(deps), list(outputs)

    def strace_runner(self, command):
        """ Run command and return its dependencies and outputs, using strace
            to determine dependencies (by looking at what files are opened or
            modified). """
        ecmd = command
        ecmd = ecmd.replace('\\', '\\\\')
        ecmd = ecmd.replace('"', '\\"')
        exename = command.split()[0]

        handle, outname = tempfile.mkstemp()
        try:
            try:
                outfile = os.fdopen(handle, 'r')
            except:
                os.close(handle)
                raise
            try:
                status, deps, outputs = self._do_strace(ecmd, outfile, outname)
                if status is None:
                    raise ExecutionError(
                        'strace of %r was killed unexpectedly' % exename)
            finally:
                outfile.close()
        finally:
            os.remove(outname)

        if status:
            raise ExecutionError(
                'strace of %r terminated with exit status %d'
                % (exename, status), '', status)
        return list(deps), list(outputs)

    def always_runner(self, command):
        """ Runner that always runs given command, used as a backup in case
            a system doesn't have strace or atimes. """
        shell(command, silent=False)
        return None, None

# default Builder instance, used by helper run() and main() helper functions
default_builder = Builder()
default_command = 'build'

def setup(builder=None, default=None, runner=None, **kwargs):
    """ Setup the default Builder (or an instance of given builder if "builder"
        is not None) with the same keyword arguments as for Builder().
        "default" is the name of the default function to run when the build
        script is run with no command line arguments. """
    global default_builder, default_command
    if builder is not None:
        default_builder = builder()
    if default is not None:
        default_command = default
    default_builder.__init__(**kwargs)
    if runner is not None:
        default_builder.runner = getattr(default_builder, runner)

def run(command):
    """ Run the given command using the default Builder (but only if its
        dependencies have changed). """
    default_builder.run(command)

def autoclean():
    """ Automatically delete all outputs of the default build. """
    default_builder.autoclean()

def memoize(command):
    """ A memoize function compatible with memoize.py. Basically the same as
        run(), but returns the status code instead of raising an exception
        if there's an error. """
    return default_builder.memoize(command)

def outofdate(command):
    """ Return True if given command is out of date and needs to be run. """
    return default_builder.outofdate(command)

def parse_options(usage):
    """ Parse command line options and return parser and args. """
    parser = optparse.OptionParser(usage='Usage: %prog '+usage,
                                   version='%prog '+__version__)
    parser.disable_interspersed_args()
    parser.add_option('-t', '--time', action='store_true',
                      help='use file modification times instead of MD5 sums')
    parser.add_option('-d', '--dir', action='append',
                      help='add DIR to list of relevant directories')
    parser.add_option('-c', '--clean', action='store_true',
                      help='autoclean build outputs before running')
    parser.add_option('-q', '--quiet', action='store_true',
                      help="don't echo commands, only print errors")
    options, args = parser.parse_args()
    default_builder.quiet = options.quiet
    if options.time:
        default_builder.hasher = mtime_hasher
    if options.dir:
        default_builder.dirs.extend(os.path.abspath(d) for d in options.dir)
    if options.clean:
        default_builder.autoclean()
    return parser, options, args

def main(globals_dict=None):
    """ Run the default function or the function(s) named in the command line
        arguments. Call this at the end of your build script. If one of the
        functions returns nonzero, main will exit with the last nonzero return
        value as its status code. """
    if globals_dict is None:
        try:
            globals_dict = sys._getframe(1).f_globals
        except:
            printerr("Your Python version doesn't support sys._getframe(1),")
            printerr("call main(globals()) explicitly")
            sys.exit(1)

    usage = '[options] build script functions to run'
    parser, options, actions = parse_options(usage)
    if not actions:
        actions = [default_command]

    status = 0
    try:
        for action in actions:
            if '(' not in action:
                action = action.strip() + '()'
            name = action.split('(')[0].split('.')[0]
            if name in globals_dict:
                this_status = eval(action, globals_dict)
                if this_status:
                    status = int(this_status)
            else:
                printerr('%r command not defined!' % action)
                sys.exit(1)
    except ExecutionError, exc:
        message, data, status = exc
        printerr(message)
    sys.exit(status)

if __name__ == '__main__':
    # if called as a script, emulate memoize.py -- run() command line
    parser, options, args = parse_options('[options] command line to run')
    status = 0
    if args:
        status = memoize(' '.join(args))
    elif not options.clean:
        parser.print_help()
        status = 1
    # autoclean may have been used
    sys.exit(status)
