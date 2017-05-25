import sys
import threading
import subprocess

IGNORE = -1
CAPTURE = -2
INTERACT = -3
PIPE = -4
STDIN = -5
STDOUT = -6
STDERR = -7
_UNDEFINED = -8

class CompletedProcess(subprocess.CompletedProcess):
    pass

class CalledProcessError(subprocess.CalledProcessError):
    pass

class CalledProcessSignal(CalledProcessError):
    pass

def _spec_count(template):
    return template.count('%') - 2 * template.count('%%')

def cmdline(template, *args):
    parts = template.split()
    if not args:
        return parts
    for i, part in enumerate(parts):
        spec_count = _spec_count(part)
        part_args = args[:spec_count]
        parts[i] = part % part_args
        args = args[spec_count:]

class Runnable(object):
    def __init__(self):
        self._pipe = None
        self._input = None
        self._output = None
        assert self._interactive_count() <= 1, 'more than one interactive stream, this causes deadlocks'
    
    # builder
    def pipe(self, cmd, *args, **kwargs):
        self._prepare_pipe()
        return Pipe(cmd, *args, _preceding=self, **kwargs)

    # blocking execution
    def run(self, input=None, timeout=None):
        self.start()
        return self.wait(timeout)

    def iterate(self, input=None, timeout=None):
        assert timeout is None, 'not implemented'
        self.start()
        assert self.output
        for line in self.output:
            yield line
        res = self.wait()
        res.check_returncode()

    # non-blocking execution
    def start(self, input=None):
        raise NotImplementedError()

    def poll(self):
        raise NotImplementedError()

    def wait(self, timeout=None):
        raise NotImplementedError()

    # misc
    @property
    def input(self):
        return self._input

    def send(self, data):
        self._input.write(data)
        self._input.flush()

    @property
    def output(self):
        return self._output
    
    @property
    def parts(self):
        raise NotImplementedError()

    def _interactive_count(self):
        raise NotImplementedError()


class Process(Runnable):
    def __init__(self, cmd, *args,
                 stdin=_UNDEFINED, stdout=_UNDEFINED, stderr=_UNDEFINED,
                 **kwargs):
        self._cmd = cmd
        self._args = args
        self._cmdline = cmdline(cmd, *args)

        assert stdin in (_UNDEFINED, STDIN, INTERACT) or hasattr(stdin, 'fileno')
        self._stdin = stdin
        assert stdout not in (STDIN, None)
        self._stdout = stdout
        assert stderr not in (STDIN, None)
        self._stderr = stderr
        assert not (stdout == PIPE and stderr == PIPE)
        self._kwargs = kwargs
        
        self._popen = None
        self._threads = []
        self._captured_stdout = None
        self._captured_stderr = None
        
        self._init_pipe = False
        Runnable.__init__(self)

    def send_signal(self, signal):
        self._popen.send_signal(self, signal)

    def terminate(self):
        self._popen.terminate()

    def kill(self):
        self._popen.kill()

    def _interactive_count(self):
        return sum(1 for arg in ('stdout', 'stderr', 'stdin') if self._kwargs.get(arg) == INTERACT)

    def _prepare_pipe(self):
        self._init_pipe = True
        if self._stdout != PIPE and self._stderr != PIPE:
            assert self._stdout == _UNDEFINED
            self._stdout = PIPE

    def _parallel(self, fn, *args, **kwargs):
        thread = threading.Thread(target=fn, name='spawn_aux', args=args, kwargs=kwargs)
        thread.daemon = True
        thread.start()
        self._threads.append(thread)

    @staticmethod
    def _out_handle(value, default, is_pipe):
        if value == IGNORE:
            return subprocess.DEVNULL
        elif value in (CAPTURE, INTERACT):
            return subprocess.PIPE
        elif value == PIPE:
            assert is_pipe
            return subprocess.PIPE
        elif value == _UNDEFINED:
            return default
        elif value == STDOUT:
            return sys.stdout
        elif value == STDERR:
            return sys.stderr
        else:
            assert hasattr(value, 'fileno'), value
            return value

    def start(self, input=None):
        assert not self._popen, 'already inited'

        if input is not None or self._stdin == INTERACT:
            assert self._stdin in (_UNDEFINED, INTERACT)
            pstdin = subprocess.PIPE
        elif self._stdin == IGNORE:
            pstdin = subprocess.DEVNULL
        elif self._stdin in (STDIN, _UNDEFINED):
            pstdin = sys.stdin
        else:
            assert hasattr(self._stdin, 'fileno'), self._stdin
            pstdin = self._stdin

        pstdout = Process._out_handle(self._stdout, sys.stdout, self._init_pipe)
        pstderr = Process._out_handle(self._stderr, sys.stderr, self._init_pipe)

        self._popen = subprocess.Popen(self._cmdline,
                                       stdin=pstdin, stdout=pstdout, stderr=pstderr,
                                       **self._kwargs)
        if self._stdin == INTERACT:
            self._input = self._popen.stdin
        elif self._stdout == INTERACT:
            self._output = self._popen.stdout
        elif self._stderr == INTERACT:
            self._output = self._popen.stderr
        if self._init_pipe:
            if self._stdout == PIPE:
                self._pipe = self._popen.stdout
            elif self._stderr == PIPE:
                self._pipe = self._popen.stderr

        if input is not None:
            self._parallel(self._popen.stdin.write, input)
        if self._stdout == CAPTURE:
            def capture():
                self._captured_stdout = self._popen.stdout.read()
            self._parallel(capture)
        if self._stderr == CAPTURE:
            def capture():
                self._captured_stderr = self._popen.stderr.read()
            self._parallel(capture)

    def poll(self):
        return self._popen.poll()

    def wait(self, timeout):
        self._popen.wait(timeout)
        for thread in self._threads:
            thread.join()
        return CompletedProcess(args=self._cmdline,
                                returncode=self._popen.returncode,
                                stdout=self._captured_stdout,
                                stderr=self._captured_stderr)
    
    @property
    def parts(self):
        return [self]

class Pipe(Runnable):
    def __init__(self, cmd, *args,
                 _preceding=None,
                 stdin=_UNDEFINED, stdout=_UNDEFINED, stderr=_UNDEFINED,
                 **kwargs):
        if not _preceding:
            assert '|' in cmd, 'not a pipe'
            preceding_cmd, cmd = cmd.rsplit('|', 1)
            spec_count = _spec_count(preceding_cmd)
            preceding_args = args[:spec_count]
            args = args[spec_count:]
            if '|' in preceding_cmd:
                PrecedingType = Pipe
            else:
                PrecedingType = Process
            _preceding = PrecedingType(preceding_cmd, *preceding_args,
                                       stdin=stdin,
                                       stdout=_UNDEFINED,
                                       stderr=stderr,
                                       **kwargs)
            _preceding._prepare_pipe()
        else:
            assert stdin == _UNDEFINED
            
        self._process = Process(cmd, *args,
                                stdin=_UNDEFINED,
                                stdout=stdout,
                                stderr=stderr,
                                **kwargs)
        self._preceding = _preceding
        self._parts = None
        Runnable.__init__(self)

    @property
    def parts(self):
        assert self._parts, 'not inited'
        return self._parts

    def _interactive_count(self):
        return self._preceding._interactive_count() + self._process._interactive_count()

    def _prepare_pipe(self):
        self._process._prepare_pipe()

    def start(self, input=None):
        self._preceding.start(input)
        assert self._preceding._pipe
        self._process._stdin = self._preceding._pipe
        self._process.start()
        self._pipe = self._process._pipe
        self._input = self._preceding._input
        self._output = self._process._output

    def poll(self):
        return self._process.poll()

    def wait(self, timeout=None):
        return self._process.wait(timeout)
