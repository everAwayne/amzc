import os
import sys


def daemonize(stdout=os.devnull, stderr=os.devnull):
    try:
        if os.fork() > 0:
            sys.exit(0)
    except OSError as err:
        raise RuntimeError('fork #1 faild: {0} ({1})\n'.format(e.errno, e.strerror))
    os.chdir('/')
    os.setsid()
    os.umask(0o22)

    try:
        if os.fork() > 0:
            sys.exit(0)
    except OSError as err:
        raise RuntimeError('fork #2 faild: {0} ({1})\n'.format(e.errno, e.strerror))
    sys.stdout.flush()
    sys.stderr.flush()
    with open(os.devnull, 'rb', 0) as f:
        os.dup2(f.fileno(), sys.stdin.fileno())
    with open(stdout, 'ab', 0) as f:
        os.dup2(f.fileno(), sys.stdout.fileno())
    with open(stderr, 'ab', 0) as f:
        os.dup2(f.fileno(), sys.stderr.fileno())
