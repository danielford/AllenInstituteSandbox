#!/mnt/miniconda/bin/python

import time
import os
import sys
import signal
import socket
from datetime import datetime
from contextlib import closing
from daemon import DaemonContext
from daemon.pidfile import PIDLockFile
from dask_yarn import YarnCluster

# from https://stackoverflow.com/a/45690594
def __find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

if __name__ == '__main__':
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    home_dir = '/home/hadoop'
    logfile = open(os.path.join(home_dir, script_name + '.log'), 'a')
    pidfile = os.path.join(home_dir, script_name + '.pid')

    if len(sys.argv) > 1 and sys.argv[1].lower() == 'stop':
        with open(pidfile, 'r') as f:
            pid = int(f.read().strip())
            print('Sending SIGTERM to pid %d' % pid)
            os.kill(pid, signal.SIGTERM)
            sys.exit()

    signals_received = {}
    def shutdown_handler(signum, frame):
        signals_received[signum] = True

    scheduler_port = __find_free_port()
    print('YarnCluster scheduler port: %d' % scheduler_port)
    sys.stdout.flush()

    with DaemonContext(
        working_directory=home_dir,
        pidfile=PIDLockFile(pidfile),
        signal_map={
            signal.SIGTERM: shutdown_handler,
            signal.SIGHUP: None,
        },
        stdout=logfile,
        stderr=logfile,
        umask=0o022
    ):
        sys.stdout.reconfigure(line_buffering=True)

        with YarnCluster(
            environment='environment.tar.gz', 
            deploy_mode='local', 
            port=scheduler_port
        ) as cluster:
            cluster.adapt()

            while True:
                time.sleep(1)

                if signals_received.get(signal.SIGTERM, False):
                    print('%s: Received SIGTERM, shutting down cluster' % datetime.now())
                    cluster._adaptive.stop()
                    break
        
        print('YarnCluster context closed, exiting cleanly')
