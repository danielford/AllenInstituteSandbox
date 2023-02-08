#!/mnt/miniconda/bin/python

import time
import os
import sys
import signal
from datetime import datetime
from daemon import DaemonContext
from daemon.pidfile import PIDLockFile
from dask_yarn import YarnCluster

def wait_for_process_exit(pid, timeout_sec=60):
    def is_running(pid):
        try:
            os.kill(pid, 0)
            return True
        except OSError as err:
            return False

    start_time = time.time()
    while is_running(pid):
        time.sleep(0.25)
        if time.time() - start_time > timeout_sec:
            raise RuntimeError('Waited %ds for pid %d to exit' % (timeout_sec, pid))

if __name__ == '__main__':
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    home_dir = '/home/hadoop'
    logfile = open(os.path.join(home_dir, script_name + '.log'), 'w')
    pidfile = os.path.join(home_dir, script_name + '.pid')
    stop_requested = len(sys.argv) > 1 and sys.argv[1].lower() == 'stop'

    if os.path.exists(pidfile) or stop_requested:
        with open(pidfile, 'r') as f:
            pid = int(f.read().strip())
            print('Sending SIGTERM to pid %d' % pid)
            os.kill(pid, signal.SIGTERM)
            wait_for_process_exit(pid)

    if stop_requested:
        sys.exit()

    signals_received = {}
    def shutdown_handler(signum, frame):
        signals_received[signum] = True

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
        ) as cluster:
            cluster.adapt()

            while True:
                time.sleep(1)

                if signals_received.get(signal.SIGTERM, False):
                    print('%s: Received SIGTERM, shutting down cluster' % datetime.now())
                    cluster._adaptive.stop()
                    break
        
        print('YarnCluster context closed, exiting cleanly')
