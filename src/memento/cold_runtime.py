"""Single bounded background cold-storage worker; no work on capture requests."""

import logging
import os
import time
from threading import Event, Thread


class ColdRuntime:
    def __init__(self, manager, *, after_days=90, min_bytes=10 * 1024 * 1024):
        if after_days < 1 or min_bytes < 1:
            raise ValueError('Invalid cold-storage retention schedule')
        self.manager = manager
        self.after_days, self.min_bytes = after_days, min_bytes
        self.stop = Event()
        self.thread = None
        self.last_error = None
        self.next_sweep = 0
        self.sweep_failure_count = 0


    def _failed(self, operation, error):
        code = type(error).__name__
        logging.getLogger(__name__).warning('Cold %s failed (%s); data retained', operation, code)
        return code


    def run_once(self):
        if time.monotonic() >= self.next_sweep:
            try:
                result = self.manager.sweep(after_days=self.after_days, min_bytes=self.min_bytes)
                self.sweep_failure_count = result['failure_count']
                self.last_error = 'PendingArchives' if self.sweep_failure_count else None
                self.next_sweep = time.monotonic() + (60 if self.sweep_failure_count else 3600)
            except Exception as error:
                self.last_error = self._failed('sweep', error)
                self.next_sweep = time.monotonic() + 60

    def start(self):
        if self.thread is not None:
            raise RuntimeError('Cold worker already started')
        def run():
            if hasattr(os, 'setpriority'):
                os.setpriority(os.PRIO_PROCESS, 0, 10)
            while not self.stop.is_set():
                self.run_once()
                self.stop.wait(60)
        self.thread = Thread(target=run, name='memento-cold', daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
