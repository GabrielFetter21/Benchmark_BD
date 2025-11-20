import psutil
import threading
import time
import statistics

class ResourceMonitor:
    def __init__(self, interval=0.5):
        self.interval = interval
        self.cpu_samples = []
        self.mem_samples = []
        self.running = False

    def _monitor(self):
        while self.running:
            self.cpu_samples.append(psutil.cpu_percent(interval=None))
            self.mem_samples.append(psutil.virtual_memory().used / (1024 ** 2))
            time.sleep(self.interval)

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._monitor)
        self.thread.start()

    def stop(self):
        self.running = False
        self.thread.join()

    def get_stats(self):
        if not self.cpu_samples:
            return {"cpu_avg": 0, "mem_avg": 0}
        return {
            "cpu_avg": round(statistics.mean(self.cpu_samples), 2),
            "mem_avg": round(statistics.mean(self.mem_samples), 2)
        }