import time
import inspect

class TimerException(Exception):
    pass

class Timer():
    def __init__(self):
        self._start_time = None
        self._function = None
        self._context = None
        self._line_no = None
        self._total_time = 0
        with open('timelogs.txt', 'w') as file:
            pass

    def start(self):
        """Start new Timer, takes text arg for method description"""
        if self._start_time != None:
            raise TimerException("Timer is running , use .stop() to stop it")
            
        stack = inspect.stack()
        self._line_no = stack[1][2]
        self._function =  stack[1][3]
        self._context = stack[-1][-2]#[0][:-1]
        self._start_time = time.perf_counter()

    def stop(self):
        """ stop timer and logs elapsed time """
        if self._start_time == None:
            raise TimerException("Timer is not running, use .start() to start it")
        
        elapsed_time = time.perf_counter() - self._start_time
        self._total_time += elapsed_time
        self.time_logger(elapsed_time)
        self._start_time = None

    def time_logger(self, elapsed_time):
        text = '\nContext:\t{0}\nLine no:\t{1}\nMethod Name:\t{2}\nTime elapsed:\t{3} secs\n'
        log_format = text.format(self._context, self._line_no, self._function, elapsed_time)
        with open('timelogs.txt', 'a') as file:
            file.write(log_format)

    def end_time(self):
        if self._start_time != None:
            raise TimerException("Timer is running , use .stop() to stop it")
        text = '\n\n\n Total Time elapsed:\t{0}'.format(self._total_time)
        with open('timelogs.txt', 'a') as file:
            file.write(text)


timer = Timer()