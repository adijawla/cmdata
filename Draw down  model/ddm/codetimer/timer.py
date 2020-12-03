
import time
import inspect
import datetime
import csv


class TimerException(Exception):
    pass

class Timer():
    def __init__(self, t_output_filename=None, txt=False, log_result=True):
        if t_output_filename == None:
            raise TimerException("Output filename is required")
        else: 
            if txt:
                t_output_filename = './{0}_{1}.txt'.format(t_output_filename, self._get_curr_date())
            else:
                t_output_filename = './{0}_{1}.csv'.format(t_output_filename, self._get_curr_date())

        self.txt = txt
        self.out_file = t_output_filename
        self._start_time = None
        self._start_asctime = None
        self._function = None
        self._line_no = None
        self._total_time = 0
        self._object_name = None
        self._args_list = None
        self._arg_values = None
        self.log_result = log_result
        self._store = []
        with open('{0}'.format(self.out_file), 'w', encoding="utf-8") as file:
            if not txt:
                fields = ['Module Name', 'Method Name', 'Start time', 'End time', 'Time elapsed', 'Line no', 'Arguments', 'Function Call count', 'Method Outputs']
                csvwriter = csv.writer(file)
                csvwriter.writerow(fields)


    def _get_curr_date(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    def start(self):
        """Start new Timer, takes text arg for method description"""
        if self._start_time != None:
            raise TimerException("Timer is running , use .stop() to stop it")
            
        stack = inspect.stack()
        self._line_no = stack[1][2]
        self._function =  stack[1][3]
        self._args_list = inspect.getargvalues(stack[1][0])[0]
        class_name = str(inspect.getargvalues(stack[1][0])[3]['self'])
        i = class_name.find('.')
        j = class_name.find('at')
        self._object_name = class_name[i+1:j]
        args_values = inspect.getargvalues(stack[1][0])[3]
        del args_values['self']
        self._arg_values =  list(args_values.items())
        self._start_time = time.perf_counter()
        self._start_asctime = self._get_curr_date()
        self._store.append(self._function)

    def stop(self,*results, end=False, reset=False):
        # print(results)
        if isinstance(results, (list, tuple)):
            results = [(' ~ ').join(map(str, a)) for a in results]
        """ stop timer and logs elapsed time """
        if self._start_time == None:
            raise TimerException("Timer is not running, use .start() to start it")
        
        elapsed_time = time.perf_counter() - self._start_time
        self._total_time += elapsed_time
        if self.txt:
            self.txt_time_logger(elapsed_time, self._get_curr_date(), results)
        else:
            self.csv_time_logger(elapsed_time, self._get_curr_date(), results)
        self._start_time = None
        if reset:
            self.end_time()
            self._total_time = 0
        if end:
            self.end_time()

    def txt_time_logger(self, elapsed_time, end_time, results):
        count = self._store.count(self._function)
        text = '\n\nModule name:\t{0}\nMethod Name:\t{1}\nStart time:\t{2}\nEnd Time:\t{3}\nTime elapsed:\t{4} secs\nLine no:\t{5}\nArguments:\t{6}\nFunction call count:\t{7}\nMETHOD OUTPUTS'
        log_format = text.format(self._object_name, self._function, self._start_asctime, end_time , elapsed_time, self._line_no, self._arg_values, count )
        
        with open('{0}'.format(self.out_file), 'a', encoding="utf-8") as file:
            file.write(log_format)
            if self.log_result:
                for r in results:
                    file.write('\n{0}'.format(r))

    def csv_time_logger(self, elapsed_time, end_time, results):
        count = self._store.count(self._function)
        row = [self._object_name, self._function, self._start_asctime, end_time , elapsed_time, self._line_no, self._arg_values, count]
        with open('{0}'.format(self.out_file), 'a') as file:
            csvwriter = csv.writer(file)
            if self.log_result:
                for r in results:
                    row.append(r)
            csvwriter.writerow(row)


    def end_time(self):
        if self._start_time != None:
            raise TimerException("Timer is running , use .stop() to stop it")
        text = '\n\n\n Total Time elapsed:\t{0}'.format(self._total_time)
        with open('{0}'.format(self.out_file), 'a') as file:
            file.write(text)
