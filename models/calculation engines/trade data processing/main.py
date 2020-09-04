import numpy as np
import pandas as pd
import re
from dateutil.parser import parse
import time, os
import inspect
import datetime
import csv
from googletrans import Translator
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb
import trade_data_script as td

db_conv = Flatdbconverter("World trade data processing")

class TimerException(Exception):
    pass

class Timer():
    def __init__(self, t_output_filename=None, txt=False, log_result=True):
        if t_output_filename == None:
            raise TimerException("Output filename is required")
        else: 
            if txt:
                t_output_filename = '{0}_{1}.txt'.format(t_output_filename, self._get_curr_date())
            else:
                t_output_filename = '{0}_{1}.csv'.format(t_output_filename, self._get_curr_date())

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

rest = td.restruct()

timer = Timer('Trade data', txt=True, log_result=False)
translator = Translator()
# self.dp_input['Working H2O_7']

class TDP:
    def __init__(self, use_translator=False, force_use_trans=False):
        timer.start()
        self.use_translator = use_translator
        self.input_filename = "trade data processing inputs.xlsx"
        # self.trade_data_inputs = pd.read_excel(self.input_filename, sheet_name="raw data")
        self.trade_data_inputs = rest["raw"]
        self.force_use_trans = force_use_trans
        self.dp_input = pd.read_excel(self.input_filename, sheet_name="data processing")
        # self.dp_input = rest["lookup"]
        self.trade_data_inputs.fillna(value=0, inplace=True)
        self.db = pd.concat([pd.DataFrame(columns=["Reference Key"]), self.trade_data_inputs], axis=1)
        self.raw_data_and_tiding = None
        self.data_to_be_processed = pd.DataFrame(columns=[])
        self.processed_data = pd.DataFrame(columns=[])
        self.translated_data = {}
        self.final_data = pd.DataFrame(columns=[])
        self.name_dumps = pd.DataFrame(columns=[])
        self.percent_dumps = pd.DataFrame(columns=[])
        timer.stop()

    def calcAll(self):
        self.convert()
        self.is_number_mapper()
        self.get_to_data_to_be_processed()
        # if self.use_translator:
        self.translate_unique_col_names(['Trade Method', 'Production And Sales Country', 'Foreign Ports', 'Currency', 'Declared Quantity Unit', 'Legal Quantity Unit', 'Weight Unit', 'Domestic Receiving And Dispatching Place', 'Transportation Method', 'Package', 'Enterprise Nature', 'Company Name', 'Receiving And Shipping Company Name', 'Chinese Port', 'Country Of Trade', 'Customs Declaration', 'Destination', 'Import And Export Port'])
        self.write_col(0, "Data Index Key")
        self.lookup_translated_data("Trade Method", "Type of Trade", 'Trade Type' )
        self.write_col(2, "Month")
        self.write_col(3, "Date of Declaration")
        self.lookup_translated_data('Production And Sales Country', 'Country Names', 'Producing Country')
        self.lookup_translated_data('Country Of Trade', 'Country Names', 'Trading Country')
        self.lookup_translated_data('Destination', 'Country Names', 'Destination')
        self.lookup_translated_data('Foreign Ports', 'Port of Loading', 'Port of Loading')
        self.write_col(8, "Price Type")
        self.write_col(9, 'USD Gross Amount')
        self.write_col(10, 'USD Price')
        self.write_col(11, 'RMB Gross Amount')
        self.write_col(12, 'RMB Price')
        self.lookup_translated_data('Currency', 'Currency', 'Currency')
        self.write_col(14, 'Gross Amount of Trading Currency')
        self.write_col(15, 'Price of Trading Currency')
        self.lookup_translated_data('Declared Quantity Unit', 'Unit of Declared Quantity', 'Unit of Declared Quantity')
        self.write_col(17, 'Declared Quantity')
        self.write_col(18, 'Legal Quantity')
        self.lookup_translated_data('Legal Quantity Unit', 'Unit of Declared Quantity', 'Unit of Legal Quantity')
        self.write_col(20, 'Gross Weight')
        self.write_col(21, 'Net Weight')
        self.lookup_translated_data('Weight Unit', 'Unit of Declared Quantity' , 'Legal Unit of Measurement')
        self.write_col(23, 'Metric Tons')
        self.lookup_translated_data('Company Name', 'Company Name', 'Company Name')
        self.lookup_translated_data('Receiving And Shipping Company Name', 'Company Name', 'Recepient Company Name', )
        self.lookup_translated_data('Domestic Receiving And Dispatching Place', 'Recepient Location', 'Recepient Location')
        self.lookup_translated_data('Chinese Port', 'Port of Landing', 'Chinese Port')
        self.lookup_translated_data('Customs Declaration', 'Port of Landing', 'Port of Declaration')
        self.lookup_translated_data('Import And Export Port', 'Port of Landing', 'Customs')
        self.lookup_translated_data('Transportation Method', 'Transportation Method', 'Transportation Method' )
        self.write_col(31, 'Name of Vessel')
        self.write_col(32, 'Number of Cargo')
        self.lookup_translated_data('Package', 'Packaging Method', 'Method of Packaging')
        self.write_col(34, 'FOB/CIF Gross USD')
        self.write_col(35, 'FOB/CIF USD Price')
        self.lookup_translated_data('Enterprise Nature', 'Nature of Business', 'Nature of Business')
        self.write_col(37, 'Freight Rate')
        self.write_col(38, 'Currency of Freight Rate')
        self.write_col(39, 'Insurance')
        self.write_col(40, 'Currency of Insurance')
        self.write_col(41, 'Other Charges')
        self.write_col(42, 'Currency of Other Charges')
        if self.use_translator:
            self.lookup_translated_data('Product Specification Model', None, 'Specifications')
        else:
            self.write_col(43, 'Specifications')
        # # AL203
        if self.use_translator:
            arr = ['A\s?L\s?2\s?O\s?3','A\s?L\s?2\s?0\s?3', 'A\s?I\s?2\s?O\s?3', 'A\s?L\s?:\s?2\s?0\s?3', '铝', 'aluminum', 'Al? O?', 'AL2LO3', '三氧化二铝', 'Aluminum oxide', '氧化铝', 'Alumina']
            self.percent_name_helper(arr, "Available Alumina Check")
            self.get_chemical_percent(arr, "Available Alumina Check", "AL2O3%")
        else:
            arr = ['AL2O3','AL203', 'AI2O3', 'AL:203', '铝', 'Al？O？', 'AL2LO3', '三氧化二铝', '氧化铝']
            self.percent_name_helper(arr, "Available Alumina Check")
            self.get_chemical_percent(arr, "Available Alumina Check", "AL2O3%")
        # # SIO2
        if self.use_translator:
            arr = ['SIOO2', 'SIO2', 'Si02', 'si', '氧化硅', '硅', 'Silicon oxide']
            self.percent_name_helper(arr, "Reactive Silica Check")
            self.get_chemical_percent(arr, "Reactive Silica Check", "SIO2%" )
        else:
            arr = ['SIOO2', 'SIO2', 'Si02', 'si', '硅', '氧化硅']
            self.percent_name_helper(arr, "Reactive Silica Check")
            self.get_chemical_percent(arr, "Reactive Silica Check", "SIO2%" )
        # # working date
        if self.use_translator:
            self.percent_name_helper(['\d+\s?([-/(year)年])+\s?\d+\s?([-/(month)月])+\s?\d*\s?([(day)日])?', '\d+\s?([.])+\s?\d+\s?([.])+\s?\d+\s?([(day)日])?', '\d{8}'], "Contract Date", parse_date=True)
        else:
            self.percent_name_helper(['\d+\s?([-/年])+\s?\d+\s?([-/月])+\s?\d*\s?[日]?', '\d+\s?([.])+\s?\d+\s?([.])+\s?\d+\s?[日]?', '\d{8}'], "Contract Date", parse_date=True)
        # # water / moisture
        if self.use_translator:
            arr = ['MOISTURE', '水份', '水', 'water', "H20", "H2O"]
            self.get_chemical_percent(arr, None ,'Working H2O_6')
        else:
            arr = ['MOISTURE', '水份', '水', 'water', "H20", "H2O"]
            self.get_chemical_percent(arr, None ,'Working H2O_6')
       
        self.h2o_from_gross_weight_and_net_weight()
        self.regency_workings()
        self.get_all_h2O()
        self.extract_needed_col()
        self.translate_unique_col_names(['Product Specification Model'], 35)
        self.lookup_translated_data('Product Specification Model', None, 'Specifications')
        self.final_data["Specs and Description"] = self.processed_data['Specifications']

    def convert(self):
        timer.start()
        self.db.loc[:, 'Declaration Date'] = self.db.loc[:, 'Declaration Date'].map(lambda x: x.date())
        for i in range(self.db.shape[0]):            
            if i <= 3234:
                self.db.at[i, "Reference Key"] = f'{self.db.iloc[i, 4].strftime("%Y%m%d")} 1'
            else:
                self.db.at[i, "Reference Key"] = f'{self.db.iloc[i, 4].strftime("%Y%m%d")} ' + (str(int(self.db.loc[i-1, 'Reference Key'][9:]) + 1) if (self.db.iloc[i, 4].month - self.db.iloc[i-1, 4].month) == 0 else '1')
        timer.stop()       

    def is_number_mapper(self):
        timer.start()
        dbc = self.db.copy()
        result = []
        for col in dbc.columns[1:]:
            values = np.array(dbc.loc[:, col])
            values = np.array([a*1 if str(a).isnumeric() else a for a in values])
            dbc.loc[:, col] = values
            result.append(values)
        self.raw_data_and_tiding = dbc
        timer.stop(np.array(result))

    def get_to_data_to_be_processed(self):
        timer.start()
        self.data_to_be_processed.loc[:, "Data Index Key"] = self.raw_data_and_tiding["Reference Key"]
        indexes = [37, 17, 4, 18, 21, 50, 33, 34, 8, 9, 10, 11, 13, 12, 14, 16, 15, 7, 6, 23, 24, 25, 26, 5, 70, 32, 20, 30, 72, 35, 36, 46, 47, 48, 49, 55, 40, 41, 42, 43, 44, 45, 3]
        cols = ["Trade Method", "Month", "Declaration Date", "Production And Sales Country", "Foreign Ports", "Country Of Trade", "Destination", "Transaction Method", "Amount Usd", "Unit Price Usd", "Rmb", "Unit Price Rmb", "Currency", "Transaction Currency Amount", "Transaction Currency Price", "Declared Quantity Unit", "Declared Quantity", "Legal Quantity", "Legal Quantity Unit", "Gross Weight", "Net Weight", "Weight Unit", "Metric Ton", "Company Name", "Receiving And Shipping Company Name", "Domestic Receiving And Dispatching Place", "Chinese Port", "Customs Declaration", "Import And Export Port", "Transportation Method", "Transportation Name", "Number", "Package", "Fob Or Cif Usd", "Fob Or Cif Usd Unit Price", "Enterprise Nature", "Freight", "Freight Currency System", "Insurance Fee", "Insurance Fee Currency System", "Miscellaneous Fee", "Miscellaneous Fee Currency", "Product Specification Model"]
        # rest = self.raw_data_and_tiding.iloc[:, indexes]
        rest = self.raw_data_and_tiding.loc[:, cols]
        self.data_to_be_processed = pd.concat([self.data_to_be_processed, rest], axis=1)
        timer.stop()
    
    def translate_unique_col_names(self, col_names, step=70):
        timer.start()
        cols_names = np.array(list(set(col_names)))
        data = self.data_to_be_processed.copy()
        store = []
        for col in col_names:
            store.extend(data[col].str.strip().unique())
        store = np.array(list(set(store)))
        shaped_data = shape_data(store, step)
        self.translated_data.update(translate_data(shaped_data))
        timer.stop()

    def write_col(self, lookup_col, result_col):
        timer.start()
        result = self.data_to_be_processed.iloc[:, lookup_col]
        self.processed_data[result_col] = result
        # print(result)
        timer.stop()

    def lookup_translated_data(self,col_name, lookup_col, result_col):
        timer.start()
        if not self.use_translator and not self.force_use_trans:
            d = self.data_to_be_processed.copy()
            decoded_col = 'Decoded {0}'.format(lookup_col)
            res = pd.merge(d.loc[:, col_name],self.lookup_input.loc[:, [lookup_col, decoded_col]], left_on=col_name, right_on=lookup_col, how="left" )
            result = np.array(res.loc[:, decoded_col])
            self.processed_data[result_col] = result
            # print(result)
            # print(self.processed_data)
        else:
            d = self.data_to_be_processed.copy()
            res = d[col_name].str.strip().replace(self.translated_data)
            self.processed_data[result_col] = res
        timer.stop()

    def percent_name_helper(self, search_val, result_col, parse_date=False):
        timer.start()
        specs = np.array(self.processed_data.loc[:, 'Specifications']).copy()
        search_val = '|'.join(search_val)
        reg = re.compile(fr'({search_val})', re.IGNORECASE)
        result = []
        count = 0
        errors = []
        for i in np.arange(len(specs)):
            first = reg.findall(specs[i])
            if len(first) > 0:
                if isinstance(first[0], (list, tuple)):
                    if parse_date:
                        date = first[0][0]
                        try:
                            date = parse(date)
                            result.append(date)
                        except Exception:
                            result.append(date)
                    else:
                        result.append(first[0][0])
                else:
                    result.append(first[0])
            else:
                count += 1
                errors.append(specs[i])
                # print(specs[i], i)
                result.append("#VALUE!")   
        # print(count)
        l = pd.DataFrame({result_col: errors})
        self.name_dumps = pd.concat([self.name_dumps, l], axis=1)
        self.processed_data[result_col] = result
        timer.stop(np.array(result))

    def get_chemical_percent(self, values, col_check, result_col):
        timer.start()
        specs = np.array(self.processed_data.loc[:, 'Specifications'])
        check = '|'.join(values)
        # exempt_ind = []
        reg = re.compile(fr'({check})+\D{{,12}}([0-9.]+[-][0-9.]+\s?%|\d+\.\d+\s?%|\d+\s?%|[0-9.]+[-][0-9.]+\s?|\d+\.\d+\s?|\d+\s?)', re.IGNORECASE)
        lookup_arr = None
        errors = []
        if isinstance(col_check, str):
            lookup_arr = self.processed_data[col_check] 
        result = []
        count = 0
        for i in np.arange(len(specs)):
            if isinstance(lookup_arr, str):
                print(lookup_arr)
                if lookup_arr[i] == "#VALUE!":
                    # print(specs[i], i)
                    count += 1
                    errors.append(specs[i])
                    result.append(0)
            else:
                first = reg.findall(specs[i])
                if len(first) == 0:
                    # print(specs[i], i)
                    # exempt_ind.append(i)
                    errors.append(specs[i])
                    count += 1
                    result.append(0)
                else:
                    percent = first[0][1]
                    if '-' in percent:
                        result.append(float(handle_range(percent)))
                        pass
                    else:
                        try:
                            result.append(float(percent[:-1]))
                        except:
                            errors.append(specs[i])
                            count += 1
                            result.append(0)
        # self.processed_data.loc[exempt_ind , "Available Alumina Check"] = "#VALUE!"
        l = pd.DataFrame({result_col: errors})
        self.percent_dumps = pd.concat([self.percent_dumps, l], axis=1)
        self.processed_data[result_col] = result
        timer.stop(np.array(result))

    def h2o_from_gross_weight_and_net_weight(self):
        timer.start()
        gross_weight =  np.array(self.processed_data['Gross Weight'])
        net_weight = np.array(self.processed_data["Net Weight"])
        diff = gross_weight - net_weight
        result = np.array(["#VALUE!" if gross_weight[i] == 0 else (diff[i]/gross_weight[i])*100 for i in np.arange(len(diff))])
        self.processed_data["H2O from Gross Weight & Net Weight"] = result
        timer.stop(result)

    def regency_workings(self):
        timer.start()
        values = ["W-Kalimantan", "CBG", "Bel Air", "Spain Refractory", "Bintan", "W-Kalimantan", "SMB-WAP", "Bel Air", "Awaso", "W-Kalimantan", "Rennell Island", "Weipa", "Juruti", "Bauxite Hills", "Bintan", "Huntly", "SMB-WAP", "Gove", "Rennell Island", "Weipa", "Gove", "Gujarat", "Kuantan"]
        keys = ["关" ,"古" ,"夫" ,"WEIPA" ,"RENN" ,"GOVE" ,"KATOUGOUMA" ,"KWINANA" ,"BINTAN" ,"SKARDON" ,"JURUTI" ,"韦帕" ,"伦内尔" ,"西加" ,"AWASO" ,"VERGA" ,"几内亚/博凯" ,"WEST KAL" ,"TANJUNG" ,"西班牙" ,"BEL AIR" ,"CBG" ,"加里曼丹" ]
        specs = np.array(self.data_to_be_processed.loc[:, 'Product Specification Model'])
        hardcoded = np.array(self.dp_input["Regency workings 2"])
        mapped_values = dict(zip(keys, values[::-1]))
        reg_format = '|'.join(mapped_values.keys())
        reg = re.compile(fr'({reg_format})', re.IGNORECASE)
        result = []
        for i in np.arange(len(specs)):
            search_res = reg.findall(specs[i])
            if len(search_res) > 0:
                result.append(mapped_values[search_res[0].upper()])
            else:
                result.append(hardcoded[i])
        # print(result)
        self.processed_data["Regency"] = result
        timer.stop(np.array(result))
        
    def get_all_h2O(self):
        timer.start()
        h20_7 = np.array(self.dp_input['Working H2O_7'])
        h20_from_gross_weight = np.array(self.processed_data["H2O from Gross Weight & Net Weight"])
        n_h20 = [re.sub(r'[^0-9.]', '-', str(a)) for a in h20_7]
        # print(n_h20)
        result = np.array([h20_7[i] if n_h20[i][0] != '-' else h20_from_gross_weight[i] for i in np.arange(len(h20_from_gross_weight))])
        self.processed_data["H2O"] = result
        df2 = pd.to_numeric(self.processed_data["H2O"],errors='coerce')
        self.processed_data["H2O"] = df2.fillna(self.processed_data["H2O"])
        timer.stop()

    def extract_needed_col(self): 
        self.final_data['Date'] = self.processed_data['Date of Declaration']
        lq = self.processed_data['Legal Quantity']
        tonnage = self.processed_data['Unit of Legal Quantity']
        self.final_data['Tonnage (DMT)'] = [lq[i]/1000 if str(tonnage[i]).lower() == 'kg' else lq[i] for i in np.arange(len(lq))]
        self.final_data['Origin'] = self.processed_data['Producing Country']
        self.final_data['Regency'] = self.processed_data['Regency']
        self.final_data['Port'] = self.processed_data['Chinese Port']
        self.final_data['Customer'] = self.processed_data['Company Name']
        self.final_data['Total Alumina %'] = self.processed_data['AL2O3%']
        self.final_data['Total Silica %'] = self.processed_data['SIO2%']
        self.final_data['Moisture %'] = self.processed_data['H2O']
        self.final_data['Price Type'] = self.processed_data['Price Type']
        self.final_data['Declared Price (US$dmt)'] = self.processed_data['USD Price']
        self.final_data['Transportation Method'] = self.processed_data['Transportation Method']
        self.final_data['Specs and Description'] = self.processed_data['Specifications']
        self.final_data['Contract Date'] = self.processed_data["Contract Date"]
        df2 = self.processed_data.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce'))
        cols = ['Date', 'Origin', 'Regency', 'Price Type', 'Declared Price (US$dmt)', 'Total Alumina %', 'Total Silica %', 'Moisture %', "Contract Date", 'Tonnage (DMT)', 'Customer', 'Port', 'Specs and Description']
        f1 = self.final_data.copy().loc[:, cols]
        cols[1], cols[4], cols[5], cols[6], cols[7], cols[9], cols[10], cols[11] = 'Country of Origin', 'Declared Price USD/dmt', 'Aa%', 'SiO2%', 'H2O%', 'Tonnage', 'Customer', 'Importing Port'
        print(cols)
        f1.columns = cols
        self.final_data_1 = f1
        self.processed_data = df2.fillna(self.processed_data)


    def save(self):
        timer.start()
        if os.path.exists("outputs"):
            pass
        else:
            os.mkdir("outputs")
        name = "outputs/trade_data_processing_output.xlsx"
        writer = pd.ExcelWriter(name)
        
        self.final_data = self.final_data[1:]
        self.processed_data = self.processed_data[1:]
        self.final_data_1 = self.final_data_1[1:]

        # self.raw_data_and_tiding.to_excel(writer,sheet_name="Raw data and tiding" , encoding='utf-8', index=False)
        self.processed_data.to_excel(writer, sheet_name="Processed data", encoding='utf-8', index=False)
        self.final_data.to_excel(writer, sheet_name="Final data", encoding="utf-8", index=False)
        self.final_data_1.to_excel(writer, sheet_name="Final data 1", encoding="utf-8", index=False)
        # self.final_data.to_csv("Final Processed data.csv", encoding='utf-8', index=False)

        # dumps
        self.name_dumps.to_csv("name_dumps.csv", encoding="utf-8", index=False)
        self.percent_dumps.to_csv("percent_dumps.csv", encoding="utf-8", index=False)

        dblist = [
            db_conv.single_year_mult_out(self.processed_data, "Processed data"),
            db_conv.single_year_mult_out(self.final_data, "Final Processed output"),
            db_conv.single_year_mult_out(self.final_data_1, "Final Processed output 1")
        ]
        snapshot_output_data = pd.concat(dblist, ignore_index=True)
        snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]
        snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
        uploadtodb.upload(snapshot_output_data)

        writer.save()
        timer.stop(end=True)


def is_date(string, fuzzy=False):
    try:
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False

def shape_data(data, max_col):
    # print(data)
    mul = max_col * (len(data) // max_col)
    shaped_data = np.reshape(data[:mul], (-1, max_col))
    result = [*shaped_data] 
    result.append(data[mul:])
    return result

def translate_data(arr):
    arr = np.array(arr)
    result = {}
    for t in arr:
        encoder = '_'
        t = np.array(t)
        t = [a.strip() for a in t ]
        merged = encoder.join(t)
        k = translator.translate(merged, dest='en')
        a = np.array(k.text.split(encoder))
        k = dict(zip(t,a))
        result.update(k)
    return result

def handle_range(r):
    r = r[:-1].split('-')
    try:
        return (int(r[0]) + int(r[1]))/ 2
    except ValueError:
        return float(r[0])
    return r


if __name__ == "__main__":
    tdp = TDP(force_use_trans=True)
    tdp.calcAll()
    tdp.save()