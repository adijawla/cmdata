import time, os
import numpy as np
import pandas as pd
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb

g_flat = Flatdbconverter("Guinea FOB Cost Model")
override_store = {}
try:
    snaps = pd.read_csv('snapshot_output_data.csv')
    override_rows = snaps.loc[snaps['override_value'] == 1]
    print(override_rows.values)
    for v in override_rows.values:
        override_store[v[4]+ '_' + str(v[5])] = float(v[6])
    print(override_store)
except FileNotFoundError:
    pass

class GuineaFOB:
    def __init__(self):
        self.sheet1  = pd.read_excel("Guinea FOB Cost Model inputs.xlsx", sheet_name="Sheet1")
        self.db      = pd.read_excel("Guinea FOB Cost Model inputs.xlsx", sheet_name="Sheet2")
        self.lookup1 = pd.read_excel("Guinea FOB Cost Model inputs.xlsx", sheet_name="Clear and grub difficulty")
        self.lookup2 = pd.read_excel("Guinea FOB Cost Model inputs.xlsx", sheet_name="General Efficiency")

    def calcs(self):
        def vlookup(search, lookup_arr, target):
            v = lookup_arr == search
            return target[v].tolist()[0]

        for col in self.db.columns[2:]:
            self.db.at[6, col]         = self.sheet1.loc[0, "FX rate GNF:US$"]
            self.db.at[9, col]         = self.db.loc[3, col] / self.db.loc[4, col] / self.db.loc[6, col] * self.db.loc[7, col] * self.db.loc[8, col]
            self.db.at[27, col]        = 0 if self.db.loc[22, col] == 0 else self.db.loc[23, col] / self.db.loc[22, col]
            self.db.at[28, col]        = self.db.loc[22, col] * self.db.loc[24, col]
            self.db.at[29, col]        = self.db.loc[23, col] * self.db.loc[24, col]
            self.db.at[30, col]        = self.db.loc[28, col] + self.db.loc[29, col]
            self.db.at[31, col]        = 0 if self.db.loc[22, col] == 0 else 1
            self.db.at[32, col]        = min([0.25, 2/self.db.loc[23, col]])
            self.db.at[34, col]        = vlookup(self.db.loc[33, col], self.lookup2.loc[:, "General Efficiency"], self.lookup2.loc[:, "Multiplier"])
            self.db.at[36, col]        = vlookup(self.db.loc[35, col], self.lookup1.loc[:, "Clear and grub difficulty"], self.lookup1.loc[:, "Multiplier"])
            self.db.at[44, col]        = self.db.loc[40, col] * self.db.loc[34, col] * self.db.loc[36, col] * self.db.loc[37, col]
            self.db.at[45, col]        = self.db.loc[41, col] * self.db.loc[34, col] * self.db.loc[36, col] * self.db.loc[37, col]
            self.db.at[46, col]        = self.db.loc[42, col] * self.db.loc[34, col] * self.db.loc[36, col] * self.db.loc[37, col]
            self.db.at[48, col]        = self.db.loc[44, col] * self.db.loc[5, col]
            self.db.at[49, col]        = self.db.loc[45, col] * self.db.loc[9, col]
            self.db.at[50, col]        = self.db.loc[46, col]
            self.db.at[51, col]        = self.db.loc[29, col]
            self.db.at[53, col]        = self.db.loc[48, col] / self.db.loc[51, col] * self.db.loc[38, col]
            self.db.at[54, col]        = self.db.loc[49, col] / self.db.loc[51, col] * self.db.loc[38, col]
            self.db.at[55, col]        = self.db.loc[50, col] / self.db.loc[51, col] * self.db.loc[38, col]
            self.db.at[57, col]        = self.db.loc[53:55, col].sum()

            self.db.at[65, col]        = self.db.loc[60, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[66, col]        = self.db.loc[61, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[67, col]        = self.db.loc[62, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[68, col]        = self.db.loc[63, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[70, col]        = self.db.loc[65, col] * self.db.loc[5, col]
            self.db.at[71, col]        = self.db.loc[66, col] * self.db.loc[9, col]
            self.db.at[72, col]        = self.db.loc[67, col]
            self.db.at[73, col]        = self.db.loc[68, col]
            self.db.at[75, col]        = self.db.loc[70, col] * self.db.loc[58, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[76, col]        = self.db.loc[71, col] * self.db.loc[58, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[77, col]        = self.db.loc[72, col] * self.db.loc[58, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[78, col]        = self.db.loc[73, col] * self.db.loc[58, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[79, col]        = self.db.loc[75:78, col].sum()

            self.db.at[80, col]        = self.db.loc[58, col]
            self.db.at[87, col]        = self.db.loc[83, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[88, col]        = self.db.loc[84, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[89, col]        = self.db.loc[85, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[91, col]        = self.db.loc[87, col] * self.db.loc[5, col] * (200 / self.db.loc[81, col])
            self.db.at[92, col]        = self.db.loc[88, col] * self.db.loc[9, col] * (200 / self.db.loc[81, col])
            self.db.at[93, col]        = self.db.loc[89, col] * (200 / self.db.loc[81, col])
            self.db.at[95, col]        = self.db.loc[91, col] * self.db.loc[80, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[96, col]        = self.db.loc[92, col] * self.db.loc[80, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[97, col]        = self.db.loc[93, col] * self.db.loc[80, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[98, col]        = self.db.loc[95:97, col].sum()

            self.db.at[99, col]        = self.db.loc[80, col]
            self.db.at[107, col]       = self.db.loc[101, col]/self.db.loc[103, col]
            self.db.at[108, col]       = (35/self.db.loc[100, col]) ** 0.312
            self.db.at[109, col]       = (62/self.db.loc[101, col]) ** 0.2
            self.db.at[111, col]       = self.db.loc[104, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[112, col]       = self.db.loc[105, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[113, col]       = self.db.loc[106, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[115, col]       = self.db.loc[111, col] * self.db.loc[108, col] * self.db.loc[109, col]
            self.db.at[116, col]       = self.db.loc[112, col] * self.db.loc[108, col] * self.db.loc[109, col]
            self.db.at[117, col]       = self.db.loc[113, col] * self.db.loc[108, col] * self.db.loc[109, col]
            self.db.at[119, col]       = self.db.loc[115, col] * self.db.loc[101, col] * 2 * self.db.loc[5, col]
            self.db.at[120, col]       = self.db.loc[116, col] * self.db.loc[107, col] * 2 * self.db.loc[9, col]
            self.db.at[121, col]       = self.db.loc[117, col] * self.db.loc[107, col]
            self.db.at[123, col]       = self.db.loc[119, col] * self.db.loc[99, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[124, col]       = self.db.loc[120, col] * self.db.loc[99, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[125, col]       = self.db.loc[121, col] * self.db.loc[99, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[126, col]       = self.db.loc[123:125, col].sum()

            self.db.at[134, col]       = self.db.loc[130, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[135, col]       = self.db.loc[131, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[136, col]       = self.db.loc[132, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[138, col]       = self.db.loc[134, col] * self.db.loc[5, col] * 651 / self.db.loc[128, col]
            self.db.at[139, col]       = self.db.loc[135, col] * self.db.loc[9, col] * 651 / self.db.loc[128, col]
            self.db.at[140, col]       = self.db.loc[136, col] * 651 / self.db.loc[128, col]
            self.db.at[142, col]       = self.db.loc[138, col] * self.db.loc[127, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[143, col]       = self.db.loc[139, col] * self.db.loc[127, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[144, col]       = self.db.loc[140, col] * self.db.loc[127, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[145, col]       = self.db.loc[142:144, col].sum()
            
            self.db.at[146, col]       = self.db.loc[127, col]
            self.db.at[153, col]       = self.db.loc[149, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[154, col]       = self.db.loc[150, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[155, col]       = self.db.loc[151, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[157, col]       = self.db.loc[153, col] * self.db.loc[5, col] * 200 / self.db.loc[147, col]
            self.db.at[158, col]       = self.db.loc[154, col] * self.db.loc[9, col] * 200 / self.db.loc[147, col]
            self.db.at[159, col]       = self.db.loc[155, col] * 200 / self.db.loc[147, col]
            self.db.at[161, col]       = self.db.loc[157, col] * self.db.loc[146, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[162, col]       = self.db.loc[158, col] * self.db.loc[146, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[163, col]       = self.db.loc[159, col] * self.db.loc[146, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[164, col]       = self.db.loc[161:163, col].sum()

            self.db.at[165, col]       = self.db.loc[146, col]
            self.db.at[173, col]       = self.db.loc[167, col]/self.db.loc[169, col]
            self.db.at[174, col]       = (35/self.db.loc[166, col]) ** 0.312
            self.db.at[175, col]       = (62/self.db.loc[167, col]) ** 0.2
            self.db.at[177, col]       = self.db.loc[170, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[178, col]       = self.db.loc[171, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[179, col]       = self.db.loc[172, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[181, col]       = self.db.loc[177, col] * self.db.loc[174, col] * self.db.loc[175, col]
            self.db.at[182, col]       = self.db.loc[178, col] * self.db.loc[174, col] * self.db.loc[175, col]
            self.db.at[183, col]       = self.db.loc[179, col] * self.db.loc[174, col] * self.db.loc[175, col]
            self.db.at[185, col]       = self.db.loc[181, col] * self.db.loc[167, col] * 2 * self.db.loc[5, col]
            self.db.at[186, col]       = self.db.loc[182, col] * self.db.loc[173, col] * 2 * self.db.loc[9, col]
            self.db.at[187, col]       = self.db.loc[183, col] * self.db.loc[173, col]
            self.db.at[189, col]       = self.db.loc[185, col] * self.db.loc[165, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[190, col]       = self.db.loc[186, col] * self.db.loc[165, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[191, col]       = self.db.loc[187, col] * self.db.loc[165, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[192, col]       = self.db.loc[189:191, col].sum()
            
            self.db.at[194, col]       = (self.db.loc[75, col] + self.db.loc[95, col] + self.db.loc[123, col]) * (1 - self.db.loc[2, col]) + (self.db.loc[142, col] + self.db.loc[161, col] + self.db.loc[189, col]) * self.db.loc[2, col]
            self.db.at[195, col]       = (self.db.loc[76, col] + self.db.loc[96, col] + self.db.loc[124, col]) * (1 - self.db.loc[2, col]) + (self.db.loc[143, col] + self.db.loc[162, col] + self.db.loc[190, col]) * self.db.loc[2, col]
            self.db.at[196, col]       = (self.db.loc[77, col] + self.db.loc[97, col] + self.db.loc[125, col]) * (1 - self.db.loc[2, col]) + (self.db.loc[144, col] + self.db.loc[163, col] + self.db.loc[191, col]) * self.db.loc[2, col]
            self.db.at[197, col]       = self.db.loc[78, col] * (1 - self.db.loc[2, col])
            self.db.at[198, col]       = self.db.loc[194:197, col].sum()

            self.db.at[206, col]       = self.db.loc[201, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[207, col]       = self.db.loc[202, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[208, col]       = self.db.loc[203, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[209, col]       = self.db.loc[204, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[211, col]       = self.db.loc[206, col] * self.db.loc[5, col]
            self.db.at[212, col]       = self.db.loc[207, col] * self.db.loc[9, col]
            self.db.at[213, col]       = self.db.loc[208, col]
            self.db.at[214, col]       = self.db.loc[209, col]
            self.db.at[216, col]       = (1 if self.db.loc[11, col].lower() == "no" else 2) * self.db.loc[32, col] * self.db.loc[211, col]
            self.db.at[217, col]       = (1 if self.db.loc[11, col].lower() == "no" else 2) * self.db.loc[32, col] * self.db.loc[212, col]
            self.db.at[218, col]       = (1 if self.db.loc[11, col].lower() == "no" else 2) * self.db.loc[32, col] * self.db.loc[213, col]
            self.db.at[221, col]       = (self.db.loc[211, col] + self.db.loc[216, col]) * self.db.loc[199, col]
            self.db.at[222, col]       = (self.db.loc[212, col] + self.db.loc[217, col]) * self.db.loc[199, col]
            self.db.at[223, col]       = (self.db.loc[213, col] + self.db.loc[218, col]) * self.db.loc[199, col]
            self.db.at[224, col]       = (self.db.loc[214, col] + self.db.loc[219, col]) * self.db.loc[199, col]
            self.db.at[225, col]       = self.db.loc[221:224, col].sum()
            
            self.db.at[233, col]       = self.db.loc[229, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[234, col]       = self.db.loc[230, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[235, col]       = self.db.loc[231, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[237, col]       = self.db.loc[233, col] * self.db.loc[5, col] * 200 / self.db.loc[227, col]
            self.db.at[238, col]       = self.db.loc[234, col] * self.db.loc[9, col] * 200 / self.db.loc[227, col]
            self.db.at[239, col]       = self.db.loc[235, col] * 200 / self.db.loc[227, col]
            self.db.at[241, col]       = self.db.loc[233, col] * self.db.loc[226, col]
            self.db.at[242, col]       = self.db.loc[234, col] * self.db.loc[226, col]
            self.db.at[243, col]       = self.db.loc[235, col] * self.db.loc[226, col]
            self.db.at[244, col]       = self.db.loc[241:243, col].sum()

            self.db.at[245, col]       = self.db.loc[226, col]
            self.db.at[253, col]       = self.db.loc[247, col]/self.db.loc[249, col]
            self.db.at[254, col]       = (35/self.db.loc[246, col]) ** 0.312
            self.db.at[255, col]       = (62/self.db.loc[247, col]) ** 0.2
            self.db.at[257, col]       = self.db.loc[250, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[258, col]       = self.db.loc[251, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[259, col]       = self.db.loc[252, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[261, col]       = self.db.loc[257, col] * self.db.loc[254, col] * self.db.loc[255, col]
            self.db.at[262, col]       = self.db.loc[258, col] * self.db.loc[254, col] * self.db.loc[255, col]
            self.db.at[263, col]       = self.db.loc[259, col] * self.db.loc[254, col] * self.db.loc[255, col]
            self.db.at[265, col]       = self.db.loc[261, col] * self.db.loc[247, col] * 2 * self.db.loc[5, col]
            self.db.at[266, col]       = self.db.loc[262, col] * self.db.loc[253, col] * 2 * self.db.loc[9, col]
            self.db.at[267, col]       = self.db.loc[263, col] * self.db.loc[253, col]
            self.db.at[269, col]       = self.db.loc[265, col] * self.db.loc[245, col]
            self.db.at[270, col]       = self.db.loc[266, col] * self.db.loc[245, col]
            self.db.at[271, col]       = self.db.loc[267, col] * self.db.loc[245, col]
            self.db.at[272, col]       = self.db.loc[269:271, col].sum()

            self.db.at[273, col]       = self.db.loc[226, col]
            self.db.at[275, col]       = 0.50
            self.db.at[276, col]       = 0.06
            self.db.at[277, col]       = 0.45
            self.db.at[279, col]       = self.db.loc[275, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[280, col]       = self.db.loc[276, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[281, col]       = self.db.loc[277, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[283, col]       = self.db.loc[279, col] * self.db.loc[5, col]
            self.db.at[284, col]       = self.db.loc[280, col] * self.db.loc[9, col]
            self.db.at[285, col]       = self.db.loc[281, col]
            self.db.at[287, col]       = self.db.loc[283, col] * self.db.loc[273, col]
            self.db.at[288, col]       = self.db.loc[284, col] * self.db.loc[273, col]
            self.db.at[289, col]       = self.db.loc[285, col] * self.db.loc[273, col]
            self.db.at[290, col]       = self.db.loc[287:289, col].sum()

            self.db.at[298, col]       = self.db.loc[294, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[299, col]       = self.db.loc[295, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[300, col]       = self.db.loc[296, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[302, col]       = self.db.loc[298, col] * self.db.loc[5, col] * 651 / self.db.loc[292, col]
            self.db.at[303, col]       = self.db.loc[299, col] * self.db.loc[9, col] * 651 / self.db.loc[292, col]
            self.db.at[304, col]       = self.db.loc[300, col] * 651 / self.db.loc[292, col]
            self.db.at[306, col]       = self.db.loc[302, col] * self.db.loc[291, col]
            self.db.at[307, col]       = self.db.loc[303, col] * self.db.loc[291, col]
            self.db.at[308, col]       = self.db.loc[304, col] * self.db.loc[291, col]
            self.db.at[309, col]       = self.db.loc[306:308, col].sum()

            self.db.at[311, col]       = (self.db.loc[221, col] + self.db.loc[241, col] + self.db.loc[269, col] + self.db.loc[287, col]) * (1 - self.db.loc[2, col]) + self.db.loc[306, col] * self.db.loc[2, col]
            self.db.at[312, col]       = (self.db.loc[222, col] + self.db.loc[242, col] + self.db.loc[270, col] + self.db.loc[288, col]) * (1 - self.db.loc[2, col]) + self.db.loc[307, col] * self.db.loc[2, col]
            self.db.at[313, col]       = (self.db.loc[223, col] + self.db.loc[243, col] + self.db.loc[271, col] + self.db.loc[289, col]) * (1 - self.db.loc[2, col]) + self.db.loc[308, col] * self.db.loc[2, col]
            self.db.at[314, col]       = self.db.loc[224, col] * (1 - self.db.loc[2, col])
            self.db.at[315, col]       = self.db.loc[311:314, col].sum()
            
            self.db.at[323, col]       = self.db.loc[319, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[324, col]       = self.db.loc[320, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[325, col]       = self.db.loc[321, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[327, col]       = self.db.loc[323, col] * self.db.loc[5, col] * 200 / self.db.loc[317, col]
            self.db.at[328, col]       = self.db.loc[324, col] * self.db.loc[9, col] * 200 / self.db.loc[317, col]
            self.db.at[329, col]       = self.db.loc[325, col] * 200 / self.db.loc[317, col]
            self.db.at[331, col]       = self.db.loc[327, col] * self.db.loc[316, col]
            self.db.at[332, col]       = self.db.loc[328, col] * self.db.loc[316, col]
            self.db.at[333, col]       = self.db.loc[329, col] * self.db.loc[316, col]
            self.db.at[334, col]       = self.db.loc[331:333, col].sum()
            
            self.db.at[335, col]       = self.db.loc[316, col]
            self.db.at[343, col]       = self.db.loc[337, col]/self.db.loc[339, col]
            self.db.at[344, col]       = (35/self.db.loc[336, col]) ** 0.312
            self.db.at[345, col]       = (62/self.db.loc[337, col]) ** 0.2
            self.db.at[347, col]       = self.db.loc[340, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[348, col]       = self.db.loc[341, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[349, col]       = self.db.loc[342, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[351, col]       = self.db.loc[347, col] * self.db.loc[344, col] * self.db.loc[345, col]
            self.db.at[352, col]       = self.db.loc[348, col] * self.db.loc[344, col] * self.db.loc[345, col]
            self.db.at[353, col]       = self.db.loc[349, col] * self.db.loc[344, col] * self.db.loc[345, col]
            self.db.at[355, col]       = self.db.loc[351, col] * self.db.loc[337, col] * 2 * self.db.loc[5, col]
            self.db.at[356, col]       = self.db.loc[352, col] * self.db.loc[343, col] * 2 * self.db.loc[9, col]            
            self.db.at[357, col]       = self.db.loc[353, col] * self.db.loc[343, col]
            self.db.at[359, col]       = self.db.loc[355, col] * self.db.loc[335, col]
            self.db.at[360, col]       = self.db.loc[356, col] * self.db.loc[335, col]        
            self.db.at[361, col]       = self.db.loc[357, col] * self.db.loc[335, col]
            self.db.at[362, col]       = self.db.loc[359:361, col].sum()

            self.db.at[364, col]       = self.db.loc[359, col] + self.db.loc[331, col]
            self.db.at[365, col]       = self.db.loc[360, col] + self.db.loc[332, col]
            self.db.at[366, col]       = self.db.loc[361, col] + self.db.loc[333, col]
            self.db.at[367, col]       = self.db.loc[364:366, col].sum()

            self.db.at[375, col]       = self.db.loc[371, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[376, col]       = self.db.loc[372, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[377, col]       = self.db.loc[373, col] * self.db.loc[34, col] * self.db.loc[37, col]            
            self.db.at[379, col]       = self.db.loc[375, col] * self.db.loc[5, col] * 200 / self.db.loc[369, col]
            self.db.at[380, col]       = self.db.loc[376, col] * self.db.loc[9, col] * 200 / self.db.loc[369, col]
            self.db.at[381, col]       = self.db.loc[377, col] * 200 / self.db.loc[369, col]
            self.db.at[383, col]       = self.db.loc[379, col] * self.db.loc[368, col]
            self.db.at[384, col]       = self.db.loc[380, col] * self.db.loc[368, col]
            self.db.at[385, col]       = self.db.loc[381, col] * self.db.loc[368, col]
            self.db.at[386, col]       = self.db.loc[383:385, col].sum()

            self.db.at[387, col]       = self.db.loc[368, col]
            self.db.at[395, col]       = self.db.loc[389, col]/self.db.loc[391, col]
            self.db.at[396, col]       = (35/self.db.loc[388, col]) ** 0.312
            self.db.at[397, col]       = (62/self.db.loc[389, col]) ** 0.2
            self.db.at[399, col]       = self.db.loc[392, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[400, col]       = self.db.loc[393, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[401, col]       = self.db.loc[394, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[403, col]       = self.db.loc[399, col] * self.db.loc[396, col] * self.db.loc[397, col]
            self.db.at[404, col]       = self.db.loc[400, col] * self.db.loc[396, col] * self.db.loc[397, col]
            self.db.at[405, col]       = self.db.loc[401, col] * self.db.loc[396, col] * self.db.loc[397, col]
            self.db.at[407, col]       = self.db.loc[403, col] * self.db.loc[389, col] * 2 * self.db.loc[5, col]
            self.db.at[408, col]       = self.db.loc[404, col] * self.db.loc[395, col] * 2 * self.db.loc[9, col]            
            self.db.at[409, col]       = self.db.loc[405, col] * self.db.loc[395, col]
            self.db.at[411, col]       = self.db.loc[407, col] * self.db.loc[387, col]
            self.db.at[412, col]       = self.db.loc[408, col] * self.db.loc[387, col]        
            self.db.at[413, col]       = self.db.loc[409, col] * self.db.loc[387, col]
            self.db.at[414, col]       = self.db.loc[411:413, col].sum()
            
            self.db.at[416, col]       = self.db.loc[383, col] * self.db.loc[411, col]
            self.db.at[417, col]       = self.db.loc[384, col] * self.db.loc[412, col]
            self.db.at[418, col]       = self.db.loc[385, col] * self.db.loc[413, col]
            self.db.at[419, col]       = self.db.loc[416:418, col].sum()

            self.db.at[427, col]       = self.db.loc[423, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[428, col]       = self.db.loc[424, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[429, col]       = self.db.loc[425, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[431, col]       = self.db.loc[427, col] * self.db.loc[5, col] * 200 / self.db.loc[421, col]
            self.db.at[432, col]       = self.db.loc[428, col] * self.db.loc[9, col] * 200 / self.db.loc[421, col]
            self.db.at[433, col]       = self.db.loc[429, col] * 200 / self.db.loc[421, col]
            self.db.at[435, col]       = self.db.loc[431, col] * self.db.loc[420, col]
            self.db.at[436, col]       = self.db.loc[432, col] * self.db.loc[420, col]
            self.db.at[437, col]       = self.db.loc[433, col] * self.db.loc[420, col]
            self.db.at[438, col]       = self.db.loc[435:437, col].sum()

            self.db.at[439, col]       = self.db.loc[420, col]
            self.db.at[447, col]       = self.db.loc[441, col]/self.db.loc[443, col]
            self.db.at[448, col]       = (35/self.db.loc[440, col]) ** 0.312
            self.db.at[449, col]       = (62/self.db.loc[441, col]) ** 0.2
            self.db.at[451, col]       = self.db.loc[444, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[452, col]       = self.db.loc[445, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[453, col]       = self.db.loc[446, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[455, col]       = self.db.loc[451, col] * self.db.loc[448, col] * self.db.loc[449, col]
            self.db.at[456, col]       = self.db.loc[452, col] * self.db.loc[448, col] * self.db.loc[449, col]
            self.db.at[457, col]       = self.db.loc[453, col] * self.db.loc[448, col] * self.db.loc[449, col]
            self.db.at[459, col]       = self.db.loc[455, col] * self.db.loc[441, col] * 2 * self.db.loc[5, col]
            self.db.at[460, col]       = self.db.loc[456, col] * self.db.loc[447, col] * 2 * self.db.loc[9, col]            
            self.db.at[461, col]       = self.db.loc[457, col] * self.db.loc[447, col]
            self.db.at[463, col]       = self.db.loc[459, col] * self.db.loc[439, col]
            self.db.at[464, col]       = self.db.loc[460, col] * self.db.loc[439, col]        
            self.db.at[465, col]       = self.db.loc[461, col] * self.db.loc[439, col]
            self.db.at[466, col]       = self.db.loc[463:465, col].sum()

            self.db.at[468, col]       = self.db.loc[463, col] + self.db.loc[435, col]
            self.db.at[469, col]       = self.db.loc[464, col] + self.db.loc[436, col]
            self.db.at[470, col]       = self.db.loc[465, col] + self.db.loc[437, col]
            self.db.at[471, col]       = self.db.loc[468:470, col].sum()

            self.db.at[473, col]       = self.db.loc[364, col] + self.db.loc[416, col] + self.db.loc[468, col]
            self.db.at[474, col]       = self.db.loc[365, col] + self.db.loc[417, col] + self.db.loc[469, col]
            self.db.at[475, col]       = self.db.loc[366, col] + self.db.loc[418, col] + self.db.loc[470, col]
            self.db.at[477, col]       = self.db.loc[473:476, col].sum()

            self.db.at[485, col]       = self.db.loc[481, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[486, col]       = self.db.loc[482, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[487, col]       = self.db.loc[483, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[489, col]       = self.db.loc[485, col] * self.db.loc[5, col] * (200 / self.db.loc[479, col])
            self.db.at[490, col]       = self.db.loc[486, col] * self.db.loc[9, col] * (200 / self.db.loc[479, col])
            self.db.at[491, col]       = self.db.loc[487, col] * (200 / self.db.loc[479, col])
            self.db.at[493, col]       = self.db.loc[489, col] * self.db.loc[478, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[494, col]       = self.db.loc[490, col] * self.db.loc[478, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[495, col]       = self.db.loc[491, col] * self.db.loc[478, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[496, col]       = self.db.loc[493:495, col].sum()

            self.db.at[497, col]       = self.db.loc[478, col]
            self.db.at[505, col]       = self.db.loc[499, col]/self.db.loc[501, col]
            self.db.at[506, col]       = (35/self.db.loc[498, col]) ** 0.312
            self.db.at[507, col]       = (62/self.db.loc[499, col]) ** 0.2
            self.db.at[509, col]       = self.db.loc[502, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[510, col]       = self.db.loc[503, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[511, col]       = self.db.loc[504, col] * self.db.loc[34, col] * self.db.loc[37, col]
            self.db.at[513, col]       = self.db.loc[509, col] * self.db.loc[506, col] * self.db.loc[507, col]
            self.db.at[514, col]       = self.db.loc[510, col] * self.db.loc[506, col] * self.db.loc[507, col]
            self.db.at[515, col]       = self.db.loc[511, col] * self.db.loc[506, col] * self.db.loc[507, col]
            self.db.at[517, col]       = self.db.loc[513, col] * self.db.loc[499, col] * 2 * self.db.loc[5, col]
            self.db.at[518, col]       = self.db.loc[514, col] * self.db.loc[505, col] * 2 * self.db.loc[9, col]
            self.db.at[519, col]       = self.db.loc[515, col] * self.db.loc[505, col]
            self.db.at[521, col]       = self.db.loc[517, col] * self.db.loc[497, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[522, col]       = self.db.loc[518, col] * self.db.loc[497, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[523, col]       = self.db.loc[519, col] * self.db.loc[497, col] * self.db.loc[28, col] / self.db.loc[29, col] * self.db.loc[31, col]
            self.db.at[524, col]       = self.db.loc[521:523, col].sum()

            self.db.at[526, col]       = self.db.loc[53, col] * self.db.loc[497, col]
            self.db.at[527, col]       = self.db.loc[54, col] * self.db.loc[497, col]
            self.db.at[528, col]       = self.db.loc[55, col] * self.db.loc[497, col]
            self.db.at[529, col]       = self.db.loc[56, col] * self.db.loc[497, col]
            self.db.at[530, col]       = self.db.loc[526:529, col].sum()

            self.db.at[532, col]       = self.db.loc[526, col] + self.db.loc[521, col] + self.db.loc[493, col]
            self.db.at[533, col]       = self.db.loc[527, col] + self.db.loc[522, col] + self.db.loc[494, col]
            self.db.at[534, col]       = self.db.loc[528, col] + self.db.loc[523, col] + self.db.loc[495, col]
            self.db.at[536, col]       = self.db.loc[532:535, col].sum()

            self.db.at[538, col]       = self.db.loc[53, col] + self.db.loc[194, col] + self.db.loc[311, col] + self.db.loc[473, col] + self.db.loc[532, col]
            self.db.at[539, col]       = self.db.loc[54, col] + self.db.loc[195, col] + self.db.loc[312, col] + self.db.loc[474, col] + self.db.loc[533, col]
            self.db.at[540, col]       = self.db.loc[55, col] + self.db.loc[196, col] + self.db.loc[313, col] + self.db.loc[475, col] + self.db.loc[534, col]
            self.db.at[541, col]       = self.db.loc[56, col] + self.db.loc[197, col] + self.db.loc[314, col] + self.db.loc[476, col] + self.db.loc[535, col]            
            self.db.at[542, col]       = self.db.loc[12, col]
            self.db.at[543, col]       = self.db.loc[13, col]
            self.db.at[544, col]       = self.db.loc[538:543, col].sum()

            # Road legs
            self.db.at[546, col]       = vlookup(self.db.loc[545, col], self.lookup2.loc[:, "General Efficiency"], self.lookup2.loc[:, "Multiplier"])
            self.db.at[554, col]       = self.db.loc[550, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[555, col]       = self.db.loc[551, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[556, col]       = self.db.loc[552, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[558, col]       = self.db.loc[554, col] * self.db.loc[5, col] * 200 / self.db.loc[548, col]
            self.db.at[559, col]       = self.db.loc[555, col] * self.db.loc[9, col] * 200 / self.db.loc[548, col]
            self.db.at[560, col]       = self.db.loc[556, col] * 200 / self.db.loc[548, col]
            self.db.at[562, col]       = self.db.loc[558, col] * self.db.loc[547, col]
            self.db.at[563, col]       = self.db.loc[559, col] * self.db.loc[547, col]
            self.db.at[564, col]       = self.db.loc[560, col] * self.db.loc[547, col]
            self.db.at[565, col]       = self.db.loc[562:564, col].sum()

            self.db.at[566, col]       = self.db.loc[547, col]
            self.db.at[574, col]       = self.db.loc[568, col]/self.db.loc[570, col]
            self.db.at[575, col]       = (35/self.db.loc[567, col]) ** 0.312
            self.db.at[576, col]       = (62/self.db.loc[568, col]) ** 0.2
            self.db.at[578, col]       = self.db.loc[571, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[579, col]       = self.db.loc[572, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[580, col]       = self.db.loc[573, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[582, col]       = self.db.loc[578, col] * self.db.loc[575, col] * self.db.loc[576, col]
            self.db.at[583, col]       = self.db.loc[579, col] * self.db.loc[575, col] * self.db.loc[576, col]
            self.db.at[584, col]       = self.db.loc[580, col] * self.db.loc[575, col] * self.db.loc[576, col]
            self.db.at[586, col]       = self.db.loc[582, col] * self.db.loc[568, col] * 2 * self.db.loc[5, col]
            self.db.at[587, col]       = self.db.loc[583, col] * self.db.loc[574, col] * 2 * self.db.loc[9, col]            
            self.db.at[588, col]       = self.db.loc[584, col] * self.db.loc[574, col]
            self.db.at[590, col]       = self.db.loc[586, col] * self.db.loc[566, col]
            self.db.at[591, col]       = self.db.loc[587, col] * self.db.loc[566, col]        
            self.db.at[592, col]       = self.db.loc[588, col] * self.db.loc[566, col]
            self.db.at[593, col]       = self.db.loc[590:592, col].sum()

            self.db.at[595, col]       = self.db.loc[590, col] + self.db.loc[562, col]
            self.db.at[596, col]       = self.db.loc[591, col] + self.db.loc[563, col]
            self.db.at[597, col]       = self.db.loc[592, col] + self.db.loc[564, col]
            self.db.at[598, col]       = self.db.loc[595:597, col].sum()

            self.db.at[606, col]       = self.db.loc[602, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[607, col]       = self.db.loc[603, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[608, col]       = self.db.loc[604, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[610, col]       = self.db.loc[606, col] * self.db.loc[5, col] * 200 / self.db.loc[600, col]
            self.db.at[611, col]       = self.db.loc[607, col] * self.db.loc[9, col] * 200 / self.db.loc[600, col]
            self.db.at[612, col]       = self.db.loc[608, col] * 200 / self.db.loc[600, col]
            self.db.at[614, col]       = self.db.loc[610, col] * self.db.loc[599, col]
            self.db.at[615, col]       = self.db.loc[611, col] * self.db.loc[599, col]
            self.db.at[616, col]       = self.db.loc[612, col] * self.db.loc[599, col]
            self.db.at[617, col]       = self.db.loc[614:616, col].sum()

            self.db.at[618, col]       = self.db.loc[599, col]
            self.db.at[626, col]       = self.db.loc[620, col]/self.db.loc[622, col]
            self.db.at[627, col]       = (35/self.db.loc[619, col]) ** 0.312
            self.db.at[628, col]       = (62/self.db.loc[620, col]) ** 0.2
            self.db.at[630, col]       = self.db.loc[623, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[631, col]       = self.db.loc[624, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[632, col]       = self.db.loc[625, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[634, col]       = self.db.loc[630, col] * self.db.loc[627, col] * self.db.loc[628, col]
            self.db.at[635, col]       = self.db.loc[631, col] * self.db.loc[627, col] * self.db.loc[628, col]
            self.db.at[636, col]       = self.db.loc[632, col] * self.db.loc[627, col] * self.db.loc[628, col]
            self.db.at[638, col]       = self.db.loc[634, col] * self.db.loc[620, col] * 2 * self.db.loc[5, col]
            self.db.at[639, col]       = self.db.loc[635, col] * self.db.loc[626, col] * 2 * self.db.loc[9, col]            
            self.db.at[640, col]       = self.db.loc[636, col] * self.db.loc[626, col]
            self.db.at[642, col]       = self.db.loc[638, col] * self.db.loc[618, col]
            self.db.at[643, col]       = self.db.loc[639, col] * self.db.loc[618, col]        
            self.db.at[644, col]       = self.db.loc[640, col] * self.db.loc[618, col]
            self.db.at[645, col]       = self.db.loc[642:644, col].sum()

            self.db.at[647, col]       = self.db.loc[642, col] + self.db.loc[614, col]
            self.db.at[648, col]       = self.db.loc[643, col] + self.db.loc[615, col]
            self.db.at[649, col]       = self.db.loc[644, col] + self.db.loc[616, col]
            self.db.at[650, col]       = self.db.loc[647:649, col].sum()

            self.db.at[658, col]       = self.db.loc[654, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[659, col]       = self.db.loc[655, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[660, col]       = self.db.loc[656, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[662, col]       = self.db.loc[658, col] * self.db.loc[5, col] * 200 / self.db.loc[652, col]
            self.db.at[663, col]       = self.db.loc[659, col] * self.db.loc[9, col] * 200 / self.db.loc[652, col]
            self.db.at[664, col]       = self.db.loc[660, col] * 200 / self.db.loc[652, col]
            self.db.at[666, col]       = self.db.loc[662, col] * self.db.loc[651, col]
            self.db.at[667, col]       = self.db.loc[663, col] * self.db.loc[651, col]
            self.db.at[668, col]       = self.db.loc[664, col] * self.db.loc[651, col]
            self.db.at[669, col]       = self.db.loc[666:668, col].sum()

            self.db.at[670, col]       = self.db.loc[651, col]
            self.db.at[678, col]       = self.db.loc[672, col]/self.db.loc[674, col]
            self.db.at[679, col]       = (35/self.db.loc[671, col]) ** 0.312
            self.db.at[680, col]       = (62/self.db.loc[672, col]) ** 0.2
            self.db.at[682, col]       = self.db.loc[675, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[683, col]       = self.db.loc[676, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[684, col]       = self.db.loc[677, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[686, col]       = self.db.loc[682, col] * self.db.loc[679, col] * self.db.loc[680, col]
            self.db.at[687, col]       = self.db.loc[683, col] * self.db.loc[679, col] * self.db.loc[680, col]
            self.db.at[688, col]       = self.db.loc[684, col] * self.db.loc[679, col] * self.db.loc[680, col]
            self.db.at[690, col]       = self.db.loc[686, col] * self.db.loc[672, col] * 2 * self.db.loc[5, col]
            self.db.at[691, col]       = self.db.loc[687, col] * self.db.loc[678, col] * 2 * self.db.loc[9, col]         
            self.db.at[692, col]       = self.db.loc[688, col] * self.db.loc[678, col]
            self.db.at[694, col]       = self.db.loc[690, col] * self.db.loc[670, col]
            self.db.at[695, col]       = self.db.loc[691, col] * self.db.loc[670, col]        
            self.db.at[696, col]       = self.db.loc[692, col] * self.db.loc[670, col]
            self.db.at[697, col]       = self.db.loc[694:696, col].sum()

            self.db.at[699, col]       = self.db.loc[694, col] + self.db.loc[666, col]
            self.db.at[700, col]       = self.db.loc[695, col] + self.db.loc[667, col]
            self.db.at[701, col]       = self.db.loc[696, col] + self.db.loc[668, col]
            self.db.at[702, col]       = self.db.loc[699:701, col].sum()

            self.db.at[704, col]       = self.db.loc[595, col] + self.db.loc[647, col] + self.db.loc[699, col]
            self.db.at[705, col]       = self.db.loc[596, col] + self.db.loc[648, col] + self.db.loc[700, col]
            self.db.at[706, col]       = self.db.loc[597, col] + self.db.loc[649, col] + self.db.loc[701, col]
            self.db.at[708, col]       = self.db.loc[704:707, col].sum()

            # Rail leg
            self.db.at[716, col]       = self.db.loc[712, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[717, col]       = self.db.loc[713, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[718, col]       = self.db.loc[714, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[720, col]       = self.db.loc[716, col] * self.db.loc[5, col] * 200 / self.db.loc[710, col]
            self.db.at[721, col]       = self.db.loc[717, col] * self.db.loc[9, col] * 200 / self.db.loc[710, col]
            self.db.at[722, col]       = self.db.loc[718, col] * 200 / self.db.loc[710, col]
            self.db.at[724, col]       = self.db.loc[720, col] * self.db.loc[709, col]
            self.db.at[725, col]       = self.db.loc[721, col] * self.db.loc[709, col]
            self.db.at[726, col]       = self.db.loc[722, col] * self.db.loc[709, col]
            self.db.at[727, col]       = self.db.loc[724:726, col].sum()

            self.db.at[728, col]       = self.db.loc[709, col]
            self.db.at[736, col]       = self.db.loc[730, col]/self.db.loc[732, col]
            self.db.at[737, col]       = (35/self.db.loc[729, col]) ** 0.312
            self.db.at[738, col]       = (62/self.db.loc[730, col]) ** 0.2
            self.db.at[740, col]       = self.db.loc[733, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[741, col]       = self.db.loc[734, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[742, col]       = self.db.loc[735, col] * self.db.loc[546, col] * self.db.loc[37, col]
            self.db.at[744, col]       = self.db.loc[740, col] * self.db.loc[737, col] * self.db.loc[738, col]
            self.db.at[745, col]       = self.db.loc[741, col] * self.db.loc[737, col] * self.db.loc[738, col] / 120
            self.db.at[746, col]       = self.db.loc[742, col] * self.db.loc[737, col] * self.db.loc[738, col]
            self.db.at[748, col]       = self.db.loc[744, col] * self.db.loc[730, col] * 2 * self.db.loc[5, col]
            self.db.at[749, col]       = self.db.loc[745, col] * self.db.loc[736, col] * 2 * self.db.loc[9, col]            
            self.db.at[750, col]       = self.db.loc[746, col] * self.db.loc[736, col]
            self.db.at[752, col]       = self.db.loc[748, col] * self.db.loc[728, col]
            self.db.at[753, col]       = self.db.loc[749, col] * self.db.loc[728, col]        
            self.db.at[754, col]       = self.db.loc[750, col] * self.db.loc[728, col]
            self.db.at[755, col]       = self.db.loc[752:754, col].sum()

            self.db.at[757, col]       = self.db.loc[752, col] + self.db.loc[724, col]
            self.db.at[758, col]       = self.db.loc[753, col] + self.db.loc[725, col]
            self.db.at[759, col]       = self.db.loc[754, col] + self.db.loc[726, col]
            self.db.at[760, col]       = self.db.loc[757:759, col].sum()

            self.db.at[762, col]       = self.db.loc[757, col] + self.db.loc[704, col] + self.db.loc[699, col]
            self.db.at[763, col]       = self.db.loc[758, col] + self.db.loc[705, col] + self.db.loc[700, col]
            self.db.at[764, col]       = self.db.loc[759, col] + self.db.loc[706, col] + self.db.loc[701, col]
            self.db.at[766, col]       = self.db.loc[14, col]
            self.db.at[767, col]       = self.db.loc[762:766, col].sum()

            # Material movements
            self.db.at[769, col]       = vlookup(self.db.loc[768, col], self.lookup2.loc[:, "General Efficiency"], self.lookup2.loc[:, "Multiplier"])
            self.db.at[777, col]       = self.db.loc[773, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[778, col]       = self.db.loc[774, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[779, col]       = self.db.loc[775, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[781, col]       = self.db.loc[777, col] * self.db.loc[5, col] * 200 / self.db.loc[771, col]
            self.db.at[782, col]       = self.db.loc[778, col] * self.db.loc[9, col] * 200 / self.db.loc[771, col]
            self.db.at[783, col]       = self.db.loc[779, col] * 200 / self.db.loc[771, col]
            self.db.at[785, col]       = self.db.loc[781, col] * self.db.loc[770, col]
            self.db.at[786, col]       = self.db.loc[782, col] * self.db.loc[770, col]
            self.db.at[787, col]       = self.db.loc[783, col] * self.db.loc[770, col]
            self.db.at[788, col]       = self.db.loc[785:787, col].sum()

            self.db.at[789, col]       = self.db.loc[770, col]
            self.db.at[797, col]       = self.db.loc[791, col]/self.db.loc[793, col]
            self.db.at[798, col]       = (35/self.db.loc[790, col]) ** 0.312
            self.db.at[799, col]       = (62/self.db.loc[791, col]) ** 0.2
            self.db.at[801, col]       = self.db.loc[794, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[802, col]       = self.db.loc[795, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[803, col]       = self.db.loc[796, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[805, col]       = self.db.loc[801, col] * self.db.loc[798, col] * self.db.loc[799, col]
            self.db.at[806, col]       = self.db.loc[802, col] * self.db.loc[798, col] * self.db.loc[799, col]
            self.db.at[807, col]       = self.db.loc[803, col] * self.db.loc[798, col] * self.db.loc[799, col]
            self.db.at[809, col]       = self.db.loc[805, col] * self.db.loc[791, col] * 2 * self.db.loc[5, col]
            self.db.at[810, col]       = self.db.loc[806, col] * self.db.loc[797, col] * 2 * self.db.loc[9, col]            
            self.db.at[811, col]       = self.db.loc[807, col] * self.db.loc[797, col]
            self.db.at[813, col]       = self.db.loc[809, col] * self.db.loc[789, col]
            self.db.at[814, col]       = self.db.loc[810, col] * self.db.loc[789, col]        
            self.db.at[815, col]       = self.db.loc[811, col] * self.db.loc[789, col]
            self.db.at[816, col]       = self.db.loc[813:815, col].sum()

            self.db.at[818, col]       = self.db.loc[813, col] + self.db.loc[785, col]
            self.db.at[819, col]       = self.db.loc[814, col] + self.db.loc[786, col]
            self.db.at[820, col]       = self.db.loc[815, col] + self.db.loc[787, col]
            self.db.at[821, col]       = self.db.loc[818:820, col].sum()

            self.db.at[829, col]       = self.db.loc[825, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[830, col]       = self.db.loc[826, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[831, col]       = self.db.loc[827, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[833, col]       = self.db.loc[829, col] * self.db.loc[5, col] * 200 / self.db.loc[823, col]
            self.db.at[834, col]       = self.db.loc[830, col] * self.db.loc[9, col] * 200 / self.db.loc[823, col]
            self.db.at[835, col]       = self.db.loc[831, col] * 200 / self.db.loc[823, col]
            self.db.at[837, col]       = self.db.loc[833, col] * self.db.loc[822, col]
            self.db.at[838, col]       = self.db.loc[834, col] * self.db.loc[822, col]
            self.db.at[839, col]       = self.db.loc[835, col] * self.db.loc[822, col]
            self.db.at[840, col]       = self.db.loc[837:839, col].sum()

            self.db.at[841, col]       = self.db.loc[822, col]
            self.db.at[849, col]       = self.db.loc[843, col]/self.db.loc[845, col]
            self.db.at[850, col]       = (35/self.db.loc[842, col]) ** 0.312
            self.db.at[851, col]       = (62/self.db.loc[843, col]) ** 0.2
            self.db.at[853, col]       = self.db.loc[846, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[854, col]       = self.db.loc[847, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[855, col]       = self.db.loc[848, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[857, col]       = self.db.loc[853, col] * self.db.loc[850, col] * self.db.loc[851, col]
            self.db.at[858, col]       = self.db.loc[854, col] * self.db.loc[850, col] * self.db.loc[851, col]
            self.db.at[859, col]       = self.db.loc[855, col] * self.db.loc[850, col] * self.db.loc[851, col]
            self.db.at[861, col]       = self.db.loc[857, col] * self.db.loc[843, col] * 2 * self.db.loc[5, col]
            self.db.at[862, col]       = self.db.loc[858, col] * self.db.loc[849, col] * 2 * self.db.loc[9, col]            
            self.db.at[863, col]       = self.db.loc[859, col] * self.db.loc[849, col]
            self.db.at[865, col]       = self.db.loc[861, col] * self.db.loc[841, col]
            self.db.at[866, col]       = self.db.loc[862, col] * self.db.loc[841, col]        
            self.db.at[867, col]       = self.db.loc[863, col] * self.db.loc[841, col]
            self.db.at[868, col]       = self.db.loc[865:867, col].sum()

            self.db.at[870, col]       = self.db.loc[865, col] + self.db.loc[837, col]
            self.db.at[871, col]       = self.db.loc[866, col] + self.db.loc[838, col]
            self.db.at[872, col]       = self.db.loc[867, col] + self.db.loc[839, col]
            self.db.at[873, col]       = self.db.loc[870:872, col].sum()

            self.db.at[881, col]       = self.db.loc[877, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[882, col]       = self.db.loc[878, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[883, col]       = self.db.loc[879, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[885, col]       = self.db.loc[881, col] * self.db.loc[5, col] * 200 / self.db.loc[875, col]
            self.db.at[886, col]       = self.db.loc[882, col] * self.db.loc[9, col] * 200 / self.db.loc[875, col]
            self.db.at[887, col]       = self.db.loc[883, col] * 200 / self.db.loc[875, col]
            self.db.at[889, col]       = self.db.loc[885, col] * self.db.loc[874, col]
            self.db.at[890, col]       = self.db.loc[886, col] * self.db.loc[874, col]
            self.db.at[891, col]       = self.db.loc[887, col] * self.db.loc[874, col]
            self.db.at[892, col]       = self.db.loc[889:891, col].sum()

            self.db.at[893, col]       = self.db.loc[874, col]
            self.db.at[901, col]       = self.db.loc[895, col]/self.db.loc[897, col]
            self.db.at[902, col]       = (35/self.db.loc[894, col]) ** 0.312
            self.db.at[903, col]       = (62/self.db.loc[895, col]) ** 0.2
            self.db.at[905, col]       = self.db.loc[898, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[906, col]       = self.db.loc[899, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[907, col]       = self.db.loc[900, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[909, col]       = self.db.loc[905, col] * self.db.loc[902, col] * self.db.loc[903, col]
            self.db.at[910, col]       = self.db.loc[906, col] * self.db.loc[902, col] * self.db.loc[903, col]
            self.db.at[911, col]       = self.db.loc[907, col] * self.db.loc[902, col] * self.db.loc[903, col]
            self.db.at[913, col]       = self.db.loc[909, col] * self.db.loc[895, col] * 2 * self.db.loc[5, col]
            self.db.at[914, col]       = self.db.loc[910, col] * self.db.loc[901, col] * 2 * self.db.loc[9, col]         
            self.db.at[915, col]       = self.db.loc[911, col] * self.db.loc[901, col]
            self.db.at[917, col]       = self.db.loc[913, col] * self.db.loc[893, col]
            self.db.at[918, col]       = self.db.loc[914, col] * self.db.loc[893, col]        
            self.db.at[919, col]       = self.db.loc[915, col] * self.db.loc[893, col]
            self.db.at[920, col]       = self.db.loc[917:919, col].sum()

            self.db.at[922, col]       = self.db.loc[917, col] + self.db.loc[889, col]
            self.db.at[923, col]       = self.db.loc[918, col] + self.db.loc[890, col]
            self.db.at[924, col]       = self.db.loc[919, col] + self.db.loc[891, col]
            self.db.at[925, col]       = self.db.loc[922:924, col].sum() 

            self.db.at[933, col]       = self.db.loc[929, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[934, col]       = self.db.loc[930, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[935, col]       = self.db.loc[931, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[937, col]       = self.db.loc[933, col] * self.db.loc[5, col] * 200 / self.db.loc[927, col]
            self.db.at[938, col]       = self.db.loc[934, col] * self.db.loc[9, col] * 200 / self.db.loc[927, col]
            self.db.at[939, col]       = self.db.loc[935, col] * 200 / self.db.loc[927, col]
            self.db.at[941, col]       = self.db.loc[937, col] * self.db.loc[926, col]
            self.db.at[942, col]       = self.db.loc[938, col] * self.db.loc[926, col]
            self.db.at[943, col]       = self.db.loc[939, col] * self.db.loc[926, col]
            self.db.at[944, col]       = self.db.loc[941:943, col].sum()

            self.db.at[945, col]       = self.db.loc[926, col]
            self.db.at[953, col]       = self.db.loc[947, col]/self.db.loc[949, col]
            self.db.at[954, col]       = (35/self.db.loc[946, col]) ** 0.312
            self.db.at[955, col]       = (62/self.db.loc[947, col]) ** 0.2
            self.db.at[957, col]       = self.db.loc[950, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[958, col]       = self.db.loc[951, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[959, col]       = self.db.loc[952, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[961, col]       = self.db.loc[957, col] * self.db.loc[954, col] * self.db.loc[955, col]
            self.db.at[962, col]       = self.db.loc[958, col] * self.db.loc[954, col] * self.db.loc[955, col]
            self.db.at[963, col]       = self.db.loc[959, col] * self.db.loc[954, col] * self.db.loc[955, col]
            self.db.at[965, col]       = self.db.loc[961, col] * self.db.loc[947, col] * 2 * self.db.loc[5, col]
            self.db.at[966, col]       = self.db.loc[962, col] * self.db.loc[953, col] * 2 * self.db.loc[9, col]         
            self.db.at[967, col]       = self.db.loc[963, col] * self.db.loc[953, col]
            self.db.at[969, col]       = self.db.loc[965, col] * self.db.loc[945, col]
            self.db.at[970, col]       = self.db.loc[966, col] * self.db.loc[945, col]        
            self.db.at[971, col]       = self.db.loc[967, col] * self.db.loc[945, col]
            self.db.at[972, col]       = self.db.loc[969:971, col].sum()

            self.db.at[974, col]       = self.db.loc[969, col] + self.db.loc[941, col]
            self.db.at[975, col]       = self.db.loc[970, col] + self.db.loc[942, col]
            self.db.at[976, col]       = self.db.loc[971, col] + self.db.loc[943, col]
            self.db.at[977, col]       = self.db.loc[974:976, col].sum()

            
            self.db.at[881, col]       = self.db.loc[877, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[882, col]       = self.db.loc[878, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[883, col]       = self.db.loc[879, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[885, col]       = self.db.loc[881, col] * self.db.loc[5, col] * 200 / self.db.loc[875, col]
            self.db.at[886, col]       = self.db.loc[882, col] * self.db.loc[9, col] * 200 / self.db.loc[875, col]
            self.db.at[887, col]       = self.db.loc[883, col] * 200 / self.db.loc[875, col]
            self.db.at[889, col]       = self.db.loc[885, col] * self.db.loc[874, col]
            self.db.at[890, col]       = self.db.loc[886, col] * self.db.loc[874, col]
            self.db.at[891, col]       = self.db.loc[887, col] * self.db.loc[874, col]
            self.db.at[892, col]       = self.db.loc[889:891, col].sum()

            self.db.at[893, col]       = self.db.loc[874, col]
            self.db.at[901, col]       = self.db.loc[895, col]/self.db.loc[897, col]
            self.db.at[902, col]       = (35/self.db.loc[894, col]) ** 0.312
            self.db.at[903, col]       = (62/self.db.loc[895, col]) ** 0.2
            self.db.at[905, col]       = self.db.loc[898, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[906, col]       = self.db.loc[899, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[907, col]       = self.db.loc[900, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[909, col]       = self.db.loc[905, col] * self.db.loc[902, col] * self.db.loc[903, col]
            self.db.at[910, col]       = self.db.loc[906, col] * self.db.loc[902, col] * self.db.loc[903, col]
            self.db.at[911, col]       = self.db.loc[907, col] * self.db.loc[902, col] * self.db.loc[903, col]
            self.db.at[913, col]       = self.db.loc[909, col] * self.db.loc[895, col] * 2 * self.db.loc[5, col]
            self.db.at[914, col]       = self.db.loc[910, col] * self.db.loc[901, col] * 2 * self.db.loc[9, col]         
            self.db.at[915, col]       = self.db.loc[911, col] * self.db.loc[901, col]
            self.db.at[917, col]       = self.db.loc[913, col] * self.db.loc[893, col]
            self.db.at[918, col]       = self.db.loc[914, col] * self.db.loc[893, col]        
            self.db.at[919, col]       = self.db.loc[915, col] * self.db.loc[893, col]
            self.db.at[920, col]       = self.db.loc[917:919, col].sum()

            self.db.at[922, col]       = self.db.loc[917, col] + self.db.loc[889, col]
            self.db.at[923, col]       = self.db.loc[918, col] + self.db.loc[890, col]
            self.db.at[924, col]       = self.db.loc[919, col] + self.db.loc[891, col]
            self.db.at[925, col]       = self.db.loc[922:924, col].sum() 

            self.db.at[933, col]       = self.db.loc[929, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[934, col]       = self.db.loc[930, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[935, col]       = self.db.loc[931, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[937, col]       = self.db.loc[933, col] * self.db.loc[5, col] * 200 / self.db.loc[927, col]
            self.db.at[938, col]       = self.db.loc[934, col] * self.db.loc[9, col] * 200 / self.db.loc[927, col]
            self.db.at[939, col]       = self.db.loc[935, col] * 200 / self.db.loc[927, col]
            self.db.at[941, col]       = self.db.loc[937, col] * self.db.loc[926, col]
            self.db.at[942, col]       = self.db.loc[938, col] * self.db.loc[926, col]
            self.db.at[943, col]       = self.db.loc[939, col] * self.db.loc[926, col]
            self.db.at[944, col]       = self.db.loc[941:943, col].sum()

            self.db.at[945, col]       = self.db.loc[926, col]
            self.db.at[953, col]       = self.db.loc[947, col]/self.db.loc[949, col]
            self.db.at[954, col]       = (35/self.db.loc[946, col]) ** 0.312
            self.db.at[955, col]       = (62/self.db.loc[947, col]) ** 0.2
            self.db.at[957, col]       = self.db.loc[950, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[958, col]       = self.db.loc[951, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[959, col]       = self.db.loc[952, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[961, col]       = self.db.loc[957, col] * self.db.loc[954, col] * self.db.loc[955, col]
            self.db.at[962, col]       = self.db.loc[958, col] * self.db.loc[954, col] * self.db.loc[955, col]
            self.db.at[963, col]       = self.db.loc[959, col] * self.db.loc[954, col] * self.db.loc[955, col]
            self.db.at[965, col]       = self.db.loc[961, col] * self.db.loc[947, col] * 2 * self.db.loc[5, col]
            self.db.at[966, col]       = self.db.loc[962, col] * self.db.loc[953, col] * 2 * self.db.loc[9, col]         
            self.db.at[967, col]       = self.db.loc[963, col] * self.db.loc[953, col]
            self.db.at[969, col]       = self.db.loc[965, col] * self.db.loc[945, col]
            self.db.at[970, col]       = self.db.loc[966, col] * self.db.loc[945, col]        
            self.db.at[971, col]       = self.db.loc[967, col] * self.db.loc[945, col]
            self.db.at[972, col]       = self.db.loc[969:971, col].sum()

            self.db.at[974, col]       = self.db.loc[969, col] + self.db.loc[941, col]
            self.db.at[975, col]       = self.db.loc[970, col] + self.db.loc[942, col]
            self.db.at[976, col]       = self.db.loc[971, col] + self.db.loc[943, col]
            self.db.at[977, col]       = self.db.loc[974:976, col].sum()


            self.db.at[985, col]       = self.db.loc[981, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[986, col]       = self.db.loc[982, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[987, col]       = self.db.loc[983, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[989, col]       = self.db.loc[985, col] * self.db.loc[5, col] * 200 / self.db.loc[979, col]
            self.db.at[990, col]       = self.db.loc[986, col] * self.db.loc[9, col] * 200 / self.db.loc[979, col]
            self.db.at[991, col]       = self.db.loc[987, col] * 200 / self.db.loc[979, col]
            self.db.at[993, col]       = self.db.loc[989, col] * self.db.loc[978, col]
            self.db.at[994, col]       = self.db.loc[990, col] * self.db.loc[978, col]
            self.db.at[995, col]       = self.db.loc[991, col] * self.db.loc[978, col]
            self.db.at[996, col]       = self.db.loc[993:995, col].sum()

            self.db.at[997, col]       = self.db.loc[978, col]
            self.db.at[1005, col]      = self.db.loc[999, col]/self.db.loc[1001, col]
            self.db.at[1006, col]      = (35/self.db.loc[998, col]) ** 0.312
            self.db.at[1007, col]      = (62/self.db.loc[999, col]) ** 0.2
            self.db.at[1009, col]      = self.db.loc[1002, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1010, col]      = self.db.loc[1003, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1011, col]      = self.db.loc[1004, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1013, col]      = self.db.loc[1009, col] * self.db.loc[1006, col] * self.db.loc[1007, col]
            self.db.at[1014, col]      = self.db.loc[1010, col] * self.db.loc[1006, col] * self.db.loc[1007, col]
            self.db.at[1015, col]      = self.db.loc[1011, col] * self.db.loc[1006, col] * self.db.loc[1007, col]
            self.db.at[1017, col]      = self.db.loc[1013, col] * self.db.loc[999, col] * 2 * self.db.loc[5, col]
            self.db.at[1018, col]      = self.db.loc[1014, col] * self.db.loc[1005, col] * 2 * self.db.loc[9, col]         
            self.db.at[1019, col]      = self.db.loc[1015, col] * self.db.loc[1005, col]
            self.db.at[1021, col]      = self.db.loc[1017, col] * self.db.loc[997, col]
            self.db.at[1022, col]      = self.db.loc[1018, col] * self.db.loc[997, col]        
            self.db.at[1023, col]      = self.db.loc[1019, col] * self.db.loc[997, col]
            self.db.at[1024, col]      = self.db.loc[1021:1023, col].sum()

            self.db.at[1026, col]       = self.db.loc[1021, col] + self.db.loc[993, col]
            self.db.at[1027, col]       = self.db.loc[1022, col] + self.db.loc[994, col]
            self.db.at[1028, col]       = self.db.loc[1023, col] + self.db.loc[995, col]
            self.db.at[1029, col]       = self.db.loc[1026:1028, col].sum() 

            self.db.at[1037, col]       = self.db.loc[1033, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1038, col]       = self.db.loc[1034, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1039, col]       = self.db.loc[1035, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1041, col]       = self.db.loc[1037, col] * self.db.loc[5, col] * 200 / self.db.loc[1031, col]
            self.db.at[1042, col]       = self.db.loc[1038, col] * self.db.loc[9, col] * 200 / self.db.loc[1031, col]
            self.db.at[1043, col]       = self.db.loc[1039, col] * 200 / self.db.loc[1031, col]
            self.db.at[1045, col]       = self.db.loc[1041, col] * self.db.loc[1030, col]
            self.db.at[1046, col]       = self.db.loc[1042, col] * self.db.loc[1030, col]
            self.db.at[1047, col]       = self.db.loc[1043, col] * self.db.loc[1030, col]
            self.db.at[1048, col]       = self.db.loc[1045:1047, col].sum()

            self.db.at[1049, col]       = self.db.loc[1030, col]
            self.db.at[1057, col]       = self.db.loc[1051, col]/self.db.loc[1053, col]
            self.db.at[1058, col]       = (35/self.db.loc[1050, col]) ** 0.312
            self.db.at[1059, col]       = (62/self.db.loc[1051, col]) ** 0.2
            self.db.at[1061, col]       = self.db.loc[1054, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1062, col]       = self.db.loc[1055, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1063, col]       = self.db.loc[1056, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1065, col]       = self.db.loc[1061, col] * self.db.loc[1058, col] * self.db.loc[1059, col]
            self.db.at[1066, col]       = self.db.loc[1062, col] * self.db.loc[1058, col] * self.db.loc[1059, col]
            self.db.at[1067, col]       = self.db.loc[1063, col] * self.db.loc[1058, col] * self.db.loc[1059, col]
            self.db.at[1069, col]       = self.db.loc[1065, col] * self.db.loc[1051, col] * 2 * self.db.loc[5, col]
            self.db.at[1070, col]       = self.db.loc[1066, col] * self.db.loc[1057, col] * 2 * self.db.loc[9, col]         
            self.db.at[1071, col]       = self.db.loc[1067, col] * self.db.loc[1057, col]
            self.db.at[1073, col]       = self.db.loc[1069, col] * self.db.loc[1049, col]
            self.db.at[1074, col]       = self.db.loc[1070, col] * self.db.loc[1049, col]        
            self.db.at[1075, col]       = self.db.loc[1071, col] * self.db.loc[1049, col]
            self.db.at[1076, col]       = self.db.loc[1073:1075, col].sum()

            self.db.at[1078, col]       = self.db.loc[1073, col] + self.db.loc[1045, col]
            self.db.at[1079, col]       = self.db.loc[1074, col] + self.db.loc[1046, col]
            self.db.at[1080, col]       = self.db.loc[1075, col] + self.db.loc[1047, col]
            self.db.at[1081, col]       = self.db.loc[1078:1080, col].sum()

            self.db.at[1083, col]       = self.db.loc[818, col] + self.db.loc[870, col] + self.db.loc[922, col] + self.db.loc[974, col] + self.db.loc[1026, col] + self.db.loc[1078, col]
            self.db.at[1084, col]       = self.db.loc[819, col] + self.db.loc[871, col] + self.db.loc[923, col] + self.db.loc[975, col] + self.db.loc[1027, col] + self.db.loc[1078, col]
            self.db.at[1085, col]       = self.db.loc[820, col] + self.db.loc[872, col] + self.db.loc[924, col] + self.db.loc[976, col] + self.db.loc[1028, col] + self.db.loc[1078, col]
            self.db.at[1087, col]       = self.db.loc[1083:1086, col].sum()

            # Crush Oversize
            self.db.at[1094, col]       = self.db.loc[1090, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1095, col]       = self.db.loc[1091, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1096, col]       = self.db.loc[1092, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1098, col]       = self.db.loc[1094, col] * self.db.loc[5, col] * (1 - self.db.loc[2, col])
            self.db.at[1099, col]       = self.db.loc[1095, col] * self.db.loc[9, col] * (1 - self.db.loc[2, col])
            self.db.at[1100, col]       = self.db.loc[1096, col] * (1 - self.db.loc[2, col])
            self.db.at[1102, col]       = self.db.loc[1098, col] * self.db.loc[1088, col]
            self.db.at[1103, col]       = self.db.loc[1099, col] * self.db.loc[1088, col]
            self.db.at[1104, col]       = self.db.loc[1100, col] * self.db.loc[1088, col]
            self.db.at[1105, col]       = self.db.loc[1102:1104, col].sum()

            # Full crushing
            self.db.at[1112, col]       = self.db.loc[1108, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1113, col]       = self.db.loc[1109, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1114, col]       = self.db.loc[1110, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1116, col]       = self.db.loc[1112, col] * self.db.loc[5, col]
            self.db.at[1117, col]       = self.db.loc[1113, col] * self.db.loc[9, col]
            self.db.at[1118, col]       = self.db.loc[1114, col]
            self.db.at[1120, col]       = self.db.loc[1116, col] * self.db.loc[1106, col]
            self.db.at[1121, col]       = self.db.loc[1117, col] * self.db.loc[1106, col]
            self.db.at[1122, col]       = self.db.loc[1118, col] * self.db.loc[1106, col]
            self.db.at[1123, col]       = self.db.loc[1120:1122, col].sum()

            # Drying
            self.db.at[1130, col]       = self.db.loc[1126, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1131, col]       = self.db.loc[1127, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1132, col]       = self.db.loc[1128, col] * self.db.loc[769, col] * self.db.loc[37, col]
            self.db.at[1134, col]       = self.db.loc[1130, col] * self.db.loc[5, col] * 0.67
            self.db.at[1135,  col]      = self.db.loc[1131, col] * self.db.loc[9, col]
            self.db.at[1136, col]       = self.db.loc[1132, col]
            self.db.at[1138, col]       = self.db.loc[1134, col] * self.db.loc[1124, col]
            self.db.at[1139, col]       = self.db.loc[1135, col] * self.db.loc[1124, col]
            self.db.at[1140, col]       = self.db.loc[1136, col] * self.db.loc[1124, col]
            self.db.at[1141, col]       = self.db.loc[1138:1140, col].sum()

            # Port costs - total
            self.db.at[1142, col]       = self.db.loc[15, col]

            # Barging & Transhipping
            self.db.at[1146, col]       =  0.65 * 0.90 
            self.db.at[1147, col]       =  0.50 * 0.90 
            self.db.at[1148, col]       =  1.50 * 0.90 
            self.db.at[1150, col]       = self.db.loc[1146, col] * self.db.loc[769, col] * self.db.loc[37, col] * (1 if self.db.loc[1144, col] == "self-propelled" else (1.1 if self.db.loc[1144, col] == "mixed" else (1.2 if self.db.loc[1144, col] == "dumb" else "error")))
            self.db.at[1151, col]       = self.db.loc[1147, col] * self.db.loc[769, col] * self.db.loc[37, col] * (1 if self.db.loc[1144, col] == "self-propelled" else (1.1 if self.db.loc[1144, col] == "mixed" else (1.2 if self.db.loc[1144, col] == "dumb" else "error")))
            self.db.at[1152, col]       = self.db.loc[1148, col] * self.db.loc[769, col] * self.db.loc[37, col] * (1 if self.db.loc[1144, col] == "self-propelled" else (1.1 if self.db.loc[1144, col] == "mixed" else (1.2 if self.db.loc[1144, col] == "dumb" else "error")))
            self.db.at[1154, col]       = self.db.loc[1150, col] * self.db.loc[5, col] * self.db.loc[1145, col] / 30
            self.db.at[1155, col]       = self.db.loc[1151, col] * self.db.loc[9, col] * self.db.loc[1145, col] / 30
            self.db.at[1156, col]       = self.db.loc[1152, col] * self.db.loc[1145, col] / 30

            self.db.at[1158, col]       = self.db.loc[1154, col] * self.db.loc[1143, col]
            self.db.at[1159, col]       = self.db.loc[1155, col] * self.db.loc[1143, col]
            self.db.at[1160, col]       = self.db.loc[1156, col] * self.db.loc[1143, col]
            self.db.at[1161, col]       = self.db.loc[16, col] * self.db.loc[1143, col]
            self.db.at[1162, col]       = self.db.loc[1158:1161, col].sum()

            # Material movements - total
            self.db.at[1163, col]       = self.db.loc[1087, col]
            
            # Crush Oversize - Total
            self.db.at[1164, col]       = self.db.loc[1105, col]
            
            # Full Crushing - Total
            self.db.at[1165, col]       = self.db.loc[1123, col]
            
            # Drying - total
            self.db.at[1166, col]       = self.db.loc[1141, col]
            
            # Port costs - total
            self.db.at[1167, col]       = self.db.loc[1142, col]
            
            # Barging & Transhipping - total
            self.db.at[1168, col]       = self.db.loc[1162, col]

            # Crushing, Drying, Port, Barging, Transhipping
            self.db.at[1169, col]       = self.db.loc[1163:1168, col].sum()

            # exploitation tax
            self.db.at[1173, col]       = self.db.loc[1172, col] * self.sheet1.loc[0, "LME US$"]
            self.db.at[1174, col]       = max([(self.db.loc[17, col] / 0.40), 1])
            self.db.at[1175, col]       = self.db.loc[1173, col] * self.db.loc[1174, col]
            self.db.at[1176, col]       = (self.db.loc[1175, col] * 1 - self.db.loc[25, col]) * self.db.loc[1171, col]

            # export tax
            self.db.at[1179, col]       = self.db.loc[1178, col] * self.sheet1.loc[0, "LME US$"]
            self.db.at[1180, col]       = max([(self.db.loc[17, col] / 0.40), 1])
            self.db.at[1181, col]       = self.db.loc[1179, col] * self.db.loc[1180, col]
            self.db.at[1182, col]       = (self.db.loc[1181, col] * 1 - self.db.loc[25, col]) * self.db.loc[1177, col]

            # SUMMARY FOB
            self.db.at[1183, col]       = self.db.loc[544, col]
            self.db.at[1184, col]       = self.db.loc[767, col]
            self.db.at[1185, col]       = self.db.loc[1163:1167, col].sum()
            self.db.at[1186, col]       = self.db.loc[1168, col]
            self.db.at[1187, col]       = self.db.loc[10, col] * self.db.loc[1183:1186, col].sum()
            self.db.at[1188, col]       = self.db.loc[1170, col]
            self.db.at[1189, col]       = self.db.loc[1182, col] + self.db.loc[1176, col]
            self.db.at[1190, col]       = self.db.loc[1183:1189, col].sum()
            self.db.at[1191, col]       = self.db.loc[1190, col] / (1 - self.db.loc[25, col])

            self.db.at[1193, col]       = self.db.loc[1191, col] + self.db.loc[1192, col]

            # edit here 
            if f'{col}_1194' in override_store:
                self.db.at[1194, col] = override_store[f'{col}_1194']
            else:    
                self.db.at[1194, col]   = self.db.loc[1183, col] / (1 - self.db.loc[25, col])
            if f'{col}_1195' in override_store:
                self.db.at[1195, col] = override_store[f'{col}_1195']
            else:
                self.db.at[1195, col]       = self.db.loc[1184, col] / (1 - self.db.loc[25, col])
            if f'{col}_1196' in override_store:
                self.db.at[1196, col] = override_store[f'{col}_1196']
            else:
                self.db.at[1196, col]       = self.db.loc[1185, col] / (((1 - self.db.loc[25, col]) + (1 - self.db.loc[26, col])) / 2)
            if f'{col}_1197' in override_store:
                self.db.at[1197, col] = override_store[f'{col}_1197']
            else:
                self.db.at[1197, col]       = self.db.loc[1186, col] / (1 - self.db.loc[26, col])
            if f'{col}_1198' in override_store:
                self.db.at[1198, col] = override_store[f'{col}_1198']
            else:
                self.db.at[1198, col]       = self.db.loc[1187, col] / (1 - self.db.loc[25, col])
            if f'{col}_1199' in override_store:
                self.db.at[1199, col] = override_store[f'{col}_1199']
            else:
                self.db.at[1199, col]       = self.db.loc[1188, col] / (1 - self.db.loc[25, col])
            if f'{col}_1200' in override_store:
                self.db.at[1200, col] = override_store[f'{col}_1200']
            else: 
                self.db.at[1200, col]       = self.db.loc[1189, col] / (1 - self.db.loc[25, col])
            if f'{col}_1201' in override_store:
                self.db.at[1201, col] = override_store[f'{col}_1201']
            else:
                self.db.at[1201, col]       = self.db.loc[1194:1200, col].sum()
            self.db.at[1202, col]       = self.db.loc[1192, col]
            self.db.at[1203, col]       = self.db.loc[1201, col] + self.db.loc[1202, col]

    def save(self):
        if os.path.exists("outputs"):
            pass
        else:
            os.mkdir("outputs")
        snapshot_output_data = pd.DataFrame(columns=g_flat.out_col)
        mod_db = self.db.copy().drop(columns=["Unit"])
        db_list = [
            snapshot_output_data,
            g_flat.single_year_mult_out(mod_db, "Guinea FOB output")
        ]

        snapshot_output_data = pd.concat(db_list, ignore_index=True)

        try:
            override_res = override_rows.values
            for i, v in enumerate(override_rows.index):
                print(snapshot_output_data.loc[v], )
                set_it = snapshot_output_data.loc[v].values
                print(override_res[i][-2:])
                set_it[-2:] = override_res[i][-2:]
                snapshot_output_data.loc[v] = set_it 
        except Exception as err:
            print(err)
            print("Error caught and skipped")

        snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
        self.db.to_csv("outputs/guinea_fob_output.csv", index=False)
        print('uploading to output db')
        
        #snapshot_output_data = snapshot_output_data.drop(['Unnamed: 0'], axis=1)        
        uploadtodb.upload(snapshot_output_data)


# if __name__ == "__main__":
start = time.process_time()

g_fob = GuineaFOB()
g_fob.calcs()
g_fob.save()


end = time.process_time() - start

print(f"{round(end/60, 2)} minutes")
