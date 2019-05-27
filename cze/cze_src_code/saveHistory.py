# coding: utf-8

import pandas as pd
from readHistory import readSrcData as rd
from include import DATA_PATH, TEST_DATA_PATH, year_str, cze_all_symbols, months

def saveAsHdf(y, f):


    x = year_str.index(y)
    year_array = year_str[x:x+3]
#    print year_array

    for i in cze_all_symbols:

        if i == 'WT':
            s = 'PM'
        elif i == 'WS':
            s = 'WH'
        elif i == 'TA':
            s = 'PTA'
        elif i == 'RO':
            s = 'OI'
        elif i == 'ER':
            s = 'RI'
        else:
            s = i

        df = rd(y)

        for k in months:
            df_con = pd.DataFrame()
            for j in year_array:
                z = j[-1:]          #get last char of string 'j', eg: for year '2016', read '6'
                dfa = df.loc[df['symbol']==(i+z+k)]
#                print 's =', s, ' j=', j, ' k=', k, '\t', i + j + k, dfa.empty
                if not dfa.empty:
                    print("Updating symbol %s" % (s + j + k))
                    dfa['symbol'][:] = s+j+k

                    if df_con.empty:
                        df_con = dfa
                    else:
                        df_con = pd.concat([df_con, dfa])

            if not df_con.empty:
                df_con.reset_index(drop=True, inplace=True)
                df_con.set_index(['date', 'symbol'], inplace=True)
#                print df_con, df_con.index
                df_con.to_hdf(f, "".join(["/",s,'/D/',"_",k]), format='table', append=True, data_columns=True, mode='a')
                    #save group name start with '_' or node cannot access by node name
def main():
    year_str = ['10', '11', '12', '13', '14', '15', '16', '17']
    f = pd.HDFStore(DATA_PATH)

    for y in year_str:
        print("Reading Year 20%s data history csv..." % y)
        saveAsHdf(y, f)

    f.flush()
    f.close()


if __name__ == "__main__":
    main()
