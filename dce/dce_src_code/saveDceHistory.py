# coding: utf-8

import pandas as pd
from readDceHistory import readSrcData as rd

def saveAsHdf(y, f):
    symbols = ['PM', 'WH', 'CF', 'SR', 'PTA', 'OI', 'RI', 'ME', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP',
               'WT', 'WS', 'TA', 'RO', 'ER']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    year_str = ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

    x = year_str.index(y)
    year_array = year_str[x:x+3]
    print (year_array)

    for i in symbols:
        for j in year_array:
            z = j[-1:]
            print ('z = %s' % z)
            for k in months:
                df = rd(y)
                dfa = df.loc[df['symbol'] == (i + z + k)]
                #           dfa = rd.df.loc[re.match(r"([a-z]+)([0-9]+)", rd.df['symbol'],re.I).groups()[0] == i ]
                #            dfa = rd.df.loc[rd.df['symbol'].str.match(r'^i')]
                #            print rd.df['symbol'].str.match(r'^i')
                #            print dfa['symbol'],
                #print dfa['symbol'] == i + j + k
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

                print('s =', s, ' j=', j, ' k=', k, '\t', i + j + k, dfa.empty)

                if not dfa.empty:
                    dfa['symbol'][:] = s + y + k
                    #                dfa.set_index('date', inplace=True)
                    dfa.to_hdf(f, '/'+s +'/D/'+'_' + k, format='table', append=True, data_columns=True, mode='a')
                    #save group name start with '_' or node cannot access by node name
def main():
    year_str = ['10', '11', '12', '13', '14', '15', '16', '17']
    f = pd.HDFStore('../data/cze.hdf5')

    for y in year_str:
        print("Year " + y + '\n')
        saveAsHdf(y, f)

    f.flush()
    f.close()


if __name__ == "__main__":
    main()
