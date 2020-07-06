import pandas as pd
import os.path
import math

#open output file to write output to
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(path+'/output/repeat_donors.txt')

try:
    repeat_donors = open(path+'/output/repeat_donors.txt','w')
except FileNotFoundError as e:
    print(e)

#read value of the percentile to calculate from percentile.txt
perc = open(path+'/input/percentile.txt','r')
percentileVal = perc.read()
perc.close() #close percentile.txt


# Read in data in a chunk of size of 500,000 records 
# Add column names as header for simplicity and only load the needed columns
attributes = ['CMTE_ID','AMNDT_IND','RPT_TP','TRANSACTION_PGI','IMAGE_NUM',
'TRANSACTION_TP','ENTITY_TP','NAME','CITY','STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION',
'TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID','TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID']

data = pd.read_csv(path+'/input/itcont.txt', sep='|', iterator=True, names=attributes, usecols=['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID'])
df = data.get_chunk(500000)

#drop all rows where OTHER_ID is not empty to exclude entities who donated
#drop all rows where NAME is empty
df.drop(df[df['OTHER_ID'] > '0'].index, inplace=True)
df.dropna(axis=0, subset=['NAME'], inplace=True)

# Change the zip code column so that all valuse are 5 digits only
# Drop rows where the zip code is invalid
df['ZIP_CODE'] = df['ZIP_CODE'].astype(str).str[0:5]
df.drop(df[df['ZIP_CODE'].str.len() < 5].index, inplace=True)

#reformat the date so it is consistent and so we can find invalid dates
#drop rows with invalid dates
df['TRANSACTION_DT'] = pd.to_datetime(df['TRANSACTION_DT'], format='%m%d%Y', errors='coerce')
#print(df['TRANSACTION_DT'].isnull().values.any())
df.dropna(axis=0, subset=['TRANSACTION_DT'], inplace=True)

#drop any rows where TRANSACTION_AMT ro CMTE_ID are empty
df.dropna(axis=0, subset=['TRANSACTION_AMT'], inplace=True)
df.dropna(axis=0, subset=['CMTE_ID'], inplace=True)


#nearest-rank method percentile contribution
def percentile(amountsLen, p):
    x = (float(p)/100)*amountsLen
    n = math.ceil(x)
    return int(n)



#Getting repeat donors information
trans = {}

for i in df.index:
    key = df.loc[i,'CMTE_ID']
    year = df.loc[i,'TRANSACTION_DT'].year
    name = df.loc[i,'NAME']
    zipcode = df.loc[i,'ZIP_CODE']
    amt = df.loc[i,'TRANSACTION_AMT']
    
    #add the CMTE_ID as the dict key if it does not already exist
    if key in trans.keys():

        #if key exists then check if it is a repeating donor
        if name in trans[key] and zipcode == trans[key][name]['ZIP']:
            qty = trans[key]['QTY']
            total = trans[key]['TOTAL']
            if year not in trans[key][name]['YEAR']:
                qty += 1
                trans[key]['QTY'] += 1
                trans[key][name]['YEAR'].append(year)
                total += amt
                trans[key]['TOTAL'] += amt
                trans[key]['AMTS'].append(amt)

                #calculate percentile and add it to formatted string
                ordinalRank = percentile(len(trans[key]['AMTS']),percentileVal)
                trans[key]['AMTS'].sort()
                p = trans[key]['AMTS'][ordinalRank-1]
                
                #write formatted string to the output file
                s = "{0}|{1}|{2}|{3}|{4}|{5}\n"
                repeat_donors.write(s.format(key,zipcode,year,p,total,qty))

        else:
            trans[key][name] = {}
            trans[key][name]['ZIP'] = zipcode
            trans[key][name]['YEAR']=[year]

    else:
        trans[key] = {}
        trans[key][name] = {}
        trans[key][name]['ZIP'] = zipcode
        trans[key][name]['YEAR'] = [year]
        trans[key]['TOTAL'] = 0
        trans[key]['QTY'] = 0
        trans[key]['AMTS'] = []


# close repeat_donors.txt
repeat_donors.close()
