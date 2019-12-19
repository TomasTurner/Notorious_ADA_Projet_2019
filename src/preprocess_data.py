import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import csv

projectdata_PATH = '../data/projectdata/'
dataprocessed_PATH = '../data/dataprocessed/'

def preprocess_data_file(Filename, newFilename): # for Tax and Liscence
    tempFilename = dataprocessed_PATH + 'tempFile.csv'
    tempFilename1 = dataprocessed_PATH + 'tempFile1.csv'
    text_to_bool(Filename, tempFilename)
    merge_str_as_header(tempFilename, tempFilename1)
    leave_out_nodata(tempFilename1, newFilename)
    
def preprocess_data_file_one(Filename, newFilename): # for health_warning
    tempFilename = dataprocessed_PATH + 'tempFile.csv'
    text_to_bool(Filename, tempFilename)
    leave_out_nodata_one(tempFilename, newFilename)

def preprocess_public_use_data(Filename, newFilename): # for community_actions
    tempFilename = dataprocessed_PATH + 'tempFile.csv'
    tempFilename1 = dataprocessed_PATH + 'tempFile1.csv'
    text_to_number(Filename, tempFilename)
    merge_str_as_header(tempFilename, tempFilename1)
    leave_out_nodata_multi(tempFilename1, newFilename)
    
def preprocess_age_limits(Filename, newFilename): #for age limits
    tempFilename = dataprocessed_PATH + 'tempFile.csv'
    merge_str_as_header(Filename, tempFilename)
    df=pd.read_csv(tempFilename, sep=',')
    #sanitize
    HEURISTIC_TOTAL_BAN = 50
    df=df.replace({'total ban': 'Total ban',
                'subnational': 'Subnational',
                'Total ban': HEURISTIC_TOTAL_BAN,
                'None':0,
                'Subnational': np.nan,
                'No data': np.nan})
    cols = df.columns[2:]
    # sets the dataframe specific data to float
    df[cols] = df[cols].astype(dtype=float)
    # rescale the dataframe specific data  between 0 and 1
    df[cols] = rescale_data(df[cols])
    df.to_csv(newFilename, index=False, sep=',')
    
    
# function for process text data, capitalize, clear out white space and '-', especially for country name
def process_text(origin_text):
    new_text = origin_text.capitalize()
    new_text = ''.join(new_text.split('-'))
    new_text = ''.join(new_text.split(' '))
    return new_text

# function for process country name, call def process_text
def process_country_name(Filename, newFile_PATH, column):
    df = pd.read_csv(Filename, header=None, sep=',')
    for row in df.iterrows():
        country_name = row[1][column]
        if isinstance(country_name,str):
            row[1][column]=process_text(country_name)
    df.to_csv(newFile_PATH, header=None, index=False, sep=',')
    df.head()

# funciton for deal with 'No data', considering about different number of columns
# however can be replaced by dropna function
def leave_out_nodata(Filename, newFile_PATH):
    df = pd.read_csv(Filename, header=None, sep=',')
    df = df[(df[2]!='No data')&(df[3]!='No data')&(df[4]!='No data')]
    df.to_csv(newFile_PATH,header=None,index=False,sep=',')

def leave_out_nodata_one(Filename, newFile_PATH):
    df = pd.read_csv(Filename, header=None, sep=',')
    df = df[df[2]!='No data']
    df.to_csv(newFile_PATH,header=None,index=False,sep=',')
    
def leave_out_nodata_multi(Filename, newFile_PATH):
    df = pd.read_csv(Filename, header=None, sep=',')
    df = df[(df[2]!='No data')&(df[3]!='No data')&(df[4]!='No data')&(df[5]!='No data')&(df[6]!='No data')&(df[7]!='No data')&(df[8]!='No data')&(df[9]!='No data')&(df[10]!='No data')]
    df.to_csv(newFile_PATH,header=None,index=False,sep=',')
    
# dict for file countriy_list_final, input country name and get iso3
def dict_country_code3(inputcountry):
    with open(projectdata_PATH + 'Country_list.csv','r',encoding="utf-8") as f:
        reader = csv.reader(f)
        fieldnames = next(reader) #get first row，as key for the dictionary get from generator next
        csv_reader = csv.DictReader(f,fieldnames=fieldnames) #self._fieldnames = fieldnames   # list of keys for the dict 以list的形式存放键名
        for row in csv_reader:
            d = {}
            for k,v in row.items():
                d[k]=v
            if d['Country']==inputcountry:
                outputcode = d['iso3']
                return(outputcode)
        return False

# dict for file countriy_list_final, input country name and get Region
def dict_country_region(inputcountry):
    with open(projectdata_PATH + 'Country_list.csv','r',encoding="utf-8") as f:
        reader = csv.reader(f)
        fieldnames = next(reader) #get first row，as key for the dictionary get from generator next
        csv_reader = csv.DictReader(f,fieldnames=fieldnames) #self._fieldnames = fieldnames   # list of keys for the dict 以list的形式存放键名
        for row in csv_reader:
            d = {}
            for k,v in row.items():
                d[k]=v
            if d['Country']==inputcountry:
                outputregion = d['Region']
                return(outputregion)
        return False

# dict for file countriy_list_final, input country name and get Religion
def dict_country_religion(inputcountry):
    with open(projectdata_PATH + 'Country_list.csv','r',encoding="utf-8") as f:
        reader = csv.reader(f)
        fieldnames = next(reader) #get first row，as key for the dictionary get from generator next
        csv_reader = csv.DictReader(f,fieldnames=fieldnames) #self._fieldnames = fieldnames   # list of keys for the dict 以list的形式存放键名
        for row in csv_reader:
            d = {}
            for k,v in row.items():
                d[k]=v
            if d['Country']==inputcountry:
                outputreligion = d['Religion']
                return(outputreligion)
        return False
# dict for file iso_countries_list, input iso3 and get iso2
def dict_country_code2(inputcode3):
    with open(projectdata_PATH +'iso_countries_list.csv','r',encoding="utf-8") as f:
        reader = csv.reader(f)
        fieldnames = next(reader)
        csv_reader = csv.DictReader(f,fieldnames=fieldnames) #self._fieldnames = fieldnames   # list of keys for the dict 以list的形式存放键名
        for row in csv_reader:
            d = {}
            for k,v in row.items():
                d[k]=v
            if d['iso3']==inputcode3:
                outputcode2 = d['iso2']
                return(outputcode2)
        return False

# dict for file countries_location_list, input iso2 and get latitude
def dict_country_latitude(inputcode2):
    with open(projectdata_PATH + 'Country_location.csv','r',encoding="utf-8") as f:
        reader = csv.reader(f)
        fieldnames = next(reader)
        csv_reader = csv.DictReader(f,fieldnames=fieldnames) #self._fieldnames = fieldnames   # list of keys for the dict 以list的形式存放键名
        for row in csv_reader:
            d = {}
            for k,v in row.items():
                d[k]=v
            if d['\ufeffiso2']==inputcode2:
                outputlatitude = d['latitude']
                return outputlatitude
        return False

# dict for file countries_location_list, input iso2 and get longitude
def dict_country_longitude(inputcode2):
    with open(projectdata_PATH + 'Country_location.csv','r',encoding="utf-8") as f:
        reader = csv.reader(f)
        fieldnames = next(reader)
        csv_reader = csv.DictReader(f,fieldnames=fieldnames) #self._fieldnames = fieldnames   # list of keys for the dict 以list的形式存放键名
        for row in csv_reader:
            d = {}
            for k,v in row.items():
                d[k]=v
            if d['\ufeffiso2']==inputcode2:
                outputlongitude = d['longitude']
                return outputlongitude
        return False
    
# preprocess add region and religion to each row for policy files
def add_code_region_religion(Filename, newFile_PATH):
    df = pd.read_csv(Filename, header=None, sep=',')
    df['CountryCode']='none'
    df['Region']='none'
    df['Religion']='none'
#     print(df.shape)
    success_count = 0
    for row in df.iterrows():
        country_name = row[1][0]
        country_name = process_text(country_name)
        outputcode = dict_country_code3(country_name)
        outputregion = dict_country_region(country_name)
        outputreligion = dict_country_religion(country_name)
        if outputregion!=False and outputreligion!=False:
            row[1][0]=country_name
            row[1]['CountryCode']=outputcode
            row[1]['Region']=outputregion
            row[1]['Religion']=outputreligion
            success_count = success_count + 1
    print("Successfully Converted:", success_count)
    df.to_csv(newFile_PATH, index=False, header=None, sep=',')
    
    # preprocess countries location
def process_countries(Filename, newFile_PATH):
    df = pd.read_csv(Filename, header=None, sep=',', names=['Number', 'iso3', 'Country', 'Region', 'Religion'])
    df['iso2']='iso2'
    df['latitude']='latitude'
    df['longitude']='longitude'
#     print(df.shape)
    success_count = 0
    for row in df.iterrows():
        country_iso3 = row[1][1]
#         print("iso3", country_iso3)
        outputiso2 = dict_country_code2(country_iso3)
#         print("outputiso2", outputiso2)
        outputlatitude = dict_country_latitude(outputiso2)
        outputlongitude = dict_country_longitude(outputiso2)
        if outputlatitude!=False and outputlongitude!=False:
            row[1]['iso2']=outputiso2
            row[1]['latitude']=outputlatitude
            row[1]['longitude']=outputlongitude
            success_count = success_count + 1
    print("Successfully Converted:", success_count)
    df = df[['Number', 'Country', 'iso2', 'iso3', 'latitude', 'longitude', 'Region', 'Religion'] ]
    df.to_csv(newFile_PATH, index=False, header=None, sep=',')
    
    
    
    
    # preprocess policy data files
def text_to_bool(Filename, new_file_PATH):
    df = pd.read_csv(Filename, header=None, sep=',')
    df.replace('Yes', '1', inplace=True)
    df.replace('No', '0', inplace=True)
    df.replace('Total ban', '1.5', inplace=True)
    df.to_csv(new_file_PATH, header=None, index=False, sep=',')

def text_to_number(Filename, new_file_PATH):
    df = pd.read_csv(Filename, header=None, sep=',')  
    df.replace('ban', '1', inplace=True)
    df.replace('partial restriction', '0.5', inplace=True)
    df.replace('voluntary/self-restricted', '0.3', inplace=True)
    df.replace('no restrictions', '0', inplace=True)
    df.to_csv(new_file_PATH, header=None, index=False, sep=',')
    
    
   
'''
target: merge the string of the two first rows of the dataframe
input: input and output file PATH
output: save df into a csv
'''
    
def merge_str_as_header(Filename, newFile_PATH, row = [0,1], first_2_columns = ['Country', 'Year']): # column name are in row 1 and 2
    df = pd.read_csv(Filename, header=None, sep=',')
    df_data = df[df.columns[2:]]
    new_col_names=[]
    for column_name, _ in df_data.iteritems():
        # checks if the names are different
        if not df.loc[row[0]][column_name] == df_data.loc[row[1]][column_name]:
            new_col_names.append(df_data.loc[row[0]][column_name] + df_data.loc[row[1]][column_name].lower())
        else:
            new_col_names.append(df_data.loc[row[0]][column_name])
    df.columns = first_2_columns + new_col_names
    df=df.drop(labels=row, axis='rows').reset_index(drop=True)
    df.to_csv(newFile_PATH,index=False,sep=',')

'''
target: list all unique values in the dataframe. This can be used to sanitize the data
input: dataframe with the interesting data (here don't put country and year)
output: list with the unique values
'''
def get_unique_values(df):
    val1 = pd.Series()
    for column in df.columns:
        val2 = pd.Series(df[column].unique())
        val1 = val1.append(val2)
    return list(val1.unique())
    

# scales the dataframe to the wanted range
def rescale_data(df, feature_range=(0,1)):
    mm_scaler = MinMaxScaler(feature_range=feature_range)
    df_mm = mm_scaler.fit_transform(df)
    return pd.DataFrame(df_mm)


# functions to add Alcohol consumption means of different regions and religions to each countries
def add_mean1_region(inputregion, df):
    mean = df.loc[df['Region'] == inputregion]['Alcohol_com1']
#     print("len_region", len(mean))
#     print(mean, type(mean), mean.index)
    return float(mean)
def add_mean1_religion(inputreligion, df):
    mean = df.loc[df['Religion'] == inputreligion]['Alcohol_com1']
#     print(inputreligion)
#     print("len_religion", len(mean))
#     print(mean, type(mean), mean.index)
    return float(mean)
def add_mean2_region(inputregion, df):
    mean = df.loc[df['Region'] == inputregion]['Alcohol_com2']
    return float(mean)
def add_mean2_religion(inputreligion, df):
    mean = df.loc[df['Religion'] == inputreligion]['Alcohol_com2']
    return float(mean)