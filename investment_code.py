import numpy as np
import pandas as pd
import re
import urllib
from urllib.request import urlopen
import requests

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

'''Importing Datasets'''
#comp=pd.read_csv(r'D:\Learning & Development\IIIT Data Science\Course 1\Module 3\cmpo.csv',sep='\s+',engine='python',encoding='latin1',error_bad_lines=False)
round2=pd.read_csv(r'D:\Learning & Development\IIIT Data Science\Course 1\Module 3\rounds2.csv', encoding = 'ANSI')
mapping=pd.read_csv(r'D:\Learning & Development\IIIT Data Science\Course 1\Module 3\mapping.csv', encoding = 'ANSI')
url = "https://cdn.upgrad.com/UpGrad/temp/d934844e-5182-4b58-b896-4ba2a499aa57/companies.txt"
urllib.parse.unquote(url)
r = requests.get(url)
companies = None
if r.status_code == 200: 
    rows  = r.text.split('\r\n')
    col = rows[0].split('\t')
    data = []
    for n in range(1, len(rows)):
        cols = rows[n].split('\t')
        data.append(cols)
    companies = pd.DataFrame(columns=col, data=data)
else:
    print("error: unable to load {}".format(url))
    sys.exit(-1)

companies=companies.mask(companies.eq('None')).dropna()
companies.to_csv('D:\Learning & Development\IIIT Data Science\Course 1\Module 3\companies_extracted.csv')
companies1=pd.read_csv(r'D:\Learning & Development\IIIT Data Science\Course 1\Module 3\companies_extracted.csv', encoding = 'ISO-8859-1')
companies=companies1.drop('Unnamed:0',axis=1,inplace=True)



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

'''Checkpoint 1: Data Cleaning 1 '''

'''Unique Companies in Rounds2 and Companies '''
#lowercase on uniques keys
round2['company_permalink']=round2['company_permalink'].str.lower()
companies['permalink']=companies['permalink'].str.lower()
uni_round=round2['company_permalink'].unique()
uni_comp=companies['permalink'].unique()


'''Are there any companies in the rounds2 file which are not  present in companies ?'''
df1=pd.DataFrame(round2['company_permalink'])
df2=pd.DataFrame(companies['permalink'])
df1.drop_duplicates()
df2.drop_duplicates
df_final=pd.merge(df1,df2,left_on='company_permalink',right_on='permalink',how='left',indicator=True)
len((df_final['_merge']=='left_only').tolist())
#no there are no sucg companies which are present in rounds2 dataframe and not in companies dataframe

'''Merging dataframes Rounds2 and Companies '''
masterframe1=pd.merge(round2,companies,how='left',left_on='company_permalink',right_on='permalink')

'''Converting raised_amount_usd attribute to integer from hexa format'''
#first converting the column in numeric
#number of missing values in raised_amount_usd column
masterframe1['raised_amount_usd'].isnull().sum()
#assuming the null values represent zero investment, therefore replacing missing values with 0
masterframe1['raised_amount_usd']=masterframe1['raised_amount_usd'].fillna(0)
masterframe1['raised_amount_usd'].isnull().sum() #after imputing missing values, missing value count is 0
masterframe1['raised_amount_usd'] = pd.to_numeric(masterframe1['raised_amount_usd'], errors='coerce')
masterframe1['raised_amount_usd'].head()
#dropping unwanted columns
master_frame=masterframe1.drop(['funding_round_code','homepage_url','status','state_code','region','city','founded_at'],axis=1)



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

'''Checkpoint 2: Funding Type Analysis '''

'''Average funding amount of venture type'''
#uisng as_index to get results in dataframe if do not use then the result will be series
avg_invst=(master_frame.groupby(master_frame['funding_round_type'])['raised_amount_usd'].mean()).reset_index()
avg_invst['raised_amount_usd']=avg_invst['raised_amount_usd'].astype('int64')
avg_ven=(master_frame.groupby(master_frame['funding_round_type']=='venture',as_index=False)['raised_amount_usd'].mean()).astype(int)
avg_angel=(master_frame.groupby(master_frame['funding_round_type']=='angel',as_index=False)['raised_amount_usd'].mean()).astype(int)
avg_seed=(master_frame.groupby(master_frame['funding_round_type']=='seed',as_index=False)['raised_amount_usd'].mean()).astype(int)
avg_pvteq=((master_frame.groupby(master_frame['funding_round_type']=='private_equity',as_index=False)['raised_amount_usd'].mean()).astype(int))

sum_invst=(master_frame.groupby(master_frame['funding_round_type'])['raised_amount_usd'].sum()).astype('int64').reset_index()



#investment between 5 to 15 million USD per investment round
invt_type=(avg_invst[avg_invst['raised_amount_usd'].between(5000000,15000000, inclusive=True)])



#as per the output venture is most suitable investment type to make an investment between 5-15million USD



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

'''Checkpoint 3: Country Analysis'''

'''Getting Top9 countries with maximum investment'''
#creating database for english speaking countries using sources like wikipedia
eng_cnt=pd.read_csv(r'D:\Learning & Development\IIIT Data Science\Course 1\Module 3\english_speaking.csv')
#merging the english peaking countries with the master_frame to add new columns having flag for english speaking countries as1
masterframe=pd.merge(master_frame,eng_cnt,how='left',left_on='country_code',right_on='ISO_Code')
#creating dataframe for the venture investment type and only english speaking countries to find the maximum investment
#done in each country
top=masterframe[(masterframe['funding_round_type']=='venture') & (masterframe['English_Speaking']==1)]
top_9=(top.groupby(master_frame['country_code'])['raised_amount_usd'].sum())
top_9=top_9.reset_index()
top_9['raised_amount_usd']=top_9['raised_amount_usd'].astype('int64')
top9=top_9.nlargest(9,['raised_amount_usd'])

'''
Indentifying the top 3 english speaking countries from dataframe top9 using the pdf provided for countries.
Top 3 countries are USA, GBR & IND.
'''


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

'''Checkpoint 4: Sector Analysis 1'''

'''Creating Columns with Primary Category in master_frame dataframe'''
new = master_frame['category_list'].str.split("|", n = 1, expand = True)
master_frame['prim_cate']=new[0]
#finding the unqiues values before merging two dataframes
master_frame['prim_cate']=master_frame['prim_cate'].str.lower()
mapping['category_list']=mapping['category_list'].str.lower()
x=master_frame['prim_cate'].unique().tolist()
y=mapping['category_list'].unique().tolist()

'''
category_list column of mapping1 df has discrepancy, as in many values "na" is replaced with "0". 
For e.g. "Analytics" is written as "A0lytics".
So converting "0" with "na" using gsub function.
replacing the 0 with na, for updating the correcat spelling'''
mapping=mapping.replace('0', 'na', regex=True)

'''merging two dataframes to map the primary sectors with 8 main sectors'''
master_sec=pd.merge(master_frame,mapping,how='left',left_on='prim_cate',right_on='category_list')
#master_sec.to_csv('D:\Learning & Development\IIIT Data Science\Course 1\Module 3\master_extracted.csv')


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

'''Checkpoint 5: Sector Analysis 2'''


'''So far doing the analysis, we have found that most suitable funding type is "venture" and the top three english 
speaking countries where investment is occuring are USA, GBR & IND. 
Finding the 3 data frames for each of the three countries.
'''

'''three dataframes for each top 3 countries USA,GBR and IND'''
D1=master_sec.loc[(master_sec['funding_round_type']=='venture') & (master_sec['country_code']=='USA')]
D2=master_sec.loc[(master_sec['funding_round_type']=='venture') & (master_sec['country_code']=='GBR')]
D3=master_sec.loc[(master_sec['funding_round_type']=='venture') & (master_sec['country_code']=='IND')]

main_sector=["Automotive & Sports",'Blanks',"Cleantech / Semiconductors",'Entertainment','Health','Manufacturing',"News, Search and Messaging",'Others',"Social, Finance, Analytics, Advertising"]
#finding missing values for main sectors
D1[main_sector].isnull().sum()
D2[main_sector].isnull().sum()
D3[main_sector].isnull().sum()

#imputing missing values with 0
D1[main_sector]=D1[main_sector].fillna(0)
D1[main_sector].isnull().sum()
D2[main_sector]=D2[main_sector].fillna(0)
D2[main_sector].isnull().sum()
D3[main_sector]=D3[main_sector].fillna(0)
D2[main_sector].isnull().sum()

'''Total number of Investments (count)'''
d1_usa_c=(D1.groupby(main_sector)['raised_amount_usd'].apply(lambda x : x.astype('int64').count())).reset_index()
d2_gbr_c=(D2.groupby(main_sector)['raised_amount_usd'].apply(lambda x : x.astype('int64').count())).reset_index()
d3_ind_c=(D3.groupby(main_sector)['raised_amount_usd'].apply(lambda x : x.astype('int64').count())).reset_index()

'''Total amount of investment (USD)'''
d1_usa=(D1.groupby(main_sector)['raised_amount_usd'].apply(lambda x : x.astype('int64').sum())).reset_index()
d2_gbr=(D2.groupby(main_sector)['raised_amount_usd'].apply(lambda x : x.astype('int64').sum())).reset_index()
d3_ind=(D3.groupby(main_sector)['raised_amount_usd'].apply(lambda x : x.astype('int64').sum())).reset_index()

'''For point 3 (top sector count-wise), which company received the highest investment? i.e. company with max investment in Others as main sector'''
mappings=mapping
mappings=pd.melt(mapping,id_vars=['category_list'],var_name='mainsector')
master_sec1=pd.merge(master_frame,mappings,how='left',left_on='prim_cate',right_on='category_list')
master_sec1['value'].isnull().sum()
master_sec1['value']=master_sec1['value'].fillna(0)
master_sec1['raised_amount_usd'].fillna(0)

'''USA and Others'''
comp_usa=master_sec1.loc[(master_sec1['mainsector']=='Others') & (master_sec['country_code']=='USA')]
top_usa=(comp_usa.groupby(['permalink','mainsector'])['raised_amount_usd'].sum()).astype('int64').reset_index()
invt_typesd=(top_usa[top_usa['raised_amount_usd'].between(5000000,15000000, inclusive=True)])
top_comp_usa=invt_typesd.nlargest(1,['raised_amount_usd'])

'''GBR and Others'''
comp_gbr=master_sec1.loc[(master_sec1['mainsector']=='Others') & (master_sec['country_code']=='GBR')]
top_gbr=(comp_gbr.groupby(['permalink','mainsector'])['raised_amount_usd'].sum()).astype('int64').reset_index()
invt_typesd1=(top_gbr[top_gbr['raised_amount_usd'].between(5000000,15000000, inclusive=True)])
top_comp_gbr=invt_typesd1.nlargest(1,['raised_amount_usd'])

'''IND and Others'''
comp_ind=master_sec1.loc[(master_sec1['mainsector']=='Others') & (master_sec['country_code']=='IND')]
top_ind=(comp_ind.groupby(['permalink','mainsector'])['raised_amount_usd'].sum()).astype('int64').reset_index()
invt_typesd2=(top_ind[top_ind['raised_amount_usd'].between(5000000,15000000, inclusive=True)])
top_comp_ind=invt_typesd2.nlargest(1,['raised_amount_usd'])


'''For point 4 (second best sector count-wise), which company received the highest investment?'''

'''USA and Cleantech / Semiconductors '''
comp2_usa=master_sec1.loc[(master_sec1['mainsector']=="Cleantech / Semiconductors") & (master_sec['country_code']=='USA')]
top2_usa=(comp2_usa.groupby(['permalink','mainsector'])['raised_amount_usd'].sum()).astype('int64').reset_index()
invt2_typesd=(top2_usa[top2_usa['raised_amount_usd'].between(5000000,15000000, inclusive=True)])
top2_comp_usa=invt2_typesd.nlargest(1,['raised_amount_usd'])

'''GBR and Social, Finance, Analytics, Advertising'''
comp2_gbr=master_sec1.loc[(master_sec1['mainsector']=="Social, Finance, Analytics, Advertising") & (master_sec['country_code']=='GBR')]
top2_gbr=(comp2_gbr.groupby(['permalink','mainsector'])['raised_amount_usd'].sum()).astype('int64').reset_index()
invt2_typesd1=(top2_gbr[top2_gbr['raised_amount_usd'].between(5000000,15000000, inclusive=True)])
top2_comp_gbr=invt2_typesd1.nlargest(1,['raised_amount_usd'])

'''IND and Social, Finance, Analytics, Advertising'''
comp2_ind=master_sec1.loc[(master_sec1['mainsector']=="Social, Finance, Analytics, Advertising") & (master_sec['country_code']=='IND')]
top2_ind=(comp2_ind.groupby(['permalink','mainsector'])['raised_amount_usd'].sum()).astype('int64').reset_index()
invt2_typesd2=(top2_ind[top2_ind['raised_amount_usd'].between(5000000,15000000, inclusive=True)])
top2_comp_ind=invt2_typesd2.nlargest(1,['raised_amount_usd'])
