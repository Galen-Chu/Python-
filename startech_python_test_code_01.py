import json
import pandas as pd
import re
# 檔案路徑或名稱
file_path='output_clean_date_technical.json'
# 讀取json檔案
with open(file_path,'r') as jsonfile:
    data=json.load(jsonfile)
    # 輸出瞭解JSON檔案格式的樣貌
    # print(data)
    # print(json.dumps(data,indent=4))
    # 輸出瞭解JSON檔案格式的筆數
    # rowcount=0
    # for i in data:
    #     for n in range(len(data[i])):
    #         t=data[i][n]['date']
    #         print(t)
    #         rowcount+=1
    # print(rowcount)
    # 修改欄位資料成為一致的json array陣列
    data['historicalPriceFull']=data['historicalPriceFull']['historical']
    for num in range(len(data['historicalPriceFull'])):
        data['historicalPriceFull'][num]['symbol']='1101.TW'
    # 建立新的欄位標籤以轉換json object物件的key在每一個json array陣列
    for index in data:
        for num in range(len(data[index])):
            data[index][num]['key_label']=index
    # 整合所有資料成為新的json array陣列檔案
    data_array=[]
    for index in data:
        for num in range(len(data[index])):
            data_array.append(data[index][num])
    # 以pandas之DataFrame轉換資料
    data_frame=pd.DataFrame(data_array)
    # 區別特定數值欄位與特定字串欄位
    numeric_cols=data_frame.select_dtypes(include='number').columns
    string_cols=data_frame.select_dtypes(include='object').columns
    for col in numeric_cols:
        data_frame[col]=data_frame[col].fillna(0)
    for col in string_cols:
        data_frame[col]=data_frame[col].fillna('')
    # 統一date欄位之日期格式
    pattern='^\d{4}\D\d{2}\D\d{2} 00:00:00$'
    for index in data_frame.index:
        if re.match(pattern,data_frame.loc[index,'date']):
            pattern='^\d{4}\D\d{2}\D\d{2}'
            data_frame.loc[index,'date']=re.findall(pattern,data_frame.loc[index,'date'])[0]
    # 以pandas之groupby方法合併重複的date及symbol行列
    # 字串欄位判斷是否有重複值進行合併
    agg_funcs={col:lambda x:''.join(sorted(set(x))) for col in string_cols}
    # 數值欄位計算總和
    agg_funcs.update({col:'sum' for col in numeric_cols})
    data_frame_mergedatesymbol=data_frame.groupby(['date','symbol'],as_index=False).agg(agg_funcs)
    # 調整行列排列順序
    data_frame_mergedatesymbol=data_frame_mergedatesymbol.sort_values(by=['symbol','date'],ascending=[False,False],ignore_index=True)
    # 季度period日期轉換
    for index,time in enumerate(data_frame_mergedatesymbol['period']):
        if time:
            year=data_frame_mergedatesymbol.loc[index,"calendarYear"]
            quarter_dict={
                'Q1':f'{year}-01-01~{year}-03-31',
                'Q2':f'{year}-04-01~{year}-06-30',
                'Q3':f'{year}-07-01~{year}-09-30',
                'Q4':f'{year}-10-01~{year}-12-31'}
            data_frame_mergedatesymbol.loc[index,'date']=quarter_dict[time]
    # 決定欄位順序
    columns=['date','symbol','open','high','low',
             'close','adjClose','volume','unadjustedVolume',
             'change','changePercent','vwap','label','changeOverTime',
             'calendarYear','period']
    for key in data_frame_mergedatesymbol.columns:
        if key in columns:
            pass
        else:
            columns.append(key)
    data_frame_sort=data_frame_mergedatesymbol[columns]
    # 新建檔案路徑或名稱
    file_name='output_clean_date_technical.csv'
    # 將整理完之檔案輸出為csv檔
    data_frame_sort.to_csv(file_name,index=True,header=True)
