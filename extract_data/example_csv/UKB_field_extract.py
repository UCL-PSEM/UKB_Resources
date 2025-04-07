import pandas as pd
import numpy as np
import argparse
import sys

def create_list_fields(field, instance, idx_max):
    # field = x['field']
    # instance = x['instance']
    # idx_max = x['idx_max']
    list_fields = []
    for k in range(idx_max):
        list_fields.append(str(field)+'-'+str(instance)+'.'+str(k))
    return list_fields

def create_listcols(df_list):
    df_list_unique = df_list[df_list['idx_max']==0]
    df_list_multi = df_list[df_list['idx_max']>0]
    df_list_unique['final_field'] = df_list_unique['field'].astype(str)+'-'+df_list_unique['instance'].astype(str)+'.0'
    df_list_multi['final_field'] = df_list_multi.apply(lambda x: create_list_fields(x['field'],x['instance'],x['idx_max']),axis=1)
    list_multi_init = list(df_list_multi['final_field'])
    list_multi = [x for xs in list_multi_init for x in xs]
    list_tot = ['eid',] + list(df_list_unique['final_field']) + list_multi
    return list_tot

def select_columns(df, list_fields):
    selected_df = df[list_fields]
    return selected_df

def encode_columns(df_sel, df_encode):
    list_field = list(df_encode['field'])
    list_code = list(df_encode['code'])
    list_value = list(df_encode['value'])
    list_encoded = []
    for (f,v,c) in zip(list_field, list_value, list_code):
        df_sel[c] = 0
        for k in df_sel.columns:
            if str(f)+'-' in k:
                print(k, v, c)
                df_sel[c] += df_sel[k].astype(str).str.contains(v)
                list_encoded.append(k)
    df_sel2 = df_sel.drop(columns=list_encoded)
    return df_sel2




def main(argv):
    parser = argparse.ArgumentParser()
       
    parser.add_argument('-file', dest='file', required=True)
    parser.add_argument('-head',dest='header',required=True)
    parser.add_argument('-fields',dest='fields',required=True)
    parser.add_argument('-encoding',dest='encoding',default='')
    parser.add_argument('-name',dest='name')
    
    
    args = parser.parse_args(argv)


    df_all = pd.read_csv(args.file)
    df_head = pd.read_csv(args.header)
    df_fields = pd.read_csv(args.fields)
    df_all.columns = df_head.columns
    list_fields = create_listcols(df_fields)
    selected_df = select_columns(df_all, list_fields)
    if args.encoding != '':
        df_encode = pd.read_csv(args.encoding)
        selected_df = encode_columns(selected_df, df_encode)
    selected_df.to_csv(args.file.split('.csv')[0]+'_'+args.name+'.csv')


if __name__ == "__main__":
    # arg_array = ['-file','/Users/carolesudre/Data/UKB/splitnew_aa',
    #              '-head','/Users/carolesudre/Data/UKB/splitnew_aa',
    #              '-fields','/Users/carolesudre/Data/UKB/fields_Xin.csv',
    #              '-name','extractXin2',
    #              '-encoding','/Users/carolesudre/Data/UKB/encoding_field.csv']
    # main(arg_array) 
    #/Users/carolesudre/Development/UKB_reading.py
    main(sys.argv[1:])