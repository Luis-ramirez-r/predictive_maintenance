from config import input_file, output_file,ventana_features,ventana_falla
import pandas as pd
import numpy as np

def seleccionar_ventanas(df,ventana_features,ventana_falla):

    df_ventana = pd.DataFrame( )
    for id_falla,i_falla in enumerate(df.query('failure != 0').index):
        shift = np.random.randint(1,ventana_falla+1)
        df_temp = df.iloc[i_falla -ventana_features + shift: i_falla + shift].copy()
        df_temp = df_temp.iloc[0:ventana_features-ventana_falla]
        df_temp['id_falla'] = id_falla
        df_temp.iloc[ventana_features-ventana_falla-1,6] = df.iloc[i_falla,6]
        df_temp.iloc[ventana_features-ventana_falla-1,7] = df.iloc[i_falla,7]
        df_ventana = pd.concat([df_ventana,df_temp]) 
    return df_ventana



def seleccionar_ventanas_sin_fallas(ventana_features,ventana_falla,n):

    df_ventana = pd.DataFrame( )
    for ix,id  in enumerate( range(1000,len(df)-1000,800)):
        df_temp = df.iloc[ix:ix+ventana_features].copy()
        if df_temp['failure'].max()!=0:
            continue
        df_temp['id_falla'] = ix
        df_ventana = pd.concat([df_ventana,df_temp])


    if len(df_ventana['id_falla'].unique()) < n:
        return df_ventana 
    else:
        # make a random sample of n ids  
        ids = np.random.choice(df_ventana['id_falla'].unique(),n,replace=False)

        df_ventana = df_ventana.query('id_falla in @ids')
        
        return df_ventana
    
    

df = pd.read_parquet(input_file)
df_ventana = seleccionar_ventanas(df,ventana_features,ventana_falla)
df_ventana.to_parquet(output_file)
