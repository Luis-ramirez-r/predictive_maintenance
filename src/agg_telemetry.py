from config import input_file, output_file,ventana_features,ventana_falla,output_ventanas_sin_fallas,output_ventanas_fallas
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



def seleccionar_ventanas_sin_fallas(df,ventana_features,ventana_falla,n):
    """
    
    """
    df_ventana = pd.DataFrame( )
    for ix,id  in enumerate( range(1000,len(df)-1000,800)):
        df_temp = df.iloc[ix:ix+ventana_features].copy()
        if df_temp['failure'].max()!=0:
            continue
        df_temp['id_falla'] = ix
        df_ventana = pd.concat([df_ventana,df_temp.iloc[:ventana_features-ventana_falla]])


    if len(df_ventana['id_falla'].unique()) < n:
        return df_ventana 
    else:
        # make a random sample of n ids  
        ids = np.random.choice(df_ventana['id_falla'].unique(),n,replace=False)

        df_ventana = df_ventana.query('id_falla in @ids')
        
        return df_ventana


df = pd.read_parquet(input_file)
df_ventana = seleccionar_ventanas(df,ventana_features,ventana_falla)
df_ventana_sin_fallas = seleccionar_ventanas_sin_fallas(df,ventana_features,ventana_falla,len(df_ventana['id_falla'].unique()) )


df_ventana.to_parquet(output_ventanas_fallas)
df_ventana_sin_fallas.to_parquet(output_ventanas_sin_fallas)


df_ventana_agg = df_ventana.groupby('id_falla').agg(mean_volt = ('volt', 'mean'),
                                                mean_rot = ('rotate', 'mean'),
                                                mean_press = ('pressure', 'mean'),
                                                mean_vib = ('vibration', 'mean'),
                                                max_volt = ('volt', 'max'),
                                                max_rot = ('rotate', 'max'),
                                                max_press = ('pressure', 'max'),
                                                max_vib = ('vibration', 'max'),
                                                min_volt = ('volt', 'min'),
                                                min_rot = ('rotate', 'min'),
                                                min_press = ('pressure', 'min'),
                                                min_vib = ('vibration', 'min'),
                                                std_volt = ('volt', 'std'),
                                                std_rot = ('rotate', 'std'),
                                                std_press = ('pressure', 'std'),
                                                std_vib = ('vibration', 'std'),
                                                failure_binary = ('failure_binary', 'max'),
                                                failure = ('failure', 'max')                                             
)  

df_ventana_sin_fallas_agg = df_ventana_sin_fallas.groupby('id_falla').agg(mean_volt = ('volt', 'mean'),
                                                mean_rot = ('rotate', 'mean'),
                                                mean_press = ('pressure', 'mean'),
                                                mean_vib = ('vibration', 'mean'),
                                                max_volt = ('volt', 'max'),
                                                max_rot = ('rotate', 'max'),
                                                max_press = ('pressure', 'max'),
                                                max_vib = ('vibration', 'max'),
                                                min_volt = ('volt', 'min'),
                                                min_rot = ('rotate', 'min'),
                                                min_press = ('pressure', 'min'),
                                                min_vib = ('vibration', 'min'),
                                                std_volt = ('volt', 'std'),
                                                std_rot = ('rotate', 'std'),
                                                std_press = ('pressure', 'std'),
                                                std_vib = ('vibration', 'std'),
                                                failure_binary = ('failure_binary', 'max'),
                                                failure = ('failure', 'max')
                                                
)  

df_resultado = pd.concat([df_ventana_agg,df_ventana_sin_fallas_agg])
df_resultado.to_parquet(output_file)
