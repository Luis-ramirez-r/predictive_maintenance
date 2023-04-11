from config import input_file, output_file,ventana_features,ventana_falla,output_ventanas_sin_fallas,output_ventanas_fallas,n_samples
import pandas as pd
import numpy as np

import pandas as pd
import numpy as np

class VentanaSelector:
    def __init__(self, df, ventana_features, ventana_falla):
        self.df = df
        self.ventana_features = ventana_features
        self.ventana_falla = ventana_falla
        self.df_ventana = pd.DataFrame()
        self.id_falla = 0

    def seleccionar_ventanas(self):
        for i_falla in self.df.query('failure != 0').index:
            shift = np.random.randint(1, self.ventana_falla + 1)
            start_index = i_falla - self.ventana_features + shift
            end_index = i_falla + shift

            df_temp = self.extract_window(start_index, end_index)
            if df_temp is not None:
                df_temp['id_falla'] = self.id_falla
                self.update_failure_information(df_temp, i_falla)
                self.df_ventana = pd.concat([self.df_ventana, df_temp])
                self.id_falla += 1

        return self.df_ventana

    def seleccionar_ventanas_sin_fallas(self, n_samples, start_index=1000, step=800):
        df_ventana = pd.DataFrame()

        for  ix in range(start_index, len(self.df) - 1000, step):
            df_temp = self.df.iloc[ix:ix + self.ventana_features].copy()
            if df_temp['failure'].max() != 0 or len(df_temp['machineID'].unique()) > 1:
                continue
            df_temp['id_falla'] = self.id_falla
            df_ventana = pd.concat([df_ventana, df_temp.iloc[:self.ventana_features - self.ventana_falla]])
            self.id_falla += 1
        if len(df_ventana['id_falla'].unique()) < n_samples:
            return df_ventana
        else:
            ids = np.random.choice(df_ventana['id_falla'].unique(), n_samples, replace=False)
            df_ventana = df_ventana.query('id_falla in @ids')
            return df_ventana

    def extract_window(self, start_index, end_index):
        df_temp = self.df.iloc[start_index: end_index].copy()
        machid = df_temp['machineID'].unique()

        if len(machid) > 1 :
            return None

        return df_temp.iloc[0:self.ventana_features - self.ventana_falla]

    def update_failure_information(self, df_temp, i_falla):
        index = self.ventana_features - self.ventana_falla - 1
        df_temp.iloc[index, 6] = self.df.iloc[i_falla, 6]
        df_temp.iloc[index, 7] = self.df.iloc[i_falla, 7]




df = pd.read_parquet(input_file)


selector = VentanaSelector(df, ventana_features, ventana_falla)
df_ventana = selector.seleccionar_ventanas()
df_ventana_sin_fallas = selector.seleccionar_ventanas_sin_fallas(n_samples)



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
