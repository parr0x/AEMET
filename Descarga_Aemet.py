
import os
from datetime import datetime
import pandas as pd
from os import listdir
import shutil
import numpy as np

def get_valuues (mes=datetime.now().month-1,año=datetime.now().year):
    import wget
    import patoolib
    last_month=datetime(datetime.now().year,datetime.now().month-1,30)
    if last_month < datetime(año,mes,1):
        print('No hay datos')
    else:
        if mes - 10 < 0:
            mes = '0' + str(mes)
        else:
            mes = str(mes)
        print(mes,año)
        url = 'https://datosclima.es/capturadatos/Aemet' + str(año) + '-' + str(mes) + '.rar'
        ruta = os.path.join(os.path.dirname(__file__))
        wget.download(url,ruta)
        temp = ruta + '/Aemet' + str(año) + '-' + str(mes) + '.rar'
        out = ruta + '/Aemet'
        patoolib.extract_archive(temp, outdir=out)
        os.remove(temp)
        df1 = pd.DataFrame()

        for arch in listdir(out):
            df = pd.read_excel(out + '/' + arch, header=4)
            anho = arch[5:9]
            mes = arch[10:12]
            dia = arch[13:15]
            df['Tmax'] = df['Temperatura máxima (ºC)'].astype('str').apply(lambda x: float(x.split(' ')[0]))
            df['Tmin'] = df['Temperatura mínima (ºC)'].astype('str').apply(lambda x: float(x.split(' ')[0]))
            df['Tmed'] = df['Temperatura media (ºC)']
            df['anho'] = anho
            df['mes'] = mes
            df['dia'] = dia
            df['Tref']=23
            df['T0']=0
            df['CDD']= round(df['Tmax']-df['Tref'],3) #unicamente me falta elimnar cuando es negativo.
            df['HDD']= round(df['Tref']-df['Tmin'],3) #unicamente me falta elimnar cuando es negativo.
            df = df[['anho', 'mes', 'dia', 'Estación', 'Provincia', 'Tmax', 'Tmin', 'Tmed','CDD','HDD']]
            df1 = pd.concat([df, df1])
            shutil.move(out + '/' + arch, ruta + '/Cargados')

        salida = ruta + '/Base_Datos/'
        df1.to_csv(salida+'BD.csv', mode='a', sep=';', index=False)
        #df2=round(df1.groupby(by=['anho','mes','Estación']).mean(),3)
        df2 = round(df1.groupby(by=['anho', 'mes', 'Estación']).agg(
            {
                'Tmed':np.mean,
                'Tmax':np.mean,
                'Tmin': np.mean,
                'HDD':np.sum,
                'CDD': np.sum
            }
        ), 3)
        df2.to_csv(salida+'promedio.csv', mode='a', sep=';', index='True')
        df3=round(df1.pivot_table(index=['anho', 'mes','Estación'],values=['Tmed','Tmax','Tmin','CDD','HDD'],aggfunc= np.mean),3)
        df3.to_csv(salida+'promedio2.csv', mode='a', sep=';', index='True')

get_valuues(8)

# for i in range(1, 13):
#     for y in range(2015, 2021):
#         get_valuues(i, y)

