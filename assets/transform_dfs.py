import pandas as pd
from datetime import datetime

def _importar_cps():
    """
    Funcion privada que inporta el dataframe de los codigos postales.
    """

    df_cp = pd.read_csv('./assets/codigos_postales.csv')
    df_cp = df_cp.astype('string')
    df_cp['localidad'] = df_cp['localidad'].str.lower()

    return df_cp

def _add_last_name(df, names):
    """
    Funcion privada que separa el last_name del first_name.
    """

    for i, full_name in enumerate(list(names)):
        full_name = full_name.split(sep=' ')
        df.loc[i,'first_name'] = full_name[0]
        df.loc[i,'last_name'] = full_name[1]
    
    return df

def _add_location(df, df_cp):
    """
    Funcion privada que añade el codigo postal o la localidad teniendo el otro dato respectivamente.
    """

    codigo_localidades = {}

    if df.loc[4, 'university'] == 'universidad de palermo':
        for cp, localidad in zip(df_cp['codigo_postal'],df_cp['localidad']):
            codigo_localidades[cp] = localidad

        for i, cp in enumerate(df['postal_code']):
            df.loc[i,'location'] = codigo_localidades[cp]

    elif df.loc[0, 'university'] == 'universidad nacional de jujuy':
        df_c = df_cp[~df_cp['localidad'].duplicated(keep='first')]
        for cp, localidad in zip(df_c['codigo_postal'],df_c['localidad']):
            codigo_localidades[localidad] = cp

        for i, location in enumerate(df['location']):
            df.loc[i,'postal_code'] = codigo_localidades.get(location, ' ')
    
    return df


def transform_df(df):
    """
    Transforma y normaliza los datos de un dataframe.
    """

    df_cp = _importar_cps()
    
    df = df.convert_dtypes()
    df = df.astype('string')

    if df.loc[0, 'university'] == '_universidad_de_palermo':
        df['age'] = pd.to_datetime(df['age'], format='%d/%b/%y')
    else:
        df['age'] = pd.to_datetime(df['age'], format='%Y/%m/%d')

    df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%Y-%m-%d')

    df['first_name'] = df['first_name'].str.replace('_', ' ')
    df['first_name'] = df['first_name'].replace(['^mrs. ', ' dds$', ' iv$', '^dr. ', '^ms. ', ' md$', ' jr.$', '^miss ', '^mr. ', ' dvm$', ' phd$', 'iii$'], '', regex=True)
    df['university'] = df['university'].replace({'_u':'u', '_':' '}, regex=True)
    df['career'] = df['career'].replace({'_$':'', '_':' '}, regex=True)
    df['gender'] = df['gender'].replace({'m':'male', 'f':'female'})

    df = _add_last_name(df, df['first_name'])

    df = _add_location(df, df_cp)

    now = datetime.now()

    df['age'] = df['age'].apply(lambda x: now.year - x.year)

    df = df[df['age']>=18]

    return df


if __name__ == '__main__':
    pass