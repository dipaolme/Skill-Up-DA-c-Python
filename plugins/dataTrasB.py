

from sqlCommandB import createPath, csvFile, txtFile

import pandas as pd
import random
from datetime import date
import csv
import datetime


def sepDat(data, sepv):
    N = len(data)
    dat_sep = [data[i].split(sepv) for i in range(N)]
    dat_sep = pd.Series(dat_sep)
    return dat_sep


def dateInsc(da):
    td = date.today()
    c = td.year - 1918
    nc = random.randrange(0, 15, 1)
    if c > 15:
        c = td.year-1-nc
    s = str(c)+"-"+da[1]+"-"+da[2]
    return s


def dateConvertI(da):

    #
    s = "20"+da[2]+"-"+da[1]+"-"+da[0]
    fecha = datetime.datetime.strptime(
        "{0}".format(s), "%Y-%b-%d").strftime("%Y-%m-%d")
    return fecha


def saveTxt(dat_file, df):
    path_datasets = createPath("datasets")
    dat_file_name = path_datasets+'/'+dat_file
    df.to_csv(dat_file_name, sep="\t", quoting=csv.QUOTE_NONE,
              escapechar=" ", index=False)


def dateConvert(da):
    num = int(da[2])
    if num > 5:
        rf = random.randrange(5, 40, 1)
#         print(rf)
        rf = rf
        if rf >= 0 and rf < 5:
            #             print(rf)
            year = "200"+str(rf)
        else:
            if rf < 10:
                year = "199"+str(rf)
            else:
                year = "19"+str(rf)
    else:
        year = "200"+str(num)

    s = year+"-"+da[1]+"-"+da[0]
    fecha = datetime.datetime.strptime(
        "{0}".format(s), "%Y-%b-%d").strftime("%Y-%m-%d")
    return fecha


def sepDat(data, sepv):
    N = len(data)
    dat_sep = [data[i].split(sepv) for i in range(N)]
    dat_sep = pd.Series(dat_sep)
    return dat_sep


def uniqueR(datR):

    sd3 = [dat for dat in datR if len(dat) <= 3]

    sd3 = list(set(sd3))
    return sd3


def uniqueI(datR):

    datR = list(datR)

    datR.append('MS.')
    datR.append('MISS')
    mr = list(set(datR))

    return mr


def calculateAge(dat):
    birthdate = date.fromisoformat(dat)
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) <
                                         (birthdate.month, birthdate.day))

    if age > 40:
        age = random.randrange(17, 40, 2)
    elif age < 17:
        age = random.randrange(17, 40, 2)
    return age


def concatDat(dat):
    dat = dat.split('_')
    cadena = ""
    # for i in dat:
    #     cadena =cadena+" "+i
    #     i = cadena
    cadena = " ".join(dat)
    return cadena


def rnam(df):
    f = " ".join(df)
    return f


def data_final(df1):
    names1 = df1[df1.columns[3]]
    path_cp = createPath("assets")
    file_cp = csvFile(path=path_cp, select="codigos_postales.csv")

    dfp = pd.read_csv(file_cp)
    names = names1

    if "_" in names[0]:
        names_e = sepDat(names, "_")
    elif " " in names[0]:
        names_e = sepDat(names, " ")
    else:
        names_e = sepDat(names, " ")

    name = pd.Series(names_e)

    nl = pd.Series(list(map(len, name)))

    dat4 = nl[:] == 4
    dat3 = nl[:] == 3
    dat2 = nl[:] == 2

    i = nl[dat4].index
    k = nl[dat3].index
    j = nl[dat2].index

    datindex4 = list(i)
    datindex3 = list(k)
    datindex2 = list(j)
    # names[i]

    # d4 = [print(names[datindex[i]]) for i in range(len(datindex))]
    d4 = [name[datindex4[i]] for i in range(len(datindex4))]

    d3 = [name[datindex3[i]] for i in range(len(datindex3))]

    d2 = [name[datindex2[i]] for i in range(len(datindex2))]

    N34 = len(d4)+len(d3)
    N3 = len(d3)
    N4 = len(d4)
    N2 = len(d2)

    Nt = N3+N4+N2

    # d4

    d4 = pd.DataFrame(d4)

    mr = d4[0].unique()
    sd = d4[3].unique()
    d4 = d4.drop(columns=[0, 3])

    d4.columns = [df1.columns[3], df1.columns[4]]

    # print(N4 == len(d4))
    d4.index = datindex4

    # d3

    d3 = pd.DataFrame(d3)
    sd3 = d3[2].unique()
    sd3 = list(sd)

    sd3 = uniqueR(d3[2].unique())
    mr3 = uniqueI(mr)
    d3.index = datindex3

    # Join columns 3 to 2
    d = pd.DataFrame(columns=[1, 2])

    na = []
    for i in range(len(mr3)):
        mr0 = d3[0] == mr3[i]
        d3t = d3[mr0]
        nam = d3t.drop(columns=[0])

        d = pd.concat([nam, d], sort=True)

    dat3 = d

    d.columns = [0, 1]
    e = pd.DataFrame(columns=[0, 1])
    na = []
    for i in range(len(sd3)):
        mr0 = d3[2] == sd3[i]
        d3t = d3[mr0]
    #     print(d3t)
        nam = d3t.drop(columns=[2])
    #     print(nam)

        d = pd.concat([nam, d], sort=True)
        e = pd.concat([nam, e], sort=True)

    d.columns = [df1.columns[3], df1.columns[4]]
    d3 = d
    N3 == len(d3)
    N4 == len(d4)

    d34 = pd.concat([d3, d4])

    N34 == len(d34)

    #  d2

    d2 = pd.DataFrame(d2)
    d2.index = datindex2

    d2.columns = [df1.columns[3], df1.columns[4]]

    d234 = pd.concat([d34, d2])
    # d234.sort_index()
    # print(len(d234)== Nt)

    d234 = d234.reset_index()
    d234 = d234.sort_values(by="index")
    d234.index = d234['index']

    df1['first_name'] = d234['first_name']
    df1['last_name'] = d234['last_name']

    df1.gender = df1.gender.replace({"M": "male", "F": "female"})

    # df1.age =df1.age.apply(calculateAge)

    N = len(df1.university)
    if "_" in df1.university[0]:
        dat_uni = sepDat(df1.university, "_")
        df1.university = df1.university.apply(concatDat)

    if "_" in df1.career[0]:
        dat_car = sepDat(df1.career, "_")
        df1.career = df1.career.apply(concatDat)

    if df1.postal_code[0] == None:
        if "_" in df1.location[0]:
            dat_car = sepDat(df1.location, "_")
            df1.location = df1.location.apply(concatDat)

        for i in range(N):

            pos = dfp.localidad == df1.location[i]
            find_loc = dfp[pos]

            daf = find_loc

            da = daf.index

            n = len(da)
            rf = random.randrange(0, n, 1)
            daf.iloc[rf].values[0]

            df1.postal_code[i] = daf.iloc[rf].values[0]
    else:
        #         print("post")
        post = df1.postal_code.unique()

        for i in post:

            cp = i
            find_cp = dfp['codigo_postal'] == cp
            find_localidad = dfp[find_cp]['localidad']
            find_localidad = find_localidad.values[0]
            valP = df1.postal_code == cp
            pos = df1[valP]
            df1.loc[pos.index, 'location'] = find_localidad

    if df1.age.dtype == 'object':
        try:
            df1.age = df1.age.apply(calculateAge)

        except:
            datfecha = df1.age.values

            da = sepDat(datfecha, "-")

            das = da.apply(dateConvert)

            df1.age = das.apply(calculateAge)
    datI = df1.inscription_date.values

    da = sepDat(datI, '-')

    try:
        df1.inscription_date = da.apply(dateConvertI)
    except:
        df1.inscription_date = df1.inscription_date

    df1.postal_code = df1.postal_code.astype('int64')
    dob = df1.select_dtypes(include=['object'])
    obc = dob.columns
    for i in obc:
        df1[i] = df1[i].str.lower()
    
    dg = df1.career.str.split()


    df1.career = dg.apply(rnam)
    return df1


def data_transform(data):

    name_dat = data.split("/")
    name_dat = name_dat[-1].split("_")
    print(name_dat)
    name_dat = name_dat[0]
    print(name_dat)
    ext = "_process.txt"
    dat_file_name = name_dat+ext
    print(dat_file_name)

    df = pd.read_csv(data)
    ds = pd.Series(['university', 'career', 'inscription_date', 'first_name',
                   'last_name', 'gender', 'age', 'postal_code', 'location', 'email'])

#     ds = pd.DataFrame(['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email'])

    ds = ['university', 'career', 'inscription_date', 'first_name',
          'last_name', 'gender', 'age', 'postal_code', 'location', 'email']

    df.drop(columns=df.columns[0], inplace=True)

    col = df.columns
#     print(len(col),":",len(ds))

    lastname = ds[4]

    df.insert(4, lastname, 4, allow_duplicates=False)
    text1 = "localidad"
    text2 = "codigo_postal"

    df.head()
    col = df.columns
#     print(len(col),":",len(ds))

    colL = list(col)

    if text2 in col:
        #         print(text2)
        n = colL.index(text2)
#         print(n)

        location = ds[n+1]
#         print(location)
        df.insert(n+1, location, 2, allow_duplicates=False)
        df.head()

    elif text1 in col:
        #         print(text1)
        n = colL.index(text1)
#         print(n)
        location = ds[n]
#         print(location)
        df.insert(n, location, None, allow_duplicates=False)
        df.head()

    df.columns = ds
    col = df.columns

    dataFinal = data_final(df)
    # print(dat_file_name)

    saveTxt(dat_file_name, dataFinal)


if __name__ == "__main__":

    # data = "./files/GBUNComahue_select.csv"
    data = "./files/GBUNSalvador_select.csv"
    data_transform(data)
