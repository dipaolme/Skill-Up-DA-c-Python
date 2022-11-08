import pandas as pd
from sqlCommandB import txtFile
# from sqlCommandB import csvFile

# university, career, inscription_date, first_name, last_name, gender, age, postal_code, location, email
def dataTransf(data):
    print("DATATRANS")
    # df = pd.read_json(data)
    df = pd.read_csv(data)
    fileSelect = txtFile("./assets", "columns_name.txt")
    dco = pd.read_csv(fileSelect)
    print(dco)
    columns_name = dco.columns
    col_dat = list(df.columns)
    print(df.columns)
    print(df.head())

    df.drop(columns=df.columns[0],inplace=True)
    print(df.head())
    col =df.columns
    print(len(col),":",len(dco.columns))

    lastname = columns_name[4]
    locat = columns_name[8]
    # print(columns_name[lastname])
    print(lastname)
    # placeI = columns_name[8]
    df.insert(4, lastname, 4, allow_duplicates=False)
    text1 ="localidad"
    text2 = "codigo_postal"
    if text1 in col_dat:
        # print("localidad")
        try:
            # n=col_dat.index(text2)
            k=col_dat.index(text1)-1
            print(k)
            print(df.columns[k])
      

        except:
            return "error"
    elif text2 in col_dat:
     
        try:
            # n=col_dat.index(text2)
            k = col_dat.index(text2)+1
            print(df.columns[k])
            print(k)

        except:
            return "error"
        

        # print(df.columns["localidad"])
    locat = columns_name[k]
    df.insert(7, locat, 2, allow_duplicates=False)

    # print(dco)
    print("COLU")
    print(df.columns)
    print(len(df.columns), ":", len(dco.columns))
    # df.columns = dco.columns
    print(df.columns)
    print(df.head())
    # print(df["location"])
    # print(df["postal_code"])
    print(df.columns)
    print(df["age"][:5])

    # print(df.head())

if __name__ =="__main__":
    
    data = "./files/GBUNComahue_select.csv"
    # data = "./files/GBUNSalvador_select.csv"
    dataTransf(data)