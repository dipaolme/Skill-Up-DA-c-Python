import pandas as pd

def dataTransf(data):
    print("DATATRANS")
    # df = pd.read_json(data)
    df = pd.read_csv(data)

    print(df.columns)

    # print(df.head())

