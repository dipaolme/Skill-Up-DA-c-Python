
import os
import pandas as pd
import logging


def identExt(path,ext):
    contenido = os.listdir(path)
    # print("conte:")
    # logging.info(contenido)
    # print(contenido)
    data = []
    for fichero in contenido:
        if os.path.isfile(os.path.join(path, fichero)) and fichero.endswith(ext):
            data.append(fichero)
    return data



def createPath(target):
    path = os.getcwd()
    contenido = os.listdir(path)
    # print(contenido)
    indexI = contenido.index(target)
    endpoint = contenido[indexI]
    path = path+'/'+endpoint
    return path

def csvFile(path,select):
    # logging.info('Select_csv')
    csv = identExt(path,'.csv')

    n = csv.index(select)
    
    selectCsv = csv[n]
    return path+'/'+selectCsv 


def txtFile(path, select):
    # logging.info('Select_txt')
    txt = identExt(path, '.txt')

    n = txt.index(select)

    selectTxt = txt[n]
    return path+'/'+selectTxt






def sqlCommand(file,point):

    # logging.info('create_command_sql')
    # print("OS-path:")
    # print(os.getcwd())
    path = createPath(point)
    # print("path____")
    # print(path)
    sql = identExt(path,'.sql')
    # print(sql)
    # print(path)
    # print(sql[0])
    n = sql.index(file)
    fd = open(path+'/'+sql[n], 'r')
    sqlFile = fd.read()
    fd.close()
    # # print(sqlFile)
    # sqlcomands = sqlFile.split(';')
    # sqlcomands.pop()
    sqlCommands =sqlFile
    # print(sqlcomands)
    # print(len(sqlcomands))

    # print(sqlcomands)
    return sqlCommands


if __name__ == '__main__':

    var = sqlCommand(path='Skill-Up-DA-c-PythonG1\include',
                     file='GBUNSalvador.sql')  

    print(var)

    # print(contenido)
