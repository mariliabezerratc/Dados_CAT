import pandas as pd
import glob
import os



def calcula_idade(nascimento, emissao_cat):
    idades = []
    for i in range(len(nascimento)):
        idade = emissao_cat[i] - nascimento[i]
        idade = round(float(idade)/(3.154*pow(10,16)))
        idades.append(idade)
    return idades

def unifica_dados(lista, colunas, colunas_utilizadas):
    allFiles = pd.DataFrame(columns=colunas_utilizadas)
    for i in range(len(lista)):
        file = pd.read_csv(lista[i], encoding='latin-1', sep=';')
        if (i<10):
            file.columns = colunas_utilizadas
        else:
            file['CBO.1'] = file['CBO']
            file['CBO'] = file['CBO'].map(lambda x: x[0:6])
            file['CID-10.1'] = file['CID-10']
            file['CID-10'] = file['CID-10'].map(lambda x: x[0:5])
            file = file[colunas]
            file.columns = colunas_utilizadas
        allFiles = pd.concat([allFiles, file])   
    return allFiles

def aplica_cnae(df, cnaes):
    df["CNAE2.0 Empregador"] = pd.to_numeric(df['CNAE2.0 Empregador'])
    df = df.loc[df["CNAE2.0 Empregador"].isin(cnaes),:]
    return df


colunas_utilizadas = ['Agente  Causador  Acidente', 'Data Acidente', 'CBO', 'CBO.1', 'CID-10',
       'CID-10.1', 'CNAE2.0 Empregador', 'CNAE2.0 Empregador.1',
       'Emitente CAT', 'Espécie do benefício', 'Filiação Segurado',
       'Indica acidente', 'Munic Empr', 'Natureza da Lesão',
       'Origem de Cadastramento CAT', 'Parte Corpo Atingida', 'Sexo',
       'Tipo do Acidente', 'UF  Munic.  Acidente', 'UF Munic. Empregador',
       'Data  Afastamento', 'Data Despacho Benefício', 'Data Acidente.1',
       'Data Nascimento', 'Data Emissão CAT']

colunas_arquivos_maisAtuais = ['Agente  Causador  Acidente', 'Data Acidente', 'CBO', 'CBO.1', 
                               'CID-10', 'CID-10.1',
       'CNAE2.0 Empregador', 'CNAE2.0 Empregador.1', 'Emitente CAT',
       'Espécie do benefício', 'Filiação Segurado', 'Indica Óbito Acidente',
       'Munic Empr', 'Natureza da Lesão', 'Origem de Cadastramento CAT',
       'Parte Corpo Atingida', 'Sexo', 'Tipo do Acidente',
       'UF  Munic.  Acidente', 'UF Munic. Empregador', 'Data Acidente.1',
       'Data Despacho Benefício', 'Data Acidente.2', 'Data Nascimento',
       'Data Emissão CAT']


colunas_selecionadas = ['Agente  Causador  Acidente', 'CBO', 'CBO.1', 'CID-10',
       'CID-10.1', 'CNAE2.0 Empregador', 'CNAE2.0 Empregador.1',
       'Emitente CAT', 'Espécie do benefício', 'Filiação Segurado',
       'Indica acidente', 'Munic Empr', 'Natureza da Lesão',
       'Origem de Cadastramento CAT', 'Parte Corpo Atingida', 'Sexo',
       'Tipo do Acidente', 'UF  Munic.  Acidente', 'UF Munic. Empregador', 'Data Despacho Benefício', 'Data Acidente.1', 'Idade']

   
cnaes = [41,412,4120,42,421,4211,4212,4213,422,4222,429,4299,43,431,
          4311,4312,4313,4319,432,4321,4322,4329,433,4330,439,4391,4399]


def filtros(df, colunas):
    df = df.loc[df['Data Nascimento'] != '00/00/0000', :]
    df = df.loc[df['Data Acidente.1'] != '00/00/0000', :]
    df['Data Emissão CAT'] = pd.to_datetime(df['Data Emissão CAT'], format='%d/%m/%Y')   
    df['Data Nascimento'] = pd.to_datetime(df['Data Nascimento'], format='%d/%m/%Y')  
    df['Data Acidente.1'] = pd.to_datetime(df['Data Acidente.1'], format='%d/%m/%Y')
    
    
    #df['Data Acidente.1'] = df['Data Acidente.1'].dt.dayofweek
    df.dropna(axis=0, how='any', inplace=True)
    df['Idade'] = calcula_idade(df['Data Nascimento'].values, df['Data Emissão CAT'].values)
    
    

    df = df[colunas]
    df = df.loc[((df['Idade'] >= 18) & (df['Idade'] <= 65)), :]
    
    return df

def tratar_campos(df):
    df.reset_index(drop=True, inplace=True)
    for coluna in df.columns:
        try:
            df[coluna] = df[coluna].apply(lambda x: x.strip())
        except: 
            continue
    
    df["Emitente CAT"] = df["Emitente CAT"].apply(corrige_cat)
    df["Espécie do benefício"] = df["Espécie do benefício"].apply(corrige_beneficio)

    df["Filiação Segurado"] =  df["Filiação Segurado"].apply(corrige_filiacao)

    df["Indica acidente"] =  df["Indica acidente"].apply(corrige_acidente)
    df["Sexo"] =  df["Sexo"].apply(corrige_sexo)
    df["Tipo do Acidente"] =  df["Tipo do Acidente"].apply(corrige_tipo)
     
    df['UF Munic. Empregador'] = df['UF Munic. Empregador'].apply(corrige_municipios)
    
    for i in range(df.shape[0]):
        try:
            df['Munic Empr'][i] = df['Munic Empr'][i][:2]
        except:
            df['Munic Empr'][i] = None
    df['UF  Munic.  Acidente'] = df['UF  Munic.  Acidente'].map(municipios)
    df['UF Munic. Empregador'] = df['UF Munic. Empregador'].map(municipios)
    
    return df


municipios = {'Rondônia':11,'Acre':12,'Amazonas':13,'Roraima':14,'Pará':15,'Amapá':16,'Tocantins':17,'Maranhão':21,'Piauí':22,'Ceará':23,'Rio Grande do Norte':24,'Paraíba':25,'Pernambuco':26,'Alagoas':27,'Sergipe':28,'Bahia':29,'Minas Gerais':31,'Espírito Santo':32,'Rio de Janeiro':33,'São Paulo':35,'Paraná':41,'Santa Catarina':42,'Rio Grande do Sul':43,'Mato Grosso do Sul':50,'Mato Grosso':51,'Goiás':52,'Distrito Federal':53}



def tratar_campos_naoDefinidos(df):
    import re
    for coluna in df.columns:
        
        causador =  df[coluna].unique()

        recorrencia = []
        for i in range(len(causador)):
            try:
                x = re.search("\{", causador[i])
                if(x!=None):
                    recorrencia.append(causador[i])
            except:
                continue
        df[coluna] = corrigeNulo(df[coluna].values, recorrencia)
    
    return df

def corrigeNulo(atributos, listaCorrecao):
    for i in range(len(atributos)):
        if(atributos[i] in listaCorrecao):
            atributos[i] = None
    return atributos


def corrige_cat(valor):
    if(valor == 'Autoridade PÃºblica'):
        return 'Autoridade Pública'
    elif(valor ==  'MÃ©dico'):
        return 'Médico'
    else:
        return valor

def corrige_beneficio(valor):
    if(valor == 'AuxÃ\xadlio Doenca por A'):
        return 'Auxílio Doenca por A'
    elif(valor ==  'PensÃ£o por Morte Aci'):
        return 'Pensão por Morte Aci'
    else:
        return valor

def corrige_filiacao(valor):
    if(valor == '{ñ class}'):
        return None
    else:
        return valor

def corrige_sexo(valor):
    if(valor == 'Não Informado'):
        return None
    elif(valor == 'NÃ£o Informado'):
        return None
    elif(valor == 'Indeterminado'):
        return None
    else:
        return valor

def corrige_acidente(valor):
    if(valor == 'NÃ£o'):
        return 'Não'
    else:
        return valor

def corrige_tipo(valor):
    if(valor == 'TÃ\xadpico'):
        return 'Típico'
    elif(valor == 'DoenÃ§a'):
        return 'Doença'
    else:
        return valor   

def corrige_municipios(valor):
    if(valor == 'SÃ£o Paulo'):
        return 'São Paulo'
    elif(valor == 'ParaÃ\xadba'):
        return 'Paraíba'
    elif(valor == 'ParÃ¡'):
        return 'Pará'
    elif(valor == 'ParanÃ¡'):
        return 'Paraná'
    elif(valor == 'EspÃ\xadrito Santo'):
        return 'Espírito Santo'
    elif(valor == 'MaranhÃ£o'):
        return 'Maranhão'
    elif(valor == 'CearÃ¡'):
        return 'Ceará'
    elif(valor == 'GoiÃ¡s'):
        return 'Goiás'
    elif(valor == 'RondÃ´nia'):
        return 'Rondônia'
    elif(valor == 'PiauÃ\xad'):
        return 'Piauí'
    elif(valor == 'AmapÃ¡'):
        return 'Amapá'
    else:
        return valor 

import numpy as np

os.chdir("C:\\Users\\lucia\\Tese_Marilia")


def executa_df():
    
    os.chdir("C:\\Users\\lucia\\Tese_Marilia")
    length = len('C:\\Users\\lucia\\Tese_Marilia' + '\\Data\\')
    path = 'C:\\Users\\lucia\\Tese_Marilia' + '\\Data\\*.csv'

    lista_arquivos = glob.glob(path)
    df = unifica_dados(lista_arquivos, colunas_arquivos_maisAtuais, colunas_utilizadas)


    df = aplica_cnae(df, cnaes)
    df = filtros(df, colunas_selecionadas)
    df = tratar_campos(df)
    df = tratar_campos_naoDefinidos(df)
    return df

