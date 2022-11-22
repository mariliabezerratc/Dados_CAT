import tratamento_dados as td
import pandas as pd
import warnings

def exec():
    
    df = td.executa_df()

    import os

    cbo = pd.read_csv('ArquivosTratados\\cbo.csv', encoding='latin-1', sep=',')
    cbo = cbo.loc[:, ['CBO', 'Unnamed: 5']]
    cbo.dropna(axis=0, how='any', inplace=True)
    cbo_dict = dict([(i, a) for i, a in zip(cbo['CBO'], cbo['Unnamed: 5'])])


    # CID
    cid = pd.read_csv('ArquivosTratados\\cid.csv', encoding='latin-1', sep=',')
    cid = cid.loc[:, ['CID-10', 'Unnamed: 5']]
    cid.dropna(axis=0, how='any', inplace=True)
    cid_dict = dict([(i, a) for i, a in zip(cid['CID-10'], cid['Unnamed: 5'])])


    # Natureza
    natureza = pd.read_csv('ArquivosTratados\\natureza.csv', encoding='latin-1', sep=',')
    natureza = natureza.loc[:, ['Natureza', 'Agente']]
    natureza.dropna(axis=0, how='any', inplace=True)
    natureza_dict = dict([(i, a) for i, a in zip(natureza['Natureza'], natureza['Agente'])])

    # Transformando o DF
    df = df.loc[:, ['Agente  Causador  Acidente', 'CBO', 'CID-10', 'CNAE2.0 Empregador', 
                    'CNAE2.0 Empregador.1', 'Emitente CAT', 'Espécie do benefício', 
                    'Filiação Segurado', 'Indica acidente', 'Munic Empr', 'Natureza da Lesão', 
                    'Origem de Cadastramento CAT', 'Parte Corpo Atingida', 'Sexo', 'Tipo do Acidente',
                    'UF  Munic.  Acidente', 'UF Munic. Empregador','Data Despacho Benefício', 'Data Acidente.1', 'Idade']]
    
    df['CBO'] = df['CBO'].map(cbo_dict)
    df['CID-10'] = df['CID-10'].map(cid_dict)
    df['Agente  Causador  Acidente'] = df['Agente  Causador  Acidente'].map(natureza_dict)
    
    def categoriza_idade(idade):
        if(idade <= 30):
            return '18-30'
        elif(idade <= 40):
            return '31-40'
        elif(idade <= 50):
            return '41-50'
        else:
            return '51-65'
    df['Idade'] = df['Idade'].apply(categoriza_idade)
    
    # Muitos Nan em UF  Munic.  Acidente. 
    df.drop('UF  Munic.  Acidente', axis=1, inplace=True)
    # Valores inconsistentes
    df.drop('Data Despacho Benefício', axis=1, inplace=True)
    
    # Excluindo qualquer linah que contenha valor nulo
    df.dropna(axis=0, how='any', inplace=True)
    
    return df
