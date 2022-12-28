# Projeto em Python - Analise da Dados CAT

## Tratamento de Dados

- Os dados utilizados são provenientes de arquivos da **CAT** baixadas do site oficial
- Estes dados quando baixados para **CSV** apresentaram alguns problemas que foi necessário corrigir
	- Colunas com nomes diferentes em períodos diferentes
	- Erros de Enconding
	- Outros
- O primeiro passo foi criar um arquivos com nome `tratamento_dados.py`

### tratamento_dados.py`

- Possui várias funções que tem por objetivo carregar os dados, filtrar e corrigir os campos
- Cada função desse módulo tem uma utilidade específica, podendo ser chamada separadamente ou sendo usado um processo lógico definido no método `executa_df()`
- Seu processo consiste:
	- Primeiro acessa o caminho local, criando uma string com o path para os arquivos da CAT
	- Cria lista de nome de arquivos para todos os CSV.
	- Retorna o Dataframe com todos os dados de CSV unificados
	- Aplica filtro de linhas apenas com CNAES escolhidos
	- Aplica filtragem baseadas em:
		- Filtragem de datas inválidas, formatação para tipo data, retorno de dia de semana
		- Exclusão de valores nulos
		- Aplica função `calcula_idade()`
		- Filtragem por idade válida entre **18** e **65** anos
	- Tratamento de campos: corrigindo enconding, usando o `strip()` para apagar espaços vazios, entre outros
	- Tratamento de campos não definidos que são considerados nulos
- Temos um método similar ao `executa_df()`, porém um pouco mais simplificado o `executa_df_simplificado()`
	- Nele o processo é muito similar, tem por objetivo fazer um tratamento e uma primeira filtragem de dados, no entanto, aqui não vamos mudar o tipo de dado, não vamos transformar município em código, não vamos calcular a idade e também não será filtrado pela idade
	- O objetivo é corrigir os dados para poder executar o modelo diretamente

## Modelo Main

### Main-DadosCompletos.ipynb

- Usa o método `executa_df_simplificado()` do módulo `tratamento_dados.py` para criar um dataframe com os dados
- Desse novo dataframe faz-se uma filtragem para obter dados apenas de construção civil
- Exclui-se algumas colunas como `CNAE` que agora possuem valor único e também `CBO` e `Data Despacho Benefício`, o primeiro por causar erros no modelo (embora seja um object é tratado como inteiro pelo modelo) e o segundo pela quantia grande de dados nulos
- Aplica-se o modelo de Kmodes, calculando o custo para um intervalo entre 1 e 41 clusters, sendo feito o calculo a um passo de 2 em 2 clusters
- Depois é feito um procedimento parecido para executar o algoritmo de K Prototype, onde nesse são considerados também os dados com valores que não são categoricos