# Projeto em Python - Analise da Dados CAT

## Tratamento de Dados

- Os dados utilizados s�o provenientes de arquivos da **CAT** baixadas do site oficial
- Estes dados quando baixados para **CSV** apresentaram alguns problemas que foi necess�rio corrigir
	- Colunas com nomes diferentes em per�odos diferentes
	- Erros de Enconding
	- Outros
- O primeiro passo foi criar um arquivos com nome `tratamento_dados.py`

### tratamento_dados.py`

- Possui v�rias fun��es que tem por objetivo carregar os dados, filtrar e corrigir os campos
- Cada fun��o desse m�dulo tem uma utilidade espec�fica, podendo ser chamada separadamente ou sendo usado um processo l�gico definido no m�todo `executa_df()`
- Seu processo consiste:
	- Primeiro acessa o caminho local, criando uma string com o path para os arquivos da CAT
	- Cria lista de nome de arquivos para todos os CSV.
	- Retorna o Dataframe com todos os dados de CSV unificados
	- Aplica filtro de linhas apenas com CNAES escolhidos
	- Aplica filtragem baseadas em:
		- Filtragem de datas inv�lidas, formata��o para tipo data, retorno de dia de semana
		- Exclus�o de valores nulos
		- Aplica fun��o `calcula_idade()`
		- Filtragem por idade v�lida entre **18** e **65** anos
	- Tratamento de campos: corrigindo enconding, usando o `strip()` para apagar espa�os vazios, entre outros
	- Tratamento de campos n�o definidos que s�o considerados nulos