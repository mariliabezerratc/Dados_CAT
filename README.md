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