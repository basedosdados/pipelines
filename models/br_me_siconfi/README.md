ATUALIZANDO O SICONFI

1. Baixar nossa pasta de input do SICONFI do Drive da BD em `Dados/Conjuntos/br_me_siconfi/input`.
2. Baixar arquivos originais do Tesouro.
	- O Tesouro atualiza retroativo os dados por um tempo depois. Por isso temos que atualizar os dados originais de todos os anos (2013-presente) sempre. É chato porque baixar cada arquivo exige vários cliques e um captcha.
	- Seguir a estrutura da pasta `/input`. Baixar cada arquivo, colocar na pasta certa, e renomear cada arquivo pelo seu ano (ex: `2013.zip`, `2018.zip`).
3. Atualizar as tabelas de compatibilização.
	- Não basta duplicar as linhas do último ano e só mudar a coluna `ano` de `t` para `t+1`. O Tesouro pode mudar a árvore de contas, acrescentando ou removendo contas. É preciso rodar um script nos dados originais de cada tabela para colher as contas únicas no formato da tabela de compatibilização.
		1. Extender na mão, copiando as linhas e subindo o ano em +1.
		2. Rodar merge, usar \_merge == 1 para o que falta, jogar fora o \_merge == 2, usar \_merge == 3 para o que está certo.
		- `keep ano estagio portaria conta`
		- `duplicates drop`
4. Rodar código no jupyter de atualização
5. Verificar testes de qualidade
	- O JOIN de contas compatibilizadas teve par para TODAS AS LINHAS. Se não teve, voltar à tabela de compatibilização e acrescentar/editar linhas.
6. Limpeza da casa
	- Subir a pasta `input` atualizada para o Drive.
