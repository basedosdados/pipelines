
//----------------------------------------------------------------------------//
// prefacio
//----------------------------------------------------------------------------//

clear all
set excelxlsxlargefile on

cd "~/Monash Uni Enterprise Dropbox/Ricardo Dahis/Academic/Data/Brazil/SEEG"

//----------------------------------------------------------------------------//
// build: uf
//----------------------------------------------------------------------------//

! mkdir "output/uf"

//-----------------------//
// constroi dicionario
//-----------------------//

import excel "input/Dados-nacionais-13.0.xlsx", clear sheet("Dados") locale("utf-8") allstring //cellrange(A1:BM100)

drop in 1

foreach n in A B C D E F G H I K {
	preserve
		keep `n'
		duplicates drop
		tempfile dict_`n'
		save `dict_`n''
	restore
}

local lA tipo
local lB gas
local lC setor
local lD categoria
local lE subcategoria
local lF produto
local lG detalhamento
local lH recorte
local lI atividade_economica
local lK bioma

foreach n in A B C D E F G H I K {
	use `dict_`n'', clear
	sort `n'
	cap label drop `n'
	encode `n', gen(aux) label(`n')
	ren aux `l`n''
	
	save "tmp/dict_`n'.dta", replace
	
}

foreach n in A B C D E F G H I K {
	
	use "tmp/dict_`n'.dta", clear
	
	ren `n' valor
	ren `l`n'' chave
	gen coluna = "`l`n''"
	
	tempfile dict`n'
	save `dict`n''
	
}

use `dictA', clear
foreach n in B C D E F G H I K {
	append using `dict`n''
}

gen tabela = "uf"
gen cobertura_temporal = ""

order tabela coluna chave cobertura_temporal valor
sort  tabela coluna chave cobertura_temporal

save "output/dicionario_uf.dta", replace

//-----------------------//
// dados
//-----------------------//

import excel "input/Dados-nacionais-13.0.xlsx", clear sheet("Dados") locale("utf-8") allstring //cellrange(A1:BM100)

drop in 1
drop if J == "Não Alocado"

foreach n in A B C D E F G H I K {
	merge m:1 `n' using "tmp/dict_`n'.dta", keep(1 3) nogenerate
	drop `n'
}

ren J nome_uf
ren L  emissao1970
ren M  emissao1971
ren N  emissao1972
ren O  emissao1973
ren P  emissao1974
ren Q  emissao1975
ren R  emissao1976
ren S  emissao1977
ren T  emissao1978
ren U  emissao1979
ren V  emissao1980
ren W  emissao1981
ren X  emissao1982
ren Y  emissao1983
ren Z  emissao1984
ren AA emissao1985
ren AB emissao1986
ren AC emissao1987
ren AD emissao1988
ren AE emissao1989
ren AF emissao1990
ren AG emissao1991
ren AH emissao1992
ren AI emissao1993
ren AJ emissao1994
ren AK emissao1995
ren AL emissao1996
ren AM emissao1997
ren AN emissao1998
ren AO emissao1999
ren AP emissao2000
ren AQ emissao2001
ren AR emissao2002
ren AS emissao2003
ren AT emissao2004
ren AU emissao2005
ren AV emissao2006
ren AW emissao2007
ren AX emissao2008
ren AY emissao2009
ren AZ emissao2010
ren BA emissao2011
ren BB emissao2012
ren BC emissao2013
ren BD emissao2014
ren BE emissao2015
ren BF emissao2016
ren BG emissao2017
ren BH emissao2018
ren BI emissao2019
ren BJ emissao2020
ren BK emissao2021
ren BL emissao2022
ren BM emissao2023
ren BN emissao2024

gen     sigla_uf = ""
replace sigla_uf = "AC" if nome_uf == "Acre"
replace sigla_uf = "AL" if nome_uf == "Alagoas"
replace sigla_uf = "AM" if nome_uf == "Amazonas"
replace sigla_uf = "AP" if nome_uf == "Amapá"
replace sigla_uf = "BA" if nome_uf == "Bahia"
replace sigla_uf = "CE" if nome_uf == "Ceará"
replace sigla_uf = "DF" if nome_uf == "Distrito Federal"
replace sigla_uf = "ES" if nome_uf == "Espírito Santo"
replace sigla_uf = "GO" if nome_uf == "Goiás"
replace sigla_uf = "MA" if nome_uf == "Maranhão"
replace sigla_uf = "MG" if nome_uf == "Minas Gerais"
replace sigla_uf = "MS" if nome_uf == "Mato Grosso do Sul"
replace sigla_uf = "MT" if nome_uf == "Mato Grosso"
replace sigla_uf = "PA" if nome_uf == "Pará"
replace sigla_uf = "PB" if nome_uf == "Paraíba"
replace sigla_uf = "PE" if nome_uf == "Pernambuco"
replace sigla_uf = "PI" if nome_uf == "Piauí"
replace sigla_uf = "PR" if nome_uf == "Paraná"
replace sigla_uf = "RJ" if nome_uf == "Rio de Janeiro"
replace sigla_uf = "RN" if nome_uf == "Rio Grande do Norte"
replace sigla_uf = "RO" if nome_uf == "Rondônia"
replace sigla_uf = "RR" if nome_uf == "Roraima"
replace sigla_uf = "RS" if nome_uf == "Rio Grande do Sul"
replace sigla_uf = "SC" if nome_uf == "Santa Catarina"
replace sigla_uf = "SE" if nome_uf == "Sergipe"
replace sigla_uf = "SP" if nome_uf == "São Paulo"
replace sigla_uf = "TO" if nome_uf == "Tocantins"

drop nome_uf

foreach n of numlist 1(1)5 {
	
	preserve
		
		keep if setor == `n'
		drop setor
		
		reshape long emissao, i(sigla_uf bioma categoria subcategoria produto detalhamento recorte atividade_economica tipo gas) j(ano)
		
		gen setor = `n'
		
		tempfile setor_`n'
		save `setor_`n''
	
	restore
	
}

use          `setor_1', clear
append using `setor_2'
append using `setor_3'
append using `setor_4'
append using `setor_5'

destring emissao, replace

order ano sigla_uf bioma gas tipo recorte setor atividade_economica categoria subcategoria produto detalhamento

save "tmp/uf.dta", replace

// particiona

foreach uf in AC AL AM AP BA CE DF ES GO MA MG MS MT NA PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
	
	! mkdir "output/uf/sigla_uf=`uf'"
	
	foreach ano of numlist 1970(1)2024 {
	
		! mkdir "output/uf/sigla_uf=`uf'/ano=`ano'"
		
		use "tmp/uf.dta" if sigla_uf == "`uf'" & ano == `ano', clear
		drop sigla_uf ano
		export delimited "output/uf/sigla_uf=`uf'/ano=`ano'/uf.csv", replace nolabel datafmt
	
	}
	
	erase "tmp/uf.dta"
	
}

//----------------------------------------------------------------------------//
// build: municipio
//----------------------------------------------------------------------------//

//-----------------------//
// pega id_municipio
// de outra tabela
//-----------------------//

import excel "input/Dados-municipais-resumido-CO2e-GWP-AR5-13.0.xlsx", clear sheet("Dados") locale("utf-8") allstring

drop in 1

keep H I
ren H municipio
ren I id_municipio

duplicates drop

gen sigla_uf = substr(municipio, -3, 2)

replace id_municipio = substr(id_municipio, 2, 7) // por algum motivo eles colocaram um `1` na frente do ID IBGE 7 digitos

drop if substr(municipio, 1, 2) == "NA" // ATENCAO: tirando observacoes sem emissoes atribuidas a municipios

save "tmp/municipio.dta", replace

//-----------------------//
// constroi dicionario
//-----------------------//

foreach uf in AC AL AM AP BA CE DF ES GO MA MG MS MT NA PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
	
	import delimited "input/Dados por Municipio 13.0/`uf'/ar6.csv", clear encoding("utf-8") stringcols(_all) //rowr(1:10000)
	
	drop in 1
	
	replace v10 = "CO2e (t) GTP" if v10 == "CO2e (t) GTP-AR6"
	replace v10 = "CO2e (t) GWP" if v10 == "CO2e (t) GWP-AR6"
	
	foreach n of numlist 1(1)10 {	
		preserve
			keep v`n'
			duplicates drop
			tempfile dict_v`n'_`uf'
			save `dict_v`n'_`uf''
		restore
		
	}
	
}

local l1  setor
local l2  categoria
local l3  subcategoria
local l4  produto
local l5  detalhamento
local l6  recorte
local l7  atividade_economica
local l8  bioma
local l9  tipo
local l10 gas

foreach n of numlist 1(1)10 {
	use `dict_v`n'_AC', clear
	foreach uf in AL AM AP BA CE DF ES GO MA MG MS MT NA PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
		append using `dict_v`n'_`uf''
	}
	duplicates drop
	sort v`n'
	cap label drop v`n'
	encode v`n', gen(aux) label(v`n')
	
	ren aux `l`n''
	
	save "tmp/dict_v`n'.dta", replace
	
}

foreach n of numlist 1(1)10 {
	
	use "tmp/dict_v`n'.dta", clear
	
	ren v`n' valor
	ren `l`n'' chave
	gen coluna = "`l`n''"
	
	tempfile dict`n'
	save `dict`n''
	
}

use `dict1', clear
foreach n of numlist 2(1)10 {
	append using `dict`n''
}

gen tabela = "municipio"
gen cobertura_temporal = ""

order tabela coluna chave cobertura_temporal valor
sort  tabela coluna chave cobertura_temporal

save "output/dicionario_municipio.dta", replace

//-----------------------//
// dados
//-----------------------//

! mkdir "output/municipio"

foreach uf in AC AL AM AP BA CE DF ES GO MA MG MS MT NA PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
	
	! mkdir "output/municipio/sigla_uf=`uf'"
	
	foreach ar in 2 4 5 6 {
		
		import delimited "input/Dados por Municipio 13.0/`uf'/ar`ar'.csv", clear encoding("utf-8") stringcols(_all) //rowr(1:1000)
		
		drop in 1
		drop if v11 == "NA (NA)" // deleta linhas com valores não-atribuídos a municípios
		
		//replace v9 = "" if v9 == "NA"
		
		replace v10 = "CO2e (t) GTP" if v10 == "CO2e (t) GTP-AR`ar'"
		replace v10 = "CO2e (t) GWP" if v10 == "CO2e (t) GWP-AR`ar'"
		
		foreach n of numlist 1(1)10 {	
			merge m:1 v`n' using "tmp/dict_v`n'.dta", keep(1 3) nogenerate
			drop v`n'
		}
		
		ren v11 municipio
		
		foreach k of numlist 12(1)66 {
			local ano = 1958 + `k'
			ren v`k' emissao_ar`ar'`ano'
		}
		
		preserve
			
			keep if gas == 1 // "CO2e (t) GTP"
			drop gas
			
			reshape long emissao_ar`ar', i(municipio bioma tipo setor atividade_economica categoria subcategoria produto detalhamento recorte) j(ano)
			
			gen gas = 1
			
			tempfile GTP
			save `GTP'
		
		restore
		preserve
			
			keep if gas == 2 // "CO2e (t) GWP"
			drop gas
			
			reshape long emissao_ar`ar', i(municipio bioma tipo setor atividade_economica categoria subcategoria produto detalhamento recorte) j(ano)
			
			gen gas = 2
			
			tempfile GWP
			save `GWP'
		
		restore
		
		use `GTP', clear
		append using `GWP'
		
		destring emissao_ar`ar', replace
		
		merge m:1 municipio using "tmp/municipio.dta", keep(3) nogenerate
		drop municipio
		
		order ano sigla_uf id_municipio bioma gas tipo recorte setor atividade_economica categoria subcategoria produto detalhamento
		
		save "tmp/municipio_`uf'_ar`ar'.dta", replace
		
	}
	
	use "tmp/municipio_`uf'_ar2.dta", clear
	merge 1:1 ano sigla_uf id_municipio bioma gas tipo recorte setor atividade_economica categoria subcategoria produto detalhamento using "tmp/municipio_`uf'_ar4.dta", nogenerate
	merge 1:1 ano sigla_uf id_municipio bioma gas tipo recorte setor atividade_economica categoria subcategoria produto detalhamento using "tmp/municipio_`uf'_ar5.dta", nogenerate
	merge 1:1 ano sigla_uf id_municipio bioma gas tipo recorte setor atividade_economica categoria subcategoria produto detalhamento using "tmp/municipio_`uf'_ar6.dta", nogenerate
	save "tmp/municipio_`uf'.dta", replace
	
	erase "tmp/municipio_`uf'_ar2.dta"
	erase "tmp/municipio_`uf'_ar4.dta"
	erase "tmp/municipio_`uf'_ar5.dta"
	erase "tmp/municipio_`uf'_ar6.dta"
	
	// particiona
	
	foreach ano of numlist 1970(1)2024 {
		
		! mkdir "output/municipio/sigla_uf=`uf'/ano=`ano'"
		
		use "tmp/municipio_`uf'.dta" if ano == `ano', clear
		drop sigla_uf ano
		export delimited "output/municipio/sigla_uf=`uf'/ano=`ano'/municipio.csv", replace nolabel datafmt
		
	}
	
	erase "tmp/municipio_`uf'.dta"
	
}

//----------------------------------------------------------------------------//
// build: dicionario
//----------------------------------------------------------------------------//

use          "output/dicionario_uf.dta", clear
append using "output/dicionario_municipio.dta"

save             "output/dicionario.dta", replace
export delimited "output/dicionario.csv", replace nolabel


