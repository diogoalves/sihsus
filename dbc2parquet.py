import os
import sys
from dbfread import DBF
from pandas import DataFrame
from pathlib import Path
import uuid

schema_columns = ['UF_ZI', 'ANO_CMPT', 'MES_CMPT', 'ESPEC', 'CGC_HOSP', 'N_AIH', 'IDENT', 'CEP', 'MUNIC_RES', 'NASC', 'SEXO', 'UTI_MES_IN', 'UTI_MES_AN', 'UTI_MES_AL', 'UTI_MES_TO', 'MARCA_UTI', 'UTI_INT_IN', 'UTI_INT_AN', 'UTI_INT_AL', 'UTI_INT_TO', 'DIAR_ACOM', 'QT_DIARIAS', 'PROC_SOLIC', 'PROC_REA', 'VAL_SH', 'VAL_SP', 'VAL_SADT', 'VAL_RN', 'VAL_ACOMP', 'VAL_ORTP', 'VAL_SANGUE', 'VAL_SADTSR', 'VAL_TRANSP', 'VAL_OBSANG', 'VAL_PED1AC', 'VAL_TOT', 'VAL_UTI', 'US_TOT', 'DT_INTER', 'DT_SAIDA', 'DIAG_PRINC', 'DIAG_SECUN', 'COBRANCA', 'NATUREZA', 'NAT_JUR', 'GESTAO', 'RUBRICA', 'IND_VDRL', 'MUNIC_MOV', 'COD_IDADE', 'IDADE', 'DIAS_PERM', 'MORTE', 'NACIONAL', 'NUM_PROC', 'CAR_INT', 'TOT_PT_SP', 'CPF_AUT', 'HOMONIMO', 'NUM_FILHOS', 'INSTRU', 'CID_NOTIF', 'CONTRACEP1', 'CONTRACEP2', 'GESTRISCO', 'INSC_PN', 'SEQ_AIH5', 'CBOR', 'CNAER', 'VINCPREV', 'GESTOR_COD', 'GESTOR_TP', 'GESTOR_CPF', 'GESTOR_DT', 'CNES', 'CNPJ_MANT', 'INFEHOSP', 'CID_ASSO', 'CID_MORTE', 'COMPLEX', 'FINANC', 'FAEC_TP', 'REGCT', 'RACA_COR', 'ETNIA', 'SEQUENCIA', 'REMESSA', 'AUD_JUST', 'SIS_JUST', 'VAL_SH_FED', 'VAL_SP_FED', 'VAL_SH_GES', 'VAL_SP_GES', 'VAL_UCI', 'MARCA_UCI', 'DIAGSEC1', 'DIAGSEC2', 'DIAGSEC3', 'DIAGSEC4', 'DIAGSEC5', 'DIAGSEC6', 'DIAGSEC7', 'DIAGSEC8', 'DIAGSEC9', 'TPDISEC1', 'TPDISEC2', 'TPDISEC3', 'TPDISEC4', 'TPDISEC5', 'TPDISEC6', 'TPDISEC7', 'TPDISEC8', 'TPDISEC9']

schema_dtypes = {
    "UF_ZI": "object",
    "ANO_CMPT": "object",
    "MES_CMPT": "object",
    "ESPEC": "object",
    "CGC_HOSP": "object",
    "N_AIH": "object",
    "IDENT": "object",
    "CEP": "object",
    "MUNIC_RES": "object",
    "NASC": "object",
    "SEXO": "object",
    "UTI_MES_IN": "int64",
    "UTI_MES_AN": "int64",
    "UTI_MES_AL": "int64",
    "UTI_MES_TO": "int64",
    "MARCA_UTI": "object",
    "UTI_INT_IN": "int64",
    "UTI_INT_AN": "int64",
    "UTI_INT_AL": "int64",
    "UTI_INT_TO": "int64",
    "DIAR_ACOM": "int64",
    "QT_DIARIAS": "int64",
    "PROC_SOLIC": "object",
    "PROC_REA": "object",
    "VAL_SH": "float64",
    "VAL_SP": "float64",
    "VAL_SADT": "float64",
    "VAL_RN": "float64",
    "VAL_ACOMP": "float64",
    "VAL_ORTP": "float64",
    "VAL_SANGUE": "float64",
    "VAL_SADTSR": "float64",
    "VAL_TRANSP": "float64",
    "VAL_OBSANG": "float64",
    "VAL_PED1AC": "float64",
    "VAL_TOT": "float64",
    "VAL_UTI": "float64",
    "US_TOT": "float64",
    "DT_INTER": "object",
    "DT_SAIDA": "object",
    "DIAG_PRINC": "object",
    "DIAG_SECUN": "object",
    "COBRANCA": "object",
    "NATUREZA": "object",
    "NAT_JUR": "object",
    "GESTAO": "object",
    "RUBRICA": "int64",
    "IND_VDRL": "object",
    "MUNIC_MOV": "object",
    "COD_IDADE": "object",
    "IDADE": "int64",
    "DIAS_PERM": "int64",
    "MORTE": "int64",
    "NACIONAL": "object",
    "NUM_PROC": "object",
    "CAR_INT": "object",
    "TOT_PT_SP": "int64",
    "CPF_AUT": "object",
    "HOMONIMO": "object",
    "NUM_FILHOS": "int64",
    "INSTRU": "object",
    "CID_NOTIF": "object",
    "CONTRACEP1": "object",
    "CONTRACEP2": "object",
    "GESTRISCO": "object",
    "INSC_PN": "object",
    "SEQ_AIH5": "object",
    "CBOR": "object",
    "CNAER": "object",
    "VINCPREV": "object",
    "GESTOR_COD": "object",
    "GESTOR_TP": "object",
    "GESTOR_CPF": "object",
    "GESTOR_DT": "object",
    "CNES": "object",
    "CNPJ_MANT": "object",
    "INFEHOSP": "object",
    "CID_ASSO": "object",
    "CID_MORTE": "object",
    "COMPLEX": "object",
    "FINANC": "object",
    "FAEC_TP": "object",
    "REGCT": "object",
    "RACA_COR": "object",
    "ETNIA": "object",
    "SEQUENCIA": "int64",
    "REMESSA": "object",
    "AUD_JUST": "object",
    "SIS_JUST": "object",
    "VAL_SH_FED": "float64",
    "VAL_SP_FED": "float64",
    "VAL_SH_GES": "float64",
    "VAL_SP_GES": "float64",
    "VAL_UCI": "float64",
    "MARCA_UCI": "object",
    "DIAGSEC1": "object",
    "DIAGSEC2": "object",
    "DIAGSEC3": "object",
    "DIAGSEC4": "object",
    "DIAGSEC5": "object",
    "DIAGSEC6": "object",
    "DIAGSEC7": "object",
    "DIAGSEC8": "object",
    "DIAGSEC9": "object",
    "TPDISEC1": "object",
    "TPDISEC2": "object",
    "TPDISEC3": "object",
    "TPDISEC4": "object",
    "TPDISEC5": "object",
    "TPDISEC6": "object",
    "TPDISEC7": "object",
    "TPDISEC8": "object",
    "TPDISEC9": "object"
}

try:
    file  = Path(sys.argv[1]).stem
    UF = file[2:4]
    YEAR = '20' + file[4:6]
    MONTH = file[6:8]
    tmp_filename = f'{str(uuid.uuid4())}.dbf'

    os.system(f'./blast-dbf/blast-dbf datasus.gov.br/{file}.dbc {tmp_filename}')

    dbf = DBF(tmp_filename)
    df = DataFrame(iter(dbf))
    
    df = df.reindex(columns=schema_columns)
    df.astype(schema_dtypes)
    columns_to_fill_empty_string = ['NAT_JUR', 'AUD_JUST', 'SIS_JUST', 'MARCA_UCI', 'DIAGSEC1', 'DIAGSEC2', 'DIAGSEC3', 'DIAGSEC4', 'DIAGSEC5', 'DIAGSEC6', 'DIAGSEC7', 'DIAGSEC8', 'DIAGSEC9', 'TPDISEC1', 'TPDISEC2', 'TPDISEC3', 'TPDISEC4', 'TPDISEC5', 'TPDISEC6', 'TPDISEC7', 'TPDISEC8', 'TPDISEC9']
    for c in columns_to_fill_empty_string:
        df[c].fillna('', inplace=True)


    df['YEAR'] = YEAR
    df['UF'] = UF
    df['MONTH'] = MONTH
    df.to_parquet(f'dataset', partition_cols=['YEAR', 'UF', 'MONTH'])
    os.system(f'ln -s -f ./YEAR={YEAR}/UF={UF}/MONTH={MONTH}/part.0.parquet  dataset/{file}.pq')

finally:
    os.remove(tmp_filename)

exit(0)
