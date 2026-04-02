import csv
import re
import unicodedata
import pandas as pd


# =========================
# CONFIG
# =========================
ARQUIVOS = {
    "HOSPITAIS": r"Hospital - 100%.CSV",
    "SADT": r"sadt_100.CSV",
    "HOSPITAIS_DIA": r"hospitais_dia_100.CSV",
}

ARQUIVO_SAIDA = "base_consolidada_fator_qualidade.csv"


# =========================
# HELPERS
# =========================
def normalizar_header(coluna: str) -> str:
    coluna = str(coluna).strip()
    coluna = unicodedata.normalize("NFKD", coluna).encode("ASCII", "ignore").decode("ASCII")
    coluna = coluna.upper()
    coluna = coluna.replace("_", " ")
    coluna = re.sub(r"\s+", " ", coluna).strip()
    return coluna


def normalizar_nulos(valor):
    if pd.isna(valor):
        return None

    texto = str(valor).strip()

    if texto == "":
        return None

    if texto.upper() in {"NULL", "NAN", "NONE", "NA", "N/A", "-"}:
        return None

    return texto


def limpar_espacos(valor):
    valor = normalizar_nulos(valor)
    if valor is None:
        return None

    valor = str(valor).strip()
    valor = re.sub(r"\s+", " ", valor)
    return valor if valor != "" else None


def remover_acentos(texto):
    if texto is None:
        return None
    return unicodedata.normalize("NFKD", str(texto)).encode("ASCII", "ignore").decode("ASCII")


def padronizar_texto_comparavel(valor):
    valor = limpar_espacos(valor)
    if valor is None:
        return None

    valor = remover_acentos(valor)
    valor = re.sub(r"\s+", " ", valor).strip()
    return valor.upper()


def somente_numeros(valor):
    """
    Remove qualquer coisa que não seja número.
    Exemplos:
    78478559000119        -> 78478559000119
    '08620828000154       -> 08620828000154
    08.620.828/0001-54    -> 08620828000154
    """
    if valor is None:
        return None

    valor = str(valor).strip()
    numeros = re.sub(r"[^0-9]", "", valor)
    return numeros if numeros != "" else None


def padronizar_cnpj(valor):
    """
    CNPJ:
    - remove qualquer caractere não numérico, inclusive '
    - completa com zero à esquerda até 14
    """
    valor = normalizar_nulos(valor)
    if valor is None:
        return None

    valor = somente_numeros(valor)
    if valor is None:
        return None

    valor = valor.zfill(14)
    return valor


def padronizar_cnes(valor):
    """
    CNES:
    - remove qualquer caractere não numérico, inclusive '
    - completa com zero à esquerda até 7 se menor
    - se maior que 7, mantém e marca observação
    """
    valor = normalizar_nulos(valor)
    if valor is None:
        return None, None

    valor = somente_numeros(valor)
    if valor is None:
        return None, None

    observacao = None

    if len(valor) < 7:
        valor = valor.zfill(7)
    elif len(valor) > 7:
        observacao = "-MAIOR QUE 7-"

    return valor, observacao


def padronizar_uf(valor):
    valor = padronizar_texto_comparavel(valor)
    if valor is None:
        return None

    valor = re.sub(r"[^A-Z]", "", valor)

    if len(valor) != 2:
        return None

    ufs_validas = {
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
        "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
        "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
    }

    return valor if valor in ufs_validas else None


def padronizar_data(valor):
    valor = normalizar_nulos(valor)
    if valor is None:
        return None

    dt = pd.to_datetime(valor, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        return None

    return dt.strftime("%Y-%m-%d")


def forcar_texto_excel(valor):
    """
    Adiciona apóstrofo no início para o Excel tratar como texto.
    Exemplo: 0018694 -> '0018694
    """
    if valor is None or pd.isna(valor) or str(valor).strip() == "":
        return ""
    return f'="{valor}"'


# =========================
# PROCESSAMENTO
# =========================
def carregar_e_transformar(caminho_arquivo: str, origem: str) -> pd.DataFrame:
    df = pd.read_csv(
        caminho_arquivo,
        sep=";",
        dtype=str,
        encoding="latin1"
    )

    # Padronizar headers
    df.columns = [normalizar_header(col) for col in df.columns]

    # Garantir coluna de data
    if "DATA DE ATUALIZACAO" not in df.columns:
        df["DATA DE ATUALIZACAO"] = None

    # Garantir colunas esperadas
    colunas_esperadas = [
        "NOME FANTASIA",
        "CNES",
        "CNPJ",
        "UF",
        "MUNICIPIO",
        "DATA DE ATUALIZACAO"
    ]

    for col in colunas_esperadas:
        if col not in df.columns:
            df[col] = None

    df = df[colunas_esperadas].copy()

    # Normalizar nulos
    for col in df.columns:
        df[col] = df[col].apply(normalizar_nulos)

    # Textos
    df["NOME FANTASIA"] = df["NOME FANTASIA"].apply(padronizar_texto_comparavel)
    df["MUNICIPIO"] = df["MUNICIPIO"].apply(padronizar_texto_comparavel)
    df["UF"] = df["UF"].apply(padronizar_uf)

    # CNPJ
    df["CNPJ"] = df["CNPJ"].apply(padronizar_cnpj)

    # CNES + observação
    cnes_tratado = df["CNES"].apply(padronizar_cnes)
    df["CNES"] = cnes_tratado.apply(lambda x: x[0])
    df["OBSERVACAO"] = cnes_tratado.apply(lambda x: x[1])

    # Data
    df["DATA DE ATUALIZACAO"] = df["DATA DE ATUALIZACAO"].apply(padronizar_data)

    # Origem
    df["ORIGEM"] = origem

    return df


# =========================
# MAIN
# =========================
def main():
    dfs = []

    for origem, arquivo in ARQUIVOS.items():
        df = carregar_e_transformar(arquivo, origem)
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    # Remover duplicados por CNES + CNPJ
    df_final = df_final.drop_duplicates(subset=["CNES", "CNPJ"], keep="first")

    # Forçar texto no Excel com apóstrofo
    df_final["CNES"] = df_final["CNES"].apply(forcar_texto_excel)
    df_final["CNPJ"] = df_final["CNPJ"].apply(forcar_texto_excel)

    # Garantir string nas demais colunas
    colunas_string = [
        "ORIGEM",
        "NOME FANTASIA",
        "CNES",
        "CNPJ",
        "UF",
        "MUNICIPIO",
        "DATA DE ATUALIZACAO",
        "OBSERVACAO"
    ]

    for col in colunas_string:
        df_final[col] = df_final[col].astype("string")

    # Ordenar colunas
    colunas_finais = [
        "ORIGEM",
        "NOME FANTASIA",
        "CNES",
        "CNPJ",
        "UF",
        "MUNICIPIO",
        "DATA DE ATUALIZACAO",
        "OBSERVACAO"
    ]
    df_final = df_final[colunas_finais]

    # Salvar CSV
    df_final.to_csv(
        ARQUIVO_SAIDA,
        sep=";",
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL
    )

    print("ETL concluído com sucesso.")
    print(f"Arquivo gerado: {ARQUIVO_SAIDA}")
    print(f"Total de registros finais: {len(df_final)}")

    qtd_maior_7 = (df_final["OBSERVACAO"] == "-MAIOR QUE 7-").sum()
    print(f"Registros com CNES maior que 7 dígitos: {qtd_maior_7}")


if __name__ == "__main__":
    main()