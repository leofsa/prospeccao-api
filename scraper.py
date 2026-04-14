import os
import re
from typing import List, Dict

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")
CSV_PATH = os.path.join(DATA_DIR, "PDA_Dados_Cursos_Graduacao_Brasil.csv")


def _fix_mojibake(texto):
    if pd.isna(texto):
        return ""

    texto = str(texto)

    try:
        return texto.encode("latin1").decode("utf-8")
    except Exception:
        return texto


def _normalizar(txt: str) -> str:
    txt = str(txt or "").strip().lower()
    txt = re.sub(r"\s+", " ", txt)
    return txt


COLUNAS_UTEIS = [
    "NOME_IES", "NOME_CURSO", "MUNICIPIO", "UF", "SITUACAO_CURSO",
    "GRAU", "MODALIDADE", "ORGANIZACAO_ACADEMICA", "CATEGORIA_ADMINISTRATIVA",
    "CODIGO_IES", "CODIGO_CURSO", "REGIAO",
]

_CSV_PARAMS: dict = {}  # cache dos parâmetros corretos (sep + encoding)


def _detectar_params() -> dict:
    """Detecta separador e encoding lendo apenas o cabeçalho do CSV."""
    if _CSV_PARAMS:
        return _CSV_PARAMS

    tentativas = [
        {"sep": ";", "encoding": "latin1"},
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ",", "encoding": "latin1"},
        {"sep": ";", "encoding": "cp1252"},
        {"sep": ",", "encoding": "cp1252"},
    ]
    for t in tentativas:
        try:
            df = pd.read_csv(CSV_PATH, sep=t["sep"], encoding=t["encoding"], nrows=2)
            if df.shape[1] > 5:
                _CSV_PARAMS.update(t)
                return t
        except Exception:
            pass
    raise RuntimeError("Não foi possível detectar o formato do CSV.")


def _ler_csv_filtrado(municipio_norm: str, uf_norm: str) -> pd.DataFrame:
    """Lê o CSV em chunks e retorna apenas as linhas do município/UF desejado."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Arquivo CSV não encontrado em: {CSV_PATH}")

    params = _detectar_params()
    chunks = []
    chunk_iter = pd.read_csv(
        CSV_PATH,
        sep=params["sep"],
        encoding=params["encoding"],
        dtype=str,
        chunksize=50_000,
        usecols=lambda c: str(c).strip().upper() in [col.upper() for col in COLUNAS_UTEIS],
    )
    for chunk in chunk_iter:
        chunk.columns = [str(c).strip().upper() for c in chunk.columns]
        if "MUNICIPIO" not in chunk.columns or "UF" not in chunk.columns:
            continue
        chunk["_M"] = chunk["MUNICIPIO"].fillna("").str.strip().str.lower()
        chunk["_U"] = chunk["UF"].fillna("").str.strip().str.lower()
        filtrado = chunk[(chunk["_M"] == municipio_norm) & (chunk["_U"] == uf_norm)]
        if not filtrado.empty:
            chunks.append(filtrado.drop(columns=["_M", "_U"]))

    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def _preparar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().upper() for c in df.columns]

    colunas_texto = [
        "NOME_IES",
        "CATEGORIA_ADMINISTRATIVA",
        "ORGANIZACAO_ACADEMICA",
        "NOME_CURSO",
        "GRAU",
        "AREA_OCDE",
        "MODALIDADE",
        "SITUACAO_CURSO",
        "AREA_OCDE_CINE",
        "MUNICIPIO",
        "UF",
        "REGIAO",
    ]

    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].apply(_fix_mojibake).astype(str).str.strip()

    return df


def buscar_cursos_mec(
    municipio: str = "Feira de Santana",
    uf: str = "BA",
    somente_ativos: bool = True
) -> List[Dict]:
    municipio_norm = _normalizar(municipio)
    uf_norm = _normalizar(uf)

    filtrado = _ler_csv_filtrado(municipio_norm, uf_norm)
    if filtrado.empty:
        return []
    filtrado = _preparar_dataframe(filtrado)

    if somente_ativos and "SITUACAO_CURSO" in filtrado.columns:
        filtrado["_SITUACAO_NORM"] = filtrado["SITUACAO_CURSO"].apply(_normalizar)
        filtrado = filtrado[filtrado["_SITUACAO_NORM"].str.contains("atividade", na=False)]

    subset_cols = ["NOME_IES", "NOME_CURSO", "MUNICIPIO", "UF"]
    filtrado = filtrado.drop_duplicates(subset=subset_cols)

    resultado = []

    for _, row in filtrado.iterrows():
        resultado.append({
            "codigo_ies": str(row["CODIGO_IES"]).strip() if "CODIGO_IES" in filtrado.columns else "",
            "instituicao": str(row["NOME_IES"]).strip(),
            "categoria_administrativa": str(row["CATEGORIA_ADMINISTRATIVA"]).strip() if "CATEGORIA_ADMINISTRATIVA" in filtrado.columns else "",
            "organizacao_academica": str(row["ORGANIZACAO_ACADEMICA"]).strip() if "ORGANIZACAO_ACADEMICA" in filtrado.columns else "",
            "codigo_curso": str(row["CODIGO_CURSO"]).strip() if "CODIGO_CURSO" in filtrado.columns else "",
            "curso": str(row["NOME_CURSO"]).strip(),
            "grau": str(row["GRAU"]).strip() if "GRAU" in filtrado.columns else "",
            "modalidade": str(row["MODALIDADE"]).strip() if "MODALIDADE" in filtrado.columns else "",
            "situacao_curso": str(row["SITUACAO_CURSO"]).strip() if "SITUACAO_CURSO" in filtrado.columns else "",
            "municipio": str(row["MUNICIPIO"]).strip(),
            "uf": str(row["UF"]).strip(),
            "regiao": str(row["REGIAO"]).strip() if "REGIAO" in filtrado.columns else "",
            "fonte": "MEC Dados Abertos (CSV local)"
        })

    resultado.sort(key=lambda x: (x["instituicao"], x["curso"]))
    return resultado


def buscar_cursos_mec_multicidades(
    municipios: List[str],
    uf: str = "BA",
    somente_ativos: bool = True
) -> List[Dict]:
    todos = []

    for municipio in municipios:
        dados = buscar_cursos_mec(
            municipio=municipio,
            uf=uf,
            somente_ativos=somente_ativos
        )
        todos.extend(dados)

    vistos = set()
    finais = []

    for item in todos:
        chave = (
            _normalizar(item["instituicao"]),
            _normalizar(item["curso"]),
            _normalizar(item["municipio"]),
            _normalizar(item["uf"])
        )
        if chave in vistos:
            continue
        vistos.add(chave)
        finais.append(item)

    finais.sort(key=lambda x: (x["municipio"], x["instituicao"], x["curso"]))
    return finais