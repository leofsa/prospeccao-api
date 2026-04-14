from typing import List, Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from scraper import buscar_cursos_mec, buscar_cursos_mec_multicidades
from ia import (
    resolver_equivalencias_instituicoes,
    normalizar_texto,
    padronizar_curso,
    extrair_instituicoes_de_celula,
    extrair_cursos_de_celula,
    padronizar_instituicao,
    chat_com_ia,
)

app = FastAPI(title="API Prospecção MEC")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# MODELOS
# =========================================================

class ProspecaoPayload(BaseModel):
    geral_rows: List[Dict[str, Any]]
    municipio: str = "Feira de Santana"
    uf: str = "BA"
    somente_ativos: bool = True


class ProspecaoRegiaoPayload(BaseModel):
    geral_rows: List[Dict[str, Any]]
    cidades: List[str]
    uf: str = "BA"
    somente_ativos: bool = True


class ChatPayload(BaseModel):
    mensagem: str
    historico: List[Dict[str, Any]] = []
    contexto: Dict[str, Any] = {}


# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================

def criar_chave(instituicao: str, curso: str) -> str:
    return f"{normalizar_texto(instituicao)}||{padronizar_curso(curso)}"


def extrair_instituicao_geral(row: Dict[str, Any]) -> str:
    return (
        row.get("Instituição")
        or row.get("Instituicao")
        or row.get("instituicao")
        or ""
    )


def extrair_curso_geral(row: Dict[str, Any]) -> str:
    return (
        row.get("Curso")
        or row.get("curso")
        or ""
    )


def expandir_chaves_geral(geral_rows: List[Dict[str, Any]]) -> set:
    """
    Expande a planilha geral para combinações atômicas de:
    grupo_institucional + curso

    Exemplo:
    Instituição = "Anhanguera, BRAVO"
    Curso = "Administração, Odontologia"

    Gera:
    - GRUPO_ANHANGUERA || administracao
    - GRUPO_ANHANGUERA || odontologia
    - bravo || administracao
    - bravo || odontologia
    """
    chaves = set()

    for row in geral_rows:
        instituicao_raw = extrair_instituicao_geral(row)
        curso_raw = extrair_curso_geral(row)

        instituicoes = extrair_instituicoes_de_celula(instituicao_raw)
        cursos = extrair_cursos_de_celula(curso_raw)

        if not instituicoes or not cursos:
            continue

        for inst in instituicoes:
            grupo = padronizar_instituicao(inst)
            if not grupo:
                continue

            for curso in cursos:
                if not curso:
                    continue

                chaves.add(criar_chave(grupo, curso))

    return chaves


def comparar_mec_com_geral_inteligente(
    geral_rows: List[Dict[str, Any]],
    mec_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compara a base MEC com a planilha geral usando:
    - expansão de múltiplas instituições na célula
    - expansão de múltiplos cursos na célula
    - grupo canônico institucional
    - IA para equivalências mais difíceis
    """
    instituicoes_geral = []
    for row in geral_rows:
        bruto = extrair_instituicao_geral(row)
        instituicoes_geral.extend(extrair_instituicoes_de_celula(bruto))

    instituicoes_mec = []
    for row in mec_rows:
        inst = row.get("instituicao", "")
        if inst:
            instituicoes_mec.append(inst)

    equivalencias = resolver_equivalencias_instituicoes(
        instituicoes_geral=instituicoes_geral,
        instituicoes_mec=instituicoes_mec
    )

    chaves_geral = expandir_chaves_geral(geral_rows)

    novos = []
    cobertos = 0

    for row in mec_rows:
        inst_mec = row.get("instituicao", "")
        curso_mec = row.get("curso", "")

        if not inst_mec or not curso_mec:
            continue

        grupo = equivalencias.get(inst_mec, padronizar_instituicao(inst_mec))
        chave = criar_chave(grupo, curso_mec)

        if chave in chaves_geral:
            cobertos += 1
        else:
            novo = dict(row)
            novo["instituicao_original_mec"] = inst_mec
            novo["instituicao_grupo"] = grupo
            novos.append(novo)

    return {
        "base_mec": len(mec_rows),
        "ja_na_geral": cobertos,
        "novas_prospeccoes": len(novos),
        "data": novos,
        "equivalencias": equivalencias
    }


# =========================================================
# ROTAS BÁSICAS
# =========================================================

@app.get("/")
def home():
    return {"msg": "API OK"}


@app.post("/chat")
def chat(payload: ChatPayload):
    try:
        resposta = chat_com_ia(
            mensagem=payload.mensagem,
            historico=payload.historico,
            contexto=payload.contexto,
        )
        return {"resposta": resposta}
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prospeccao_mec")
def prospeccao_mec(
    municipio: str = Query(default="Feira de Santana", description="Município"),
    uf: str = Query(default="BA", description="UF"),
    somente_ativos: bool = Query(default=True, description="Trazer apenas cursos em atividade")
):
    dados = buscar_cursos_mec(
        municipio=municipio,
        uf=uf,
        somente_ativos=somente_ativos
    )
    return {
        "fonte": "MEC Dados Abertos (CSV local)",
        "municipio": municipio,
        "uf": uf,
        "somente_ativos": somente_ativos,
        "total": len(dados),
        "data": dados
    }


@app.get("/prospeccao_mec_regiao")
def prospeccao_mec_regiao(
    uf: str = Query(default="BA", description="UF"),
    cidades: str = Query(
        default="Feira de Santana,Santo Estevão,Conceição da Feira,São Gonçalo dos Campos,Santa Bárbara,Amélia Rodrigues,Irará"
    ),
    somente_ativos: bool = Query(default=True, description="Trazer apenas cursos em atividade")
):
    lista_cidades = [c.strip() for c in cidades.split(",") if c.strip()]

    dados = buscar_cursos_mec_multicidades(
        municipios=lista_cidades,
        uf=uf,
        somente_ativos=somente_ativos
    )

    return {
        "fonte": "MEC Dados Abertos (CSV local)",
        "uf": uf,
        "cidades": lista_cidades,
        "somente_ativos": somente_ativos,
        "total": len(dados),
        "data": dados
    }


# =========================================================
# ROTAS INTELIGENTES COM IA
# =========================================================

@app.post("/prospeccao_mec_inteligente")
def prospeccao_mec_inteligente(payload: ProspecaoPayload):
    mec_rows = buscar_cursos_mec(
        municipio=payload.municipio,
        uf=payload.uf,
        somente_ativos=payload.somente_ativos
    )

    resultado = comparar_mec_com_geral_inteligente(
        geral_rows=payload.geral_rows,
        mec_rows=mec_rows
    )

    return {
        "fonte": "MEC Dados Abertos (CSV local) + IA",
        "municipio": payload.municipio,
        "uf": payload.uf,
        "somente_ativos": payload.somente_ativos,
        **resultado
    }


@app.post("/prospeccao_mec_regiao_inteligente")
def prospeccao_mec_regiao_inteligente(payload: ProspecaoRegiaoPayload):
    mec_rows = buscar_cursos_mec_multicidades(
        municipios=payload.cidades,
        uf=payload.uf,
        somente_ativos=payload.somente_ativos
    )

    resultado = comparar_mec_com_geral_inteligente(
        geral_rows=payload.geral_rows,
        mec_rows=mec_rows
    )

    return {
        "fonte": "MEC Dados Abertos (CSV local) + IA",
        "uf": payload.uf,
        "cidades": payload.cidades,
        "somente_ativos": payload.somente_ativos,
        **resultado
    }