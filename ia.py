import os
import json
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, List, Dict, Optional

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# =========================================================
# GRUPOS CANÔNICOS INSTITUCIONAIS
# =========================================================
# Aqui não estamos mais só padronizando nome.
# Estamos classificando instituições em um "grupo institucional".
# Isso deixa o cruzamento robusto.
# =========================================================

GRUPOS_FIXOS = {
    "GRUPO_ANHANGUERA": [
        "anhanguera",
        "pitagoras",
        "unopar",
        "universidade anhanguera",
        "universidade pitagoras unopar anhanguera",
        "centro universitario anhanguera pitagoras",
        "centro universitario anhanguera pitagoras ampli",
        "faculdade pitagoras",
    ],
    "UNEF": [
        "unef",
        "universidade de ensino superior de feira de santana",
    ],
    "UEFS": [
        "uefs",
        "universidade estadual de feira de santana",
    ],
    "UFRB": [
        "ufrb",
        "universidade federal do reconcavo da bahia",
    ],
    "UNEB": [
        "uneb",
        "universidade do estado da bahia",
    ],
    "UNIFAT": [
        "unifat",
        "fat",
        "faculdade anisio teixeira",
        "faculdade anisio teixeira fat",
        "faculdade anisio teixeira - fat",
        "colegio anisio teixeira",
        "colegio anisio teixeira cat",
    ],
    "UNIFAN": [
        "unifan",
        "fan",
        "faculdade nobre",
        "colegio nobre",
    ],
    "ESTACIO": [
        "estacio",
        "estacio de sa",
        "universidade estacio de sa",
    ],
    "UNEX": ["unex"],
    "UNIAENE": ["uniaene"],
    "UNICESUMAR": [
        "unicesumar",
        "universidade cesumar",
    ],
    "UNIFACS": [
        "unifacs",
        "universidade salvador",
        "unifacs universidade salvador",
    ],
    "UNIASSELVI": [
        "uniasselvi",
        "centro universitario leonardo da vinci",
    ],
    "UNINASSAU": ["uninassau"],
    "UNIT": [
        "unit",
        "universidade tiradentes",
        "universidade tiradentes unit",
    ],
    "UNIRB": ["unirb"],
    "UNIFACEMP": ["unifacemp"],
    "UNIJORGE": [
        "unijorge",
        "centro universitario jorge amado",
    ],
    "UNIMAM": ["unimam"],
    "FARESI": ["faresi"],
    "FAEL": ["fael"],
    "FSC": [
        "fsc",
        "faculdade de santa cruz da bahia",
        "faculdade de santa cruz da bahia fsc",
    ],
    "FARJ": [
        "farj",
        "faculdade regional de riachao do jacuipe",
        "faculdade regional de riachao de jacuipe",
    ],
    "GRAU_TECNICO": ["grau tecnico"],
    "POLICIA_MILITAR_BA": [
        "pm ba",
        "policia militar da bahia",
    ],
    "CBM_BA": ["cbm ba"],
    "CEEP": ["ceep"],
    "CEEP_AUREO_FILHO": ["ceep aureo filho"],
    "UFS": ["universidade federal de sergipe"],
    "UNIAGES": ["uniages", "uniages jacobina"],
}


_STOPWORDS = {"de", "da", "do", "dos", "das", "e", "em", "a", "o", "as", "os",
              "um", "uma", "para", "por", "com", "no", "na", "nos", "nas", "ao"}


def _tokens_sig(txt: str) -> set:
    """Tokens com mais de 2 chars e fora das stopwords."""
    return {t for t in txt.split() if len(t) > 2 and t not in _STOPWORDS}


def _score_alias(n: str, alias_n: str) -> float:
    """Retorna similaridade 0-1 entre texto normalizado n e um alias normalizado."""
    if not alias_n or not n:
        return 0.0
    # 1) Exato
    if n == alias_n:
        return 1.0
    # 2) Palavra de fronteira (evita "fat" dentro de "fatec")
    if len(alias_n) >= 4:
        if re.search(r'\b' + re.escape(alias_n) + r'\b', n):
            return 0.95
        if re.search(r'\b' + re.escape(n) + r'\b', alias_n):
            return 0.95
    # 3) Sobreposição de tokens significativos
    tok_n = _tokens_sig(n)
    tok_a = _tokens_sig(alias_n)
    if tok_a and len(tok_a) >= 2:
        overlap = len(tok_n & tok_a) / len(tok_a)
        if overlap >= 0.65:
            return 0.80 + (overlap - 0.65) * 0.5   # 0.80 → 0.975
    # 4) Similaridade sequencial (fallback)
    return SequenceMatcher(None, n, alias_n).ratio()


def normalizar_texto(txt: str) -> str:
    txt = str(txt or "").strip().lower()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[-_/|]+", " ", txt)
    txt = re.sub(r"[^\w\s,]", " ", txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()


def padronizar_curso(txt: str) -> str:
    return normalizar_texto(txt)


def extrair_itens_de_celula(txt: str) -> List[str]:
    bruto = str(txt or "").strip()
    if not bruto:
        return []

    partes = [p.strip() for p in re.split(r"\s*,\s*", bruto) if p.strip()]

    resultado = []
    for parte in partes:
        if " - " in parte:
            pedacos = [p.strip() for p in parte.split(" - ") if p.strip()]
            resultado.extend(pedacos)
        else:
            resultado.append(parte)

    vistos = set()
    finais = []
    for item in resultado:
        chave = normalizar_texto(item)
        if chave and chave not in vistos:
            vistos.add(chave)
            finais.append(item)

    return finais


def extrair_instituicoes_de_celula(txt: str) -> List[str]:
    return extrair_itens_de_celula(txt)


def extrair_cursos_de_celula(txt: str) -> List[str]:
    return extrair_itens_de_celula(txt)


def classificar_grupo_institucional(txt: str) -> str:
    n = normalizar_texto(txt)
    if not n:
        return ""

    melhor_grupo: Optional[str] = None
    melhor_score = 0.0

    for grupo, aliases in GRUPOS_FIXOS.items():
        for alias in aliases:
            alias_n = normalizar_texto(alias)
            score = _score_alias(n, alias_n)
            if score > melhor_score:
                melhor_score = score
                melhor_grupo = grupo

    # Threshold 0.75 para evitar falsos positivos
    if melhor_score >= 0.75:
        return melhor_grupo  # type: ignore[return-value]

    return n


def padronizar_instituicao(txt: str) -> str:
    """
    Mantém compatibilidade com o servidor atual.
    Agora retorna o GRUPO CANÔNICO quando houver.
    """
    return classificar_grupo_institucional(txt)


def padronizar_cursos(lista):
    try:
        lista_unica = sorted({str(x).strip() for x in lista if str(x).strip()})

        prompt = f"""
Padronize nomes de cursos removendo duplicidades e variações de escrita.

Responda apenas JSON válido:
{{
  "cursos": ["curso 1", "curso 2"]
}}

Lista:
{json.dumps(lista_unica, ensure_ascii=False)}
"""
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Responda apenas JSON válido."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        content = response.choices[0].message.content.strip()
        data = json.loads(content)
        cursos = data.get("cursos", lista_unica)

        if not isinstance(cursos, list):
            return lista_unica

        return cursos

    except Exception as e:
        print("Erro na IA ao padronizar cursos:", e)
        return lista


def resolver_equivalencias_instituicoes(
    instituicoes_geral: List[str],
    instituicoes_mec: List[str]
) -> Dict[str, str]:
    """
    Retorna:
    {
      "NOME_DA_BASE_MEC": "GRUPO_CANONICO"
    }
    """
    try:
        geral_expandida = []
        for item in instituicoes_geral:
            geral_expandida.extend(extrair_instituicoes_de_celula(item))

        geral_unicas = sorted({str(x).strip() for x in geral_expandida if str(x).strip()})
        mec_unicas = sorted({str(x).strip() for x in instituicoes_mec if str(x).strip()})

        equivalencias = {}

        # 1) primeiro classifica por heurística
        for mec in mec_unicas:
            grupo = classificar_grupo_institucional(mec)
            if grupo and grupo != normalizar_texto(mec):
                equivalencias[mec] = grupo

        # 2) IA só para o que sobrou sem grupo claro
        pendentes_mec = [m for m in mec_unicas if m not in equivalencias]

        if not pendentes_mec:
            return equivalencias

        prompt = f"""
Você é especialista em padronização de instituições de ensino do Brasil.

Objetivo:
Classificar nomes da BASE MEC em grupos institucionais canônicos.

Regras:
- Se um nome do MEC representar uma instituição/grupo já conhecido, devolva o grupo canônico
- Só relacione quando houver segurança real
- Não invente
- Responda SOMENTE JSON válido

Formato:
{{
  "equivalencias": {{
    "NOME_DA_BASE_MEC": "GRUPO_CANONICO"
  }}
}}

Instituições da base geral:
{json.dumps(geral_unicas, ensure_ascii=False)}

Instituições MEC pendentes:
{json.dumps(pendentes_mec, ensure_ascii=False)}

Grupos canônicos já conhecidos:
{json.dumps(GRUPOS_FIXOS, ensure_ascii=False)}
"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Responda apenas JSON válido."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        content = response.choices[0].message.content.strip()
        # Remove blocos markdown que o modelo às vezes insere
        content = re.sub(r"```(?:json)?\s*|\s*```", "", content).strip()
        data = json.loads(content)

        extras = data.get("equivalencias", {})
        if isinstance(extras, dict):
            equivalencias.update(extras)

        return equivalencias

    except Exception as e:
        print("Erro ao resolver equivalências de instituições:", e)
        return {}


# =========================================================
# CHAT COM IA
# =========================================================

def chat_com_ia(
    mensagem: str,
    historico: List[Dict] = None,
    contexto: Dict[str, Any] = None
) -> str:
    """
    Responde perguntas do assistente do dashboard.
    Suporta tanto perguntas sobre dados internos das planilhas quanto
    informações externas sobre instituições (endereço, telefone, perfil etc.).
    """
    sistema = (
        "Você é um assistente inteligente especializado em educação superior e "
        "prospecção comercial educacional no Brasil. Responda em português de forma "
        "clara, objetiva e útil.\n"
        "Você pode responder sobre:\n"
        "• Dados das planilhas do usuário (instituições, cursos, contratos, semestres, perdas, prospecções)\n"
        "• Informações gerais sobre instituições de ensino: endereços, telefones, histórico, perfil\n"
        "• O mercado educacional brasileiro e tendências do setor\n"
        "• Dúvidas sobre prospecção, negociação e contratos educacionais\n"
        "Se não tiver certeza sobre uma informação, diga claramente."
    )

    if contexto:
        resumo = contexto.get("resumo", "")
        registros = contexto.get("registros_relevantes", [])
        if resumo:
            sistema += f"\n\nDADOS DAS PLANILHAS DO USUÁRIO:\n{resumo}"
        if registros:
            sistema += "\n\nREGISTROS RELEVANTES PARA A PERGUNTA:\n"
            sistema += "\n".join(
                json.dumps(r, ensure_ascii=False, default=str)
                for r in registros[:20]
            )

    messages: List[Dict] = [{"role": "system", "content": sistema}]
    for msg in (historico or [])[-10:]:
        if isinstance(msg, dict) and msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": str(msg["content"])})
    messages.append({"role": "user", "content": mensagem})

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.4,
        max_tokens=1200,
    )
    return response.choices[0].message.content