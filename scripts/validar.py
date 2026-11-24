#!/usr/bin/env python3
"""Compara as saidas (q1.txt..q5.txt, obtidas com hdfs dfs -getmerge) com resultados_esperados.txt.

Q1: valor unico. Q2/Q3/Q4: conjuntos de pares chave,valor (ordem livre). Q5: lista ordenada.
As contagens devem ser inteiras (ex.: "35.0" e reprovado).
"""
import argparse
import re
import sys
from pathlib import Path

INTEIRO = re.compile(r"^-?\d+$")


def ler_esperado(caminho):
    secoes, atual = {}, None
    for linha in Path(caminho).read_text(encoding="utf-8").splitlines():
        m = re.match(r"##\s*(Q\d)", linha)
        if m:
            atual = m.group(1).lower()
            secoes[atual] = []
        elif atual and linha.strip():
            secoes[atual].append(linha.strip())
    return secoes


def linhas_saida(arq):
    return [l.strip().replace("\t", ",") for l in arq.read_text(encoding="utf-8").splitlines() if l.strip()]


def pares(linhas, erros):
    out = []
    for l in linhas:
        chave, _, valor = l.rpartition(",")
        if not INTEIRO.match(valor.strip()):
            erros.append(f"valor nao inteiro: {l!r}")
            continue
        out.append((chave.strip(), int(valor)))
    return out


def comparar(q, esperado, obtido_linhas):
    erros = []
    if q == "q1":
        valor = obtido_linhas[0].rpartition(",")[2].strip() if obtido_linhas else ""
        if not INTEIRO.match(valor):
            return [f"valor nao inteiro/ausente: {valor!r}"]
        return [] if int(valor) == int(esperado[0]) else [f"esperado {esperado[0]}, obtido {valor}"]
    esp = pares(esperado, [])
    obt = pares(obtido_linhas, erros)
    if q == "q5":
        if obt != esp:
            erros.append(f"ordem/valores diferem\n      esperado: {esp}\n      obtido:   {obt}")
    else:
        de, do = dict(esp), dict(obt)
        if len(obt) != len(do):
            erros.append("chaves duplicadas na saida")
        for k in sorted(set(de) | set(do)):
            if de.get(k) != do.get(k):
                erros.append(f"{k}: esperado {de.get(k)}, obtido {do.get(k)}")
    return erros


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--esperado", required=True)
    ap.add_argument("--dir", required=True, help="diretorio com q1.txt ... q5.txt")
    ap.add_argument("--nome", default="")
    a = ap.parse_args()

    esperado = ler_esperado(a.esperado)
    falhou = False
    print(f"--- {a.nome or a.dir}")
    for q in ["q1", "q2", "q3", "q4", "q5"]:
        arq = Path(a.dir) / f"{q}.txt"
        if not arq.exists():
            print(f"  {q.upper()}: nao executada")
            continue
        erros = comparar(q, esperado[q], linhas_saida(arq))
        if erros:
            falhou = True
            print(f"  {q.upper()}: FALHA")
            for e in erros:
                print(f"    - {e}")
        else:
            print(f"  {q.upper()}: OK")
    sys.exit(1 if falhou else 0)


if __name__ == "__main__":
    main()
