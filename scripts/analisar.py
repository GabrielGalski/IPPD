#!/usr/bin/env python3
"""Le resultados/tempos_q2.csv e gera:
  resultados/tabelas.md                 (tabelas no formato do enunciado + mediana do tempo interno + speedup)
  resultados/grafico_volume.png         (mediana x registros, por framework)
  resultados/grafico_paralelismo.png    (mediana x P, por framework e base)
  resultados/grafico_speedup.png        (T(1)/T(P))
Graficos exigem matplotlib (pode rodar no seu notebook: copie o CSV do master).
"""
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else RAIZ / "resultados" / "tempos_q2.csv"
OUT = CSV.parent
NOMES = {10000: "10 mil", 1000000: "1 milhão", 10000000: "10 milhões"}


def num(x):
    try:
        return float(x)
    except ValueError:
        return None


def carregar():
    grupos = defaultdict(lambda: {"total": [], "interno": []})
    with CSV.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = num(r["tempo_total_s"])
            if t is None:
                continue  # execucao que falhou
            g = grupos[(r["experimento"], r["framework"], int(r["registros"]), int(r["paralelismo"]))]
            g["total"].append(t)
            i = num(r["tempo_interno_s"])
            if i is not None:
                g["interno"].append(i)
    return grupos


def med(v):
    return statistics.median(v) if v else float("nan")


def tabela(grupos, exp):
    chaves = sorted(k for k in grupos if k[0] == exp)
    if not chaves:
        return ""
    n = max(len(grupos[k]["total"]) for k in chaves)
    cab = ["Framework", "Registros", "P"] + [f"Execução {i+1} (s)" for i in range(n)] + ["Mediana (s)", "Mediana interna (s)"]
    linhas = ["| " + " | ".join(cab) + " |", "|" + "---|" * len(cab)]
    for k in chaves:
        g = grupos[k]
        execs = [f"{t:.2f}" for t in g["total"]] + ["—"] * (n - len(g["total"]))
        linhas.append("| " + " | ".join([k[1].capitalize(), NOMES.get(k[2], str(k[2])), str(k[3])] + execs
                                         + [f"{med(g['total']):.2f}", f"{med(g['interno']):.2f}"]) + " |")
    return "\n".join(linhas)


def tabela_speedup(grupos):
    linhas = ["| Framework | Registros | P | Mediana (s) | Speedup T(1)/T(P) | Eficiência |", "|---|---|---|---|---|---|"]
    for (exp, fw, reg, p) in sorted(k for k in grupos if k[0] == "paralelismo"):
        base = grupos.get(("paralelismo", fw, reg, 1))
        t = med(grupos[(exp, fw, reg, p)]["total"])
        if base:
            s = med(base["total"]) / t
            linhas.append(f"| {fw.capitalize()} | {NOMES.get(reg, reg)} | {p} | {t:.2f} | {s:.2f} | {s/p:.2f} |")
    return "\n".join(linhas)


def graficos(grupos):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib indisponivel: graficos nao gerados (tabelas.md foi gerado).")
        return
    fws = sorted({k[1] for k in grupos})

    vol = [k for k in grupos if k[0] == "volume"]
    if vol:
        plt.figure(figsize=(6, 4))
        for fw in fws:
            ks = sorted(k for k in vol if k[1] == fw)
            plt.plot([k[2] for k in ks], [med(grupos[k]["total"]) for k in ks], "o-", label=f"{fw} (total)")
            plt.plot([k[2] for k in ks], [med(grupos[k]["interno"]) for k in ks], "x--", label=f"{fw} (interno)")
        plt.xscale("log"); plt.xlabel("registros"); plt.ylabel("mediana (s)")
        plt.title("Q2 - tempo x volume"); plt.grid(alpha=.3); plt.legend(); plt.tight_layout()
        plt.savefig(OUT / "grafico_volume.png", dpi=150); plt.close()

    par = [k for k in grupos if k[0] == "paralelismo"]
    if par:
        for nome, fn in (("paralelismo", lambda k: med(grupos[k]["total"])),
                         ("speedup", lambda k: med(grupos[("paralelismo", k[1], k[2], 1)]["total"]) / med(grupos[k]["total"])
                          if ("paralelismo", k[1], k[2], 1) in grupos else float("nan"))):
            plt.figure(figsize=(6, 4))
            for fw in fws:
                for reg in sorted({k[2] for k in par}):
                    ks = sorted(k for k in par if k[1] == fw and k[2] == reg)
                    if ks:
                        plt.plot([k[3] for k in ks], [fn(k) for k in ks], "o-", label=f"{fw} {NOMES.get(reg, reg)}")
            if nome == "speedup":
                ps = sorted({k[3] for k in par}); plt.plot(ps, ps, "k:", label="ideal")
            plt.xlabel("paralelismo P"); plt.ylabel("mediana (s)" if nome == "paralelismo" else "speedup")
            plt.title(f"Q2 - {nome}"); plt.grid(alpha=.3); plt.legend(); plt.tight_layout()
            plt.savefig(OUT / f"grafico_{nome}.png", dpi=150); plt.close()
    print("graficos salvos em", OUT)


def main():
    grupos = carregar()
    md = ["# Resultados - Q2", "",
          "Tempo total = parede medido no shell (inclui submissão/inicialização do job ou sessão, leitura do HDFS, "
          "processamento e escrita do resultado no HDFS). Tempo interno = Hadoop: submissão→término do job; "
          "Spark: após criar a SparkSession até o fim da escrita.", "",
          "## Volume de dados", "", tabela(grupos, "volume"), "",
          "## Paralelismo", "", tabela(grupos, "paralelismo"), "", tabela_speedup(grupos), ""]
    (OUT / "tabelas.md").write_text("\n".join(md), encoding="utf-8")
    print("\n".join(md))
    graficos(grupos)


if __name__ == "__main__":
    main()
