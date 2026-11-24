#!/usr/bin/env python3
"""Gera logs HTTP sintéticos reproduzíveis em CSV, sem dependências externas."""
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

RECURSOS = [('/index.html', 30), ('/produtos', 23), ('/login', 16), ('/busca', 12), ('/carrinho', 9), ('/contato', 6), ('/ajuda', 4)]
STATUS = [(200, 78), (301, 5), (403, 3), (404, 10), (500, 4)]

def gerar(qtd, destino, semente, ips, inicio):
    rng = random.Random(semente)
    inicio_dt = datetime.fromisoformat(inicio)
    recursos, pesos_recursos = zip(*RECURSOS)
    codigos, pesos_codigos = zip(*STATUS)
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open('w', newline='', encoding='utf-8') as arq:
        writer = csv.writer(arq, lineterminator='\n')
        writer.writerow(['timestamp', 'ip', 'recurso', 'status', 'bytes'])
        for i in range(qtd):
            instante = inicio_dt + timedelta(seconds=i // 3)
            numero_ip = rng.randrange(ips)
            ip = f'10.{(numero_ip // 65536) % 256}.{(numero_ip // 256) % 256}.{numero_ip % 256}'
            recurso = rng.choices(recursos, weights=pesos_recursos, k=1)[0]
            status = rng.choices(codigos, weights=pesos_codigos, k=1)[0]
            tamanho = 0 if status in (301, 403, 404) else rng.randint(200, 12000)
            writer.writerow([instante.isoformat(timespec='seconds'), ip, recurso, status, tamanho])
    print(f'Gerados {qtd} registros em {destino} (semente={semente}).')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registros', type=int, required=True, help='Quantidade de registros (>= 1)')
    parser.add_argument('--saida', required=True, help='Caminho do CSV de saída')
    parser.add_argument('--semente', type=int, default=42, help='Semente para reprodução (padrão: 42)')
    parser.add_argument('--ips', type=int, default=500, help='Número de IPs distintos (1 a 16777216)')
    parser.add_argument('--inicio', default='2026-10-01T00:00:00', help='Data/hora ISO inicial')
    args = parser.parse_args()
    if args.registros < 1 or not 1 <= args.ips <= 16777216:
        parser.error('--registros deve ser >= 1 e --ips deve estar entre 1 e 16777216')
    try:
        gerar(args.registros, args.saida, args.semente, args.ips, args.inicio)
    except ValueError as exc:
        parser.error(str(exc))

if __name__ == '__main__':
    main()
