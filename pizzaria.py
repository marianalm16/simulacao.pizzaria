import multiprocessing
import os
import threading
import time

NUM_CLIENTES = 8
NUM_PIZZAIOLOS = 3
CAPACIDADE_FORNO = 2
ARQUIVO_SAIDA = "pizzas_prontas.txt"
SABORES = ["Calabresa", "Marguerita", "Frango com Catupiry", "Quatro Queijos"]


def cliente_worker(cliente_id, fila_pedidos, lock_print):
    time.sleep(0.5)
    sabor = SABORES[cliente_id % len(SABORES)]
    fila_pedidos.put({"id": cliente_id, "sabor": sabor})

    with lock_print:
        print(f"[CLIENTE] Pedido #{cliente_id} ({sabor}) realizado.")


def entrega_worker(pedido, lock_print):
    pedido_id = pedido["id"]

    with lock_print:
        print(f"[ENTREGA] Entregador a caminho da entrega #{pedido_id}...")

    time.sleep(2.0)

    with lock_print:
        print(f"[CONCLUÍDO] Pedido #{pedido_id} entregue ao cliente!")


def pizzaiolo_worker(
    fila_pedidos, semaforo_forno, vagas_ocupadas, lock_arquivo, lock_print
):
    pid = os.getpid()
    threads_entrega = []

    while True:
        pedido = fila_pedidos.get()

        if pedido is None:
            break

        pedido_id = pedido["id"]
        sabor = pedido["sabor"]

        with lock_print:
            print(
                f"[PIZZAIOLO {pid}] Iniciando preparo "
                f"da pizza #{pedido_id} ({sabor})."
            )

        time.sleep(3.0)

        semaforo_forno.acquire()

        try:
            with vagas_ocupadas.get_lock():
                vagas_ocupadas.value += 1
                ocupadas = vagas_ocupadas.value

            with lock_print:
                print(
                    f"[FORNO] Pizza #{pedido_id} entrou no forno "
                    f"({ocupadas}/{CAPACIDADE_FORNO} vagas ocupadas)."
                )

            time.sleep(2.5)

            with vagas_ocupadas.get_lock():
                vagas_ocupadas.value -= 1

            with lock_print:
                print(f"[FORNO] Pizza #{pedido_id} assada e retirada.")

        finally:
            semaforo_forno.release()

        with lock_arquivo:
            with open(ARQUIVO_SAIDA, "a", encoding="utf-8") as f:
                f.write(
                    f"Pedido #{pedido_id} | Sabor: {sabor} | "
                    f"Pizzaiolo PID: {pid}\n"
                )

        t_entrega = threading.Thread(
            target=entrega_worker, args=(pedido, lock_print)
        )

        t_entrega.start()
        threads_entrega.append(t_entrega)

    for t in threads_entrega:
        t.join()


def main():
    pid_gerente = os.getpid()

    arquitetura = (
        os.uname().machine
        if hasattr(os, "uname")
        else os.environ.get("PROCESSOR_ARCHITECTURE", "Desconhecida")
    )

    print("=== SISTEMA DA PIZZARIA AUTOMATIZADA INICIADO ===")
    print(f"Gerente PID: {pid_gerente} | Arquitetura: {arquitetura}\n")

    if os.path.exists(ARQUIVO_SAIDA):
        os.remove(ARQUIVO_SAIDA)

    fila_pedidos = multiprocessing.Queue()
    semaforo_forno = multiprocessing.Semaphore(CAPACIDADE_FORNO)
    vagas_ocupadas = multiprocessing.Value("i", 0)
    lock_arquivo = multiprocessing.Lock()
    lock_print = multiprocessing.Lock()

    processos_pizzaiolos = []

    for _ in range(NUM_PIZZAIOLOS):
        p = multiprocessing.Process(
            target=pizzaiolo_worker,
            args=(
                fila_pedidos,
                semaforo_forno,
                vagas_ocupadas,
                lock_arquivo,
                lock_print,
            ),
        )

        p.start()
        processos_pizzaiolos.append(p)

    threads_clientes = []

    for i in range(1, NUM_CLIENTES + 1):
        t = threading.Thread(
            target=cliente_worker, args=(i, fila_pedidos, lock_print)
        )

        t.start()
        threads_clientes.append(t)

    for t in threads_clientes:
        t.join()

    for _ in range(NUM_PIZZAIOLOS):
        fila_pedidos.put(None)

    for p in processos_pizzaiolos:
        p.join()

    print(
        f"\n=== EXPEDIENTE ENCERRADO! "
        f"Registros salvos em '{ARQUIVO_SAIDA}' ==="
    )


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
