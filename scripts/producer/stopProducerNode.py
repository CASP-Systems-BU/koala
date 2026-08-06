import psutil


if __name__ == "__main__":

    # Stop all processes with name containing "KafkaProducer"
    cnt = 0
    for proc in psutil.process_iter(["pid", "name"]):
        if "KafkaProducer" in proc.info["name"]:
            # Terminate the process
            print(
                f"[Producer INFO] Terminating process {proc.info['name']} with PID {proc.info['pid']}"
            )
            proc.terminate()
            proc.wait()
            cnt += 1
    print(f"[Producer INFO] Total {cnt} producer processes terminated on this node.")
