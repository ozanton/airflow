import subprocess
import logging

def main():
    # запуск run_current.sh с полным путем
    process = subprocess.Popen(["bash", "/myproject/weatherapi/run_current.sh"], stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"run_current.sh exited with code {process.returncode}")
    # логи вывода
    logging.info(f"stdout: {stdout}")
    if stderr:
        logging.error(f"stderr: {stderr}")

if __name__ == '__main__':
    main()