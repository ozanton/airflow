import subprocess
import logging

def main():
    # запуск run_windy.sh
    process = subprocess.Popen(["bash", "/myproject/windyapi/run_windy.sh"], stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"run_windy.sh exited with code {process.returncode}")
    # логи вывода
    logging.info(f"stdout: {stdout}")
    if stderr:
        logging.error(f"stderr: {stderr}")

if __name__ == '__main__':
    main()