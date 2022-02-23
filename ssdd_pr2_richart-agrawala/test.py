import sys
import threading
import os

def run_node(process, number, local_file, users_file_ra, users_file_fm):
    cmd = process+" "+number+" "+local_file+" "+users_file_ra+" "+users_file_fm
    os.system(f"start cmd /k {cmd}")

def main():
    # sys.argv[1]: nodos_WRITE
    # sys.argv[2]: nodos_READ
    # sys.argv[3]: local_file
    # sys.argv[4]: users_file_ra
    # sys.argv[5]: users_file_fm
    os.system("go build escritor.go")
    os.system("go build lector.go")
    node = 1
    thread_list = []
    for i in range(int(sys.argv[1])):
        thread_list.append(threading.Thread(target=run_node,args=("escritor.exe",str(node),sys.argv[3],sys.argv[4],sys.argv[5],)))
        node += 1
    for i in range(int(sys.argv[2])):
        thread_list.append(threading.Thread(target=run_node,args=("lector.exe",str(node),sys.argv[3],sys.argv[4],sys.argv[5],)))
        node += 1
    for i in thread_list:
        i.start()
    for i in thread_list:
        i.join()

if __name__ == "__main__":
    main()