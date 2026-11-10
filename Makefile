CC          = gcc
CFLAGS      = -std=c11 -Wall -Wextra -D_POSIX_C_SOURCE=200809L
LDLIBS      = -pthread -lrt
TARGET      = ecom_system

MAIN_SRC    = supervisor.c
OTHER_SRCS  = workers.c services.c ipc.c
SRCS        = $(MAIN_SRC) $(OTHER_SRCS)

.PHONY: all run valgrind gtest coverage cppcheck misra ipcclean clean

all:
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS) $(LDLIBS)

run:
	./$(TARGET)

valgrind:
	valgrind --trace-children=yes --leak-check=full --show-leak-kinds=all --track-origins=yes --verbose ./$(TARGET)

gtest:
	gcc $(CFLAGS) --coverage -Dmain=ecom_application_main -c supervisor.c -o supervisor.o
	gcc $(CFLAGS) --coverage -DENABLE_GCOV_DUMP -c workers.c services.c ipc.c
	g++ -std=c++17 --coverage -o gtest_runner "gtest_ecom.cpp" supervisor.o workers.o services.o ipc.o -lgtest -lgtest_main -pthread -lrt
	./gtest_runner

coverage:
	gcov -b -c $(SRCS)

cppcheck:
	cppcheck --enable=all --inconclusive --std=c11 --language=c --suppressions-list=suppressionC.txt *.c 2> cppcheck.txt

misra:
	cppcheck --platform=unix64 --addon=misra --std=c11 --language=c --suppressions-list=suppressionM.txt *.c 2> misra.txt

# Removes leftover IPC objects after a crash (adjust names to your MQ_NAME / SHM_NAME)
ipcclean:
	rm -f /dev/mqueue/ecom_orders /dev/shm/ecom_*

clean:
	rm -f $(TARGET) gtest_runner *.o *.gcda *.gcno *.gcov cppcheck.txt misra.txt
