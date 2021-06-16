CC=g++
CXX=g++
LD=g++

SRC=MapReduceClient.cpp MapReduceFramework.cpp
OBJ=$(SRC:.cpp=.o)
OUT = libMapReduceFramework.a

INCS=-I.
CFLAGS = -Wall -std=c++14 -g $(INCS)
CXXFLAGS = -Wall -std=c++14 -g $(INCS)
LDFLAGS = -pthread

.cpp.o:
	$(CC) $(INCS) $(CXXFLAGS) -c $(SRC)

all: .cpp.o
	ar rcs $(OUT) $(OBJ)


#clean:
#	$(RM) $(TARGETS) $(EXE) $(OBJ) $(EXEOBJ) *~ *core

#depend:
#	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

#tar:
#	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
