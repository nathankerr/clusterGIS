#include "clustergis.h"

int main(int argc, char** argv) {
	int world_rank;
	int comm_rank;
	int comm_size;
	MPI_Comm strided;

	clusterGIS_Init(&argc, &argv);

	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	strided = clusterGIS_Create_strided_communicator(MPI_COMM_WORLD, 3);

	MPI_Comm_rank(strided, &comm_rank);
	MPI_Comm_size(strided, &comm_size);

	printf("%d: now %d of %d\n", world_rank, comm_rank, comm_size);

	clusterGIS_Finalize();
	return 0;
}
