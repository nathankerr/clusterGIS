#include "clustergis.h"

int main(int argc, char** argv) {
	clusterGIS_dataset* dataset;
	clusterGIS_record* record;
	int count;
	int total_count;
	int last_id;
	int previous_id;
	int first_id;
	MPI_Status status;
	int rank;
	int tasks;

	/* Process local arguments */
	if (argc != 2) {
		fprintf(stderr, "Usage: %s input\n", argv[0]);
		exit(1);
	}

	clusterGIS_Init(&argc, &argv);
	dataset = clusterGIS_Load_csv_distributed(MPI_COMM_WORLD, argv[1]);

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &tasks);

	record = dataset->data;
	last_id = atoi(record->data[0]) - 1;
	count = 0;
	while(record != NULL) {
		count++;
		if(atoi(record->data[0]) != last_id + 1) {
			printf("%d: MISSING RECORD %d\n", rank, last_id + 1);
		}
		last_id = atoi(record->data[0]);
		record = record->next;
	}

	MPI_Reduce(&count, &total_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

	if(rank == 0) {
		printf("Count: %d\n", total_count);
	}

	if (tasks > 1) {
		if (rank == 0) {
			MPI_Send(&last_id, 1, MPI_INT, rank+1, 99, MPI_COMM_WORLD);
			previous_id = 0;
		} else if (rank == tasks - 1) {
			MPI_Recv(&previous_id, 1, MPI_INT, rank-1, 99, MPI_COMM_WORLD, &status);
		} else {
			MPI_Sendrecv(&last_id, 1, MPI_INT, rank+1, 99, &previous_id, 1, MPI_INT, rank-1, 99, MPI_COMM_WORLD, &status);
		}
		first_id = atoi(dataset->data->data[0]);
		if(previous_id == first_id) {
			printf("Record %d is duplicated between tasks %d and %d\n", previous_id, rank - 1, rank);
		} else 	if(previous_id + 1 != first_id) {
			printf("Record %d is missing between tasks %d and %d\n", previous_id + 1, rank -1, rank);
		}
		//printf("%d: %d, %s - %s\n", rank, previous_id, dataset->data->data[0], last_record->data[0]);
	}
	
	/* Finalize */
	clusterGIS_Finalize();
	return 0;
}
