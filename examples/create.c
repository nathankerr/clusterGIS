/* File: create.c
 * Author: Nathan Kerr
 *
 * Copies a dataset, adding a new record in the process.
 */

#include "clustergis.h"

int main(int argc, char** argv) {
	clusterGIS_dataset* dataset;
	clusterGIS_record* record;
	int rank;

	/* Process local arguments */
	if (argc != 3) {
		fprintf(stderr, "Usage: %s input output\n", argv[0]);
		exit(1);
	}

	clusterGIS_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	dataset = clusterGIS_Load_csv_distributed(MPI_COMM_WORLD, argv[1]);

	/* adds a new record to the beginning of the dataset */
	if(rank == 0) {
		int start = 0;
		record = clusterGIS_Create_record_from_csv("97123897,POINT(0 0),C\n", &start);
		record->next = dataset->data;
		dataset->data = record;
	}

	/* writes the records to disk */
	clusterGIS_Write_csv_distributed(MPI_COMM_WORLD, argv[2], dataset);

	/* Finalize */
	clusterGIS_Finalize();
	return 0;
}
