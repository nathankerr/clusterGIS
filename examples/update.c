/* File: read.c
 * Author: Nathan Kerr
 *
 * Changes record 1008130 from R to C
 */

#include "clustergis.h"
#include "string.h"

int main(int argc, char** argv) {
	clusterGIS_dataset* dataset;
	clusterGIS_record* record;

	/* Process local arguments */
	if (argc != 3) {
		fprintf(stderr, "Usage: %s input output\n", argv[0]);
		exit(1);
	}

	/* Init */
	clusterGIS_Init(&argc, &argv);
	dataset =  clusterGIS_Load_csv_distributed(MPI_COMM_WORLD, argv[1]);

	record = dataset->data;
	/* keep records that match the criteria, otherwise delete them */
	while(record != NULL) {
		if(atoi(record->data[0]) == 1008130) {
			record->data[2] = "C";
		}
		record = record->next;
	}

	clusterGIS_Write_csv_distributed(MPI_COMM_WORLD, argv[2], dataset);

	/* Finalize */
	clusterGIS_Finalize();
	return 0;
}
