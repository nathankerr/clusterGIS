#include "clustergis.h"
#include "string.h"

#define BLOCK_SIZE 8
#define EMPLOYERS_GEOMETRY_COLUMN 1
#define PARCELS_GEOMETRY_COLUMN 1

/* reduce function for min distances */
void min_distance_function (double *invec, double* outvec, int *len, MPI_Datatype *datatype) {
	/* outvec[i] = invec[i] op outvec[i] */
	if(invec[1] < outvec[1]) {
		outvec[0] = invec[0];
		outvec[1] = invec[1];
	}
}

int main(int argc, char** argv) {
	char* employers_filename;
	char* parcels_filename;
	MPI_Comm employers_comm;
	MPI_Comm parcels_comm;
	clusterGIS_dataset* employers;
	clusterGIS_dataset* parcels;
	clusterGIS_record* employer;
	clusterGIS_record* parcel;
	double distance;
	double min_distance;
	clusterGIS_record* min_distance_parcel;
	double *min;
	double *global_min;
	int world_rank;
	int employers_processed;
	double starttime;
	double average_time;
	int employers_count;
	MPI_Op min_distance_op;
	char* output_csv;
	clusterGIS_dataset* output = NULL;
	clusterGIS_record* output_record = NULL;
	int start = 0;
	char* output_filename;

	clusterGIS_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	MPI_Op_create((MPI_User_function*) min_distance_function, 1, &min_distance_op);

	if(argc != 4 && world_rank == 0) {
		printf("Usage: %s employers parcels output\n", argv[0]);
		MPI_Abort(MPI_COMM_WORLD, 1);
	}
	employers_filename = argv[1];
	parcels_filename = argv[2];
	output_filename = argv[3];


	/* Load data into appropriate communicators and create their geometries */
	employers_comm = clusterGIS_Create_strided_communicator(MPI_COMM_WORLD, BLOCK_SIZE);
	employers = clusterGIS_Load_csv_distributed(employers_comm, employers_filename);
	clusterGIS_Create_wkt_geometries(employers, EMPLOYERS_GEOMETRY_COLUMN);
	parcels_comm = clusterGIS_Create_chunked_communicator(MPI_COMM_WORLD, BLOCK_SIZE);
	parcels = clusterGIS_Load_csv_distributed(parcels_comm, parcels_filename);
	clusterGIS_Create_wkt_geometries(parcels, PARCELS_GEOMETRY_COLUMN);

	employer = employers->data;
	employers_count = 0;
	while(employer != NULL) {
		employers_count++;
		employer = employer->next;
	}

	starttime = MPI_Wtime();

	/* Find the min distance */
	employer = employers->data;
	employers_processed = 0;
	min = malloc(sizeof(double)*2);
	global_min = malloc(sizeof(double)*2);
	output_csv = malloc(sizeof(char)*128);
	output = clusterGIS_Create_dataset();
	while(employer != NULL) {

		/* find the local min */
		parcel = parcels->data;
		GEOSDistance(employer->geometry, parcel->geometry, &min_distance);
		min_distance_parcel = parcel;
		while(parcel != NULL) {
			if(strncmp(employer->data[2], parcel->data[2], 1) == 0) {
				GEOSDistance(employer->geometry, parcel->geometry, &distance);
				if(distance < min_distance) {
					min_distance = distance;
					min_distance_parcel = parcel;
				}
			}
			parcel = parcel->next;
		}

		/* find the global min */
		min[0] = atoi(min_distance_parcel->data[0]);
		min[1] = min_distance;
		MPI_Allreduce(min, global_min, 2, MPI_DOUBLE, min_distance_op, parcels_comm);

		/* Add the min to the output dataset */
		sprintf(output_csv, "\"%s\",\"%d\"\n", employer->data[0], (int) global_min[0]);
		start = 0;
		output_record = clusterGIS_Create_record_from_csv(output_csv, &start);
		output_record->next = output->data;
		output->data = output_record;

		employer = employer->next;
		employers_processed++;
		if(world_rank == 0 && employers_processed % 100 == 0) {
			average_time = (MPI_Wtime() - starttime) / employers_processed;

			printf("%d/%d employers processed at %f s/employers, approx. %f minutes left\n", employers_processed, employers_count, average_time, ((employers_count - employers_processed) * average_time)/60);
		}
	}

	if(world_rank % BLOCK_SIZE == 0) {
		clusterGIS_Write_csv_distributed(employers_comm, output_filename, output);
	}

	MPI_Op_free(&min_distance_op);
	clusterGIS_Finalize();
	return 0;
}
