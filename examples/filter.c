#include "clustergis.h"
#include "geos_c.h"

int main(int argc, char** argv) {
	GEOSGeometry* box;
	GEOSWKTReader* reader;
	clusterGIS_dataset* dataset;
	clusterGIS_record* record;
	clusterGIS_record** head;
	GEOSGeometry* record_geometry;
	int rank;
	double startprocessing;
	
	if(argc != 3) {
		fprintf(stderr, "Usage %s input output", argv[0]);
		exit(1);
	}

	clusterGIS_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	reader = GEOSWKTReader_create();

	box = GEOSWKTReader_read(reader, "POLYGON((-112.0859375 33.4349975585938,-112.0859375 33.4675445556641,-112.059799194336 33.4675445556641,-112.059799194336 33.4349975585938,-112.0859375 33.4349975585938))");
	dataset = clusterGIS_Load_csv_distributed(MPI_COMM_WORLD, argv[1]);
	clusterGIS_Create_wkt_geometries(dataset, 1);

	startprocessing = MPI_Wtime();
	record = dataset->data;
	head = &(dataset->data);
	/* keep records that match the criteria, otherwise delete them */
	while(record != NULL) {
		char intersects;

		/*record_geometry = GEOSWKTReader_read(reader, record->data[1]);
		if(record_geometry == NULL) {
			fprintf(stderr, "%d: parse error\n", rank);
			MPI_Abort(MPI_COMM_WORLD, 2);
		}*/

		intersects = GEOSIntersects(record->geometry, box);
		if(intersects == 2) {
			fprintf(stderr, "%d: error with overlap function\n", rank);
			MPI_Abort(MPI_COMM_WORLD, 2);
		}
		
		if(intersects == 1) { /* record overlaps with box */
			head = &(record->next);
			record = record->next;
		} else if(intersects == 0) { /* no overlap */
			*head = record->next;
			clusterGIS_Free_record(record);
			record = *head;
		} else {
			/* should never get here */
			MPI_Abort(MPI_COMM_WORLD, 2);
		}
	}
	printf("%d: processing time %5.2fs\n", rank, MPI_Wtime() - startprocessing);

	clusterGIS_Write_csv_distributed(MPI_COMM_WORLD, argv[2], dataset);

	clusterGIS_Finalize();
	return 0;
}
