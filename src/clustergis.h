#ifndef CLUSTERGIS_H
#define CLUSTERGIS_H

#define CLUSTERGIS_BUFFERSIZE 2*1024*1024

#include "stdio.h"
#include "stdlib.h"
#include "mpi.h"
#include "geos_c.h"

/* variables */
int clusterGIS_started;

/* datatypes */
struct clusterGIS_record_el {
	char** data;
	int columns;
	GEOSGeometry* geometry;
	struct clusterGIS_record_el * next;
};
typedef struct clusterGIS_record_el clusterGIS_record;
struct clusterGIS_dataset {
	clusterGIS_record* data;
};
typedef struct clusterGIS_dataset clusterGIS_dataset;

/* startup and shutdown */
void clusterGIS_Init(int* argc, char*** argv);
void clusterGIS_Finalize(void);

/* dataset operations */
clusterGIS_dataset* clusterGIS_Create_dataset(void);
clusterGIS_dataset* clusterGIS_Load_csv_distributed(MPI_Comm comm, char* filename);
clusterGIS_dataset* clusterGIS_Load_csv_replicated(MPI_Comm comm, char* filename);
void clusterGIS_Write_csv(char* filename, clusterGIS_dataset* dataset);
void clusterGIS_Write_csv_distributed(MPI_Comm comm, char* filename, clusterGIS_dataset* dataset);
void clusterGIS_Free_dataset(clusterGIS_dataset* dataset);

/* record operations */
clusterGIS_record* clusterGIS_Create_record_from_csv(char* csv, int* size);
void clusterGIS_Free_record(clusterGIS_record* record);

/* MPI operations */
MPI_Comm clusterGIS_Create_chunked_communicator(MPI_Comm comm, int size);
MPI_Comm clusterGIS_Create_strided_communicator(MPI_Comm comm, int stride);

/* Geometry operations */
void clusterGIS_Create_wkt_geometries(clusterGIS_dataset* dataset, int geometry_column);
void clusterGIS_Create_wkt_geometry(clusterGIS_record* record, int geometry_column);

#endif
