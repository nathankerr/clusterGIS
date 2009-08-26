#include "clustergis.h"
#include "string.h"
#include "sys/stat.h"
#include "assert.h"

/* clusterGIS_Init
 *
 * Sets up the clusterGIS environment
 *
 * argc - count of arguments in argv
 * argv - char** of arguments
 */
void clusterGIS_Init(int* argc, char*** argv) {
	MPI_Init(argc, argv);
	initGEOS(NULL, NULL);

	clusterGIS_started = 1;
}

/* clusterGIS_Finalize
 *
 * Closes out the clusterGIS environment
 */
void clusterGIS_Finalize(void) {
	MPI_Finalize();
	finishGEOS();
}

/* dataset operations */

/* clusterGIS_Create_dataset
 *
 * Creates a new clusterGIS_dataset
 */
clusterGIS_dataset* clusterGIS_Create_dataset(void) {
	clusterGIS_dataset* dataset = malloc(sizeof(clusterGIS_dataset));
	dataset->data = NULL;

	return dataset;
}

/* clusterGIS_Load_csv_distributed
 *
 * Loads a portion of a dataset on each task
 *
 * comm - MPI communicator to use
 * filename - path to the dataset
 * dataset - the dataset which will be created
 */
clusterGIS_dataset* clusterGIS_Load_csv_distributed(MPI_Comm comm, char* filename) {
	MPI_File file;
	int err;
	char* buffer;
	MPI_Status status;
	clusterGIS_record** record;
	MPI_Offset offset;
	MPI_Offset chunkstart;
	MPI_Offset chunkend;
	int count;
	MPI_Offset filesize;
	int i;
	int start;
	int end;
	clusterGIS_dataset* dataset;
	int comm_rank;
	int comm_size;

	MPI_Comm_rank(comm, &comm_rank);
	MPI_Comm_size(comm, &comm_size);

	err = MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &file);
	assert(err == MPI_SUCCESS);

	buffer = (char*) malloc(CLUSTERGIS_BUFFERSIZE);
	MPI_File_get_size(file, &filesize);
	offset = 0;
	dataset = (clusterGIS_dataset*) malloc(sizeof(clusterGIS_dataset));
	record = &dataset->data;

	/* determine chunksizes, last task picks up the slack */
	chunkstart = comm_rank * (filesize / comm_size);
	if (comm_rank == comm_size - 1) {
		chunkend = filesize;
	} else { 
		chunkend = chunkstart + (filesize / comm_size) - 1;
	}

	offset = chunkstart;
	while(offset < chunkend - 1) {
		MPI_File_read_at(file, offset, buffer, CLUSTERGIS_BUFFERSIZE, MPI_CHAR, &status);
		MPI_Get_count(&status, MPI_CHAR, &count);

		/* determine the start of this record set */
		start = 0;
		if (offset == chunkstart && comm_rank != 0) {
			while(buffer[start] != '\n' && start < count) {
				start++;
			}
			if(start == count) {
				fprintf(stderr, "%d: buffer is too small\n",comm_rank);
				MPI_Abort(MPI_COMM_WORLD, 1);
			}
			start++;
		}

		/* determine the end of this record set */
		end = count - 1;
		while(end > 0 && buffer[end] != '\n') {
			end--;
		}
		if(end == 0) {
			fprintf(stderr, "%d: \\n not found from end\n", comm_rank);
			MPI_Abort(MPI_COMM_WORLD, 1);
		}
		end++;

		if(chunkend - offset < count) {
			/* Buffer overruns the responsibility of this task */
			end = chunkend - offset + 1;
			while(end < count && buffer[end] != '\n') {
				end++;
			}
			end++;
			if (end > count - 1) {
				end = chunkend - offset + 1;
				while(end > 0 && buffer[end] != '\n') {
					end--;
				}
				if(end == 0) {
					fprintf(stderr, "%d: \\n not found from end\n", comm_rank);
					MPI_Abort(MPI_COMM_WORLD, 1);
				}
				end++;
			}
		}

		/* Put the records into the dataset */
		i = start;
		while (i < end) {
			(*record) = clusterGIS_Create_record_from_csv(buffer, &i);
			(*record)->next = NULL;
			record = &(*record)->next;
			i++;
		}

		offset += end;
	}
	
	free(buffer);
	MPI_File_close(&file);

	return dataset;
}

/* clusterGIS_Load_csv_replicated
 *
 * Loads an entire copy of a csv data source on each task included in comm
 *
 * comm - MPI communicator of which all members will get a copy of this dataset
 * filename - path to the csv formatted dataset
 *
 * returns a pointer to the dataset
 */
clusterGIS_dataset* clusterGIS_Load_csv_replicated(MPI_Comm comm, char* filename) {
	MPI_File file;
	int err;
	char* buffer;
	int buffersize = 2*1024*1024;
	MPI_Status status;
	clusterGIS_record** record;
	MPI_Offset offset;
	int count;
	int last_full_record_end;
	MPI_Offset filesize;
	int i;
	clusterGIS_dataset* dataset;
	int comm_rank;

	MPI_Comm_rank(comm, &comm_rank);

	err = MPI_File_open(comm, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &file);
	if(err != MPI_SUCCESS) {
		fprintf(stderr, "%d: Error opening file %s\n", comm_rank, filename);
		MPI_Abort(comm, err);
	}

	buffer = (char*) malloc(buffersize);
	MPI_File_get_size(file, &filesize);
	offset = 0;
	dataset = (clusterGIS_dataset*) malloc(sizeof(clusterGIS_dataset));
	record = &dataset->data;

	while(offset < filesize) {
		MPI_File_read_at_all(file, offset, buffer, buffersize, MPI_CHAR, &status);
		MPI_Get_count(&status, MPI_CHAR, &count);
	
		/* find where the last full record ends */
		last_full_record_end = count - 1;
		while(buffer[last_full_record_end] != '\n' && last_full_record_end >= 0) {
			last_full_record_end--;
		}
		if(last_full_record_end < 0) {
			fprintf(stderr, "%d: Error in clusterGIS_Load_data_replicated: buffersize is too small, %d\n", comm_rank, count);
			MPI_Abort(comm, 1);
		}

		/* Put the records into the dataset */
		i = 0;
		while (i < last_full_record_end) {
			(*record) = clusterGIS_Create_record_from_csv(buffer, &i);
			(*record)->next = NULL;
			record = &(*record)->next;
			i++;
		}

		offset = offset + last_full_record_end + 1;
	}

	free(buffer);
	MPI_File_close(&file);
	
	return dataset;
}

/* clusterGIS_Write_csv
 *
 * Writes a dataset out to a file as csv
 *
 * filename - file to write to
 * dataset - dataset to write
 */
void clusterGIS_Write_csv(char* filename, clusterGIS_dataset* dataset) {
	FILE* file;
	clusterGIS_record* record;
	int i;

	remove(filename);
	file = fopen(filename, "w");

	record = dataset->data;
	while(record != NULL) {
		fprintf(file, "\"%s\"", record->data[0]);
		for(i = 1; i < record->columns; i++) {
			fprintf(file, ",\"%s\"", record->data[i]);
		}
		fprintf(file, "\n");
		record = record->next;
	}

	fclose(file);
}

/* clusterGIS_Write_csv_distributed
 *
 * Writes a distributed dataset to disk using MPI-IO
 *
 * comm - MPI communicator of the participants of the distributed dataset
 * filename - path of the file to write to
 * dataset - dataset to write
 */
void clusterGIS_Write_csv_distributed(MPI_Comm comm, char* filename, clusterGIS_dataset* dataset) {
	char* filename_part;
	MPI_Offset filesize;
	MPI_Offset offset;
	int i;
	int tmp;
	FILE* file_part;
	char* buffer;
	int buffersize=1024*1024;
	MPI_File file;
	struct stat *file_part_stat;
	int size;
	MPI_Status status;
	int comm_rank;
	int comm_size;

	MPI_Comm_rank(comm, &comm_rank);
	MPI_Comm_size(comm, &comm_size);

	/* Write local part of dataset to individual files */
	filename_part = (char*) malloc(strlen(filename) + 5);
	sprintf(filename_part, "%s.%d", filename, comm_rank);
	clusterGIS_Write_csv(filename_part, dataset);

	/* Figure out offset by talking with other tasks */
	file_part_stat = malloc(sizeof(struct stat));
	stat(filename_part, file_part_stat);
	filesize = (int) file_part_stat->st_size;
	free(file_part_stat);

	offset = 0;
	for(i = 0; i < comm_size; i++) {
		tmp = filesize;
		MPI_Bcast(&tmp, 1, MPI_INT, i, comm);
		if (i < comm_rank) {
			offset += tmp;
		}
	}

	/* Write local parts back together into a single large file */
	file_part = fopen(filename_part, "r");
	remove(filename);
	MPI_File_open(comm, filename, MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_INFO_NULL, &file);
	buffer = (char*) malloc(buffersize);

	size = fread(buffer, 1, buffersize, file_part);
	while(size != 0) {
		MPI_File_write_at(file, offset, buffer, size, MPI_CHAR, &status); 
		offset += size;
		size = fread(buffer, 1, buffersize, file_part);
	}
	free(buffer);
	fclose(file_part);
	remove(filename_part);
	MPI_File_close(&file);

	free(filename_part);
}

/* clusterGIS_Free_dataset
 *
 * Frees all memory associated with a dataset (all associated records, etc)
 *
 * dataset - the dataset to be freed
 */
void clusterGIS_Free_dataset(clusterGIS_dataset* dataset) {
	clusterGIS_record* head;
	clusterGIS_record* current;

	head = dataset->data;
	current = head;
	while(current != NULL) {
		head = current->next;
		clusterGIS_Free_record(current);
		current = head;
	}

	free(dataset);
}

/* clusterGIS_Create_record_from_csv
 * 
 * Creates a record from the given csv formatted char*
 *
 * csv - csv formatted representation of record
 * start - index of the start of the record in csv, returned with the end index
 *
 * Returns generated record
 */
clusterGIS_record* clusterGIS_Create_record_from_csv(char* csv, int* start) {
	int end = *start;
	int field_end;
	int field_count;
	int field_start;
	int i;
	int j;
	clusterGIS_record* record;

	struct item {
		char* data;
		int len;
		struct item* next;
	};
	struct item* head;
	struct item* current;

	/* find end of record */
	while(csv[end] != '\n') {
		end++;
	}
	
	/* Pull the fields out of the record. Assumes fields are comma delimited. Quotes surround fields with commas or quotes (escaped in the field) */
	i = *start;
	field_count = 0;
	head = (struct item*) malloc(sizeof(struct item));
	current = head;
	current->next = NULL;
	while(i < end) {
		/* get the start and stop indexes of the field */
		if(csv[i] == '"') {
			/* escaped field */
			i++;
			field_start = i;
			while (csv[i] != '"' && csv[i-1] != '\\') {
				i++;
			}
			field_end = i;
			i++; /* moves to the , or \n that follows the " */
		} else {
			/* non escaped field */
			field_start = i;
			while(csv[i] != ',' && csv[i] != '\n') {
				i++;
			}
			field_end = i;
		}

		/* add field to the linked list and move to the next field */
		current->data = (char*) malloc(field_end - field_start + 1);
		for(j = 0; j < field_end - field_start; j++) {
			current->data[j] = csv[field_start + j];
		}
		current->data[j] = '\0';

		current->next = (struct item*) malloc(sizeof(struct item));
		current = current->next;
		current->next = NULL;
		field_count++;
		i++;
	}

	/* Convert the linked list to an array of strings */
	current = head;
	i = 0;
	record = (clusterGIS_record*) malloc(sizeof(clusterGIS_record));
	record->data = malloc(field_count * sizeof(char*));
	record->columns = field_count;
	record->next = NULL;
	record->geometry = NULL;
	for(i = 0; i < field_count; i++) {
		record->data[i] = current->data;
		head = current->next;
		free(current);
		current = head;
	}

	(*start) = end;
	return record;
}

/* clusterGIS_Free_record
 *
 * Frees all memory associated with a record
 * 
 * record - the record to free
 */
void clusterGIS_Free_record(clusterGIS_record* record) {
	if(record != NULL) {
		free(record->data);
		free(record);
	}
}

/* MPI operations */
/* clusterGIS_Create_sub_communicator
 *
 * Creates a new MPI_Comm of size contiguous tasks
 *
 * comm - The communicator which to create a subset from
 * size - The size of the sub comminicator
 * new_comm - The address of the new communicator
 */
MPI_Comm clusterGIS_Create_chunked_communicator(MPI_Comm comm, int size) {
	int* members;
	int start;
	int end;
	int rank;
	int tasks;
	MPI_Group old_group;
	MPI_Group new_group;
	int i;
	MPI_Comm new_comm;

	MPI_Comm_rank(comm, &rank);
	MPI_Comm_size(comm, &tasks);
	MPI_Comm_group(comm, &old_group);

	/* Determine start and end of this group */
	start = rank - (rank % size);
	end = start + size - 1;
	if(end >= tasks) end = tasks - 1;
	size = end - start + 1;

	/* List the members in this group */
	members = malloc(sizeof(int) * size);
	for(i = 0; i < size; i++) {
		members[i] = start + i;
	}

	/* Create the new group, and then from it the new communicator */
	MPI_Group_incl(old_group, size, members, &new_group);
	MPI_Comm_create(comm, new_group, &new_comm);

	free(members);
	return new_comm;
}

MPI_Comm clusterGIS_Create_strided_communicator(MPI_Comm comm, int stride) {
	int* members;
	int comm_rank;
	int comm_size;
	MPI_Group old_group;
	MPI_Group new_group;
	int i;
	MPI_Comm new_comm;
	int rank;
	int position;

	MPI_Comm_rank(comm, &comm_rank);
	MPI_Comm_size(comm, &comm_size);
	MPI_Comm_group(comm, &old_group);

	/* Determine which stride group we are in */
	position = comm_rank % stride;

	/* List the members in this group */
	i = 0;
	members = malloc(sizeof(int) * comm_size);
	for(rank = 0; rank < comm_size; rank++) {
		if(rank % stride == position) {
			members[i] = rank;
			i++;
		}
	}

	/* Create the new group, and then from it the new communicator */
	MPI_Group_incl(old_group, i, members, &new_group);
	MPI_Comm_create(comm, new_group, &new_comm);

	free(members);
	return new_comm;
}

/* clusterGIS_Create_wkt_geometries
 *
 * Creates geometries in the dataset from the WKT formatted data in geometry_column
 *
 * dataset - dataset to be modified
 * geometry_column - column of the dataset the WKT formatted geometry is located in
 */
void clusterGIS_Create_wkt_geometries(clusterGIS_dataset* dataset, int geometry_column) {
	clusterGIS_record* record;

	record = dataset->data;
	while(record != NULL) {
		clusterGIS_Create_wkt_geometry(record, geometry_column);
		record = record->next;
	}
}

/* clusterGIS_Create_wkt_geometry
 *
 * Creates a geometry in the record from the WKT formatted datas in geometry_column
 *
 * record - the record to be modified
 * geometry_column - the column in record->data containing the WKT formatted geometry data
 */
void clusterGIS_Create_wkt_geometry(clusterGIS_record* record, int geometry_column) {
	GEOSWKTReader* reader = GEOSWKTReader_create();
	record->geometry = GEOSWKTReader_read(reader, record->data[geometry_column]);
	GEOSWKTReader_destroy(reader);
}
