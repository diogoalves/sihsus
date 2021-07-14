DATASUS_DIR = datasus.gov.br
DATASET_DIR = dataset

DATASUS_DOWNLOAD_MASK = "open ftp.datasus.gov.br:/dissemin/publicos/SIHSUS/200801_/dados; mirror -i RDAC..01.dbc ./ ./$(DATASUS_DIR)"


DBC_FILES = $(sort $(wildcard ./datasus.gov.br/*.dbc))
PQ_FILES = $(addprefix $(DATASET_DIR)/,$(notdir ${subst dbc,pq,$(DBC_FILES)}))

.PHONY = pipeline init dataset clean

# Data pipeline definition
#
pipeline:
		$(MAKE) init
		$(MAKE) generate-parquets -j 4
		$(MAKE) dataset


# Initialization
#
init:
	@ echo  "\n[Creating directories and syncing with DataSus.gov.br repository]"
	@ mkdir -p $(DATASUS_DIR)
	@ mkdir -p $(DATASET_DIR)
	@ @ lftp -c $(DATASUS_DOWNLOAD_MASK)


# Generate parquet files
#
generate-parquets: $(PQ_FILES) blast-dbf/blast-dbf

dataset/%.pq: ./datasus.gov.br/%.dbc
	@ echo "\n[Generating parquet] $@ from $<" 
	@ python3 dbc2parquet.py $< $@
	
blast-dbf/blast-dbf:
	cd blast-dbf && $(MAKE) -f Makefile


# Create parquet dataset
#
dataset: $(DATASET_DIR)/_metadata

$(DATASET_DIR)/_metadata: $(PQ_FILES)
	@ echo  "\n[Creating dataset metadata...]"
	@ python3 merge.py
	@ echo  "\n[Done!]"



# Clean
#
clean:
	-@ rm ${DATASUS_DIR}/*.dbf 2> /dev/null 
	@ if [ -z ${DATASET_DIR} ]; then echo "\n[DATASET_DIR is unset]"; else echo "\n[Removing directory '${DATASET_DIR}']" && rm -rf ${DATASET_DIR}; fi
	@ cd blast-dbf && $(MAKE) -f Makefile clean