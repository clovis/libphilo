#include "pack.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

//shouldn't be this much in main().  fix later.
int main(int argc, char **argv) {
	FILE *dbspecs4;
	GDBM_FILE gdbh;
	FILE *blocks;
	dbspec *dbs = NULL;
	hitbuffer *hb = NULL;

	N64 totalhits = 0;
	N64 wordhits = 0;
	N64 uniq_words = 0;
	char word[512];
	char page[512];
	int state;
	Z32 hit[10];

	//hack for pages
	hit[8] = 0;
//	fprintf(stderr, "%d arguments from command line.\n", argc);
	if (argc < 2) {
		exit(1);
	}
	
	//load dbspecs.
	fprintf(stderr, "reading dbspecs in from %s...\n", argv[1]);
	dbspecs4 = fopen(argv[1],"r");
	if (dbspecs4 == NULL) {
		fprintf(stderr, "couldn't open %s for reading.\n", argv[1]);
		exit(1);
	}
	dbs = init_dbspec_file(dbspecs4);
	if (dbs == NULL) {
		fprintf(stderr, "couldn't understand %s.\n", argv[1]);
		exit(1);
	}
	
	//initialize gdbm.
	gdbh = gdbm_open("index",0,GDBM_NEWDB, 0666,NULL);
	if (gdbh == NULL) {
		fprintf(stderr, "couldn't create index\n");
		exit(2);
	}
	
	//open the block file
	blocks = fopen("index.1","wb");
	if (blocks == NULL) {
		fprintf(stderr, "couldn't create index.1\n");
		exit(3);
	}
	
	//ugly...
	hb = malloc(sizeof(hitbuffer));

	hb->db = malloc(sizeof(dbh));
	hb->db->hash_file = gdbh;
	hb->db->block_file = blocks;
	hb->db->dbspec = dbs;
	strcpy(hb->word, "");

	hb->dir = malloc(hb->db->dbspec->block_size);
	hb->dir_malloced = hb->db->dbspec->block_size;
	hb->blk = malloc(hb->db->dbspec->block_size); // this is VERY conservative, but I should probably fix it.  
	hb->blk_malloced = hb->db->dbspec->block_size;

	hb->offset = 0;
	//all this should go in a subroutine.

	//scanning
	while(1) {
		state = fscanf(stdin,
		               "%s %d %d %d %d %d %d %d %d %s\n", 
		               word, &hit[0],&hit[1],&hit[2],&hit[3],&hit[4],&hit[5],&hit[6],&hit[7], page
					  );

		if (state == 10) {
			if ((strcmp(word,hb->word))) {				
				hitbuffer_finish(hb);
				hitbuffer_init(hb, word);
				uniq_words += 1;
			}
			hitbuffer_inc(hb, hit);
			totalhits += 1;
		}
		else if (state == EOF) {
			hitbuffer_finish(hb);
			break;
		}
		else {
			fprintf(stderr, "Couldn't understand hit.\n");
		}
	}

	fprintf(stderr, "done. %Ld hits packed in %Ld entries.\n", totalhits, uniq_words);
	return 0;
}

int hitbuffer_init(hitbuffer *hb, Z8 *word) {
	strncpy(hb->word,word,strlen(word) + 1);
	hb->type = 0;
	hb->freq = 0;
	hb->in_block = 0;
	hb->dir_length = 0;
	hb->blk_length = 0;
	hb->offset = ftell(hb->db->block_file);
	return 0;
}

int hitbuffer_inc(hitbuffer *hb, Z32 *hit) {
	hb->freq += 1;
	int result;
	if (hb->freq < PHILO_INDEX_CUTOFF) {
		add_to_dir(hb, hit, 1);
	}
	else if (hb->freq == PHILO_INDEX_CUTOFF) {
			result = add_to_block(hb,hb->dir,PHILO_INDEX_CUTOFF - 1);
			hb->dir_length = 0;
			hb->type = 1;
			result = add_to_dir(hb,hit,1);
		//	fprintf(stderr, "started new block for %s...", hb->word);
	}
	if (hb->freq > PHILO_INDEX_CUTOFF) {
		result = add_to_block(hb,hit,1);
		if (result == PHILO_BLOCK_FULL) {
			// IF the block add failed,
			write_blk(hb);
			hb->in_block = 0;
			add_to_dir(hb,hit,1);
		//	fprintf(stderr, "started new block for %s...", hb->word);
		}
	}		
	return 0;
}

int hitbuffer_finish(hitbuffer *hb) {
	if (!strcmp(hb->word, "")) {
		return 0;
	}
	if (hb->type == 0) {	
		fprintf(stderr, "%s: %Ld\n", hb->word, hb->freq, hb->dir_length);
		write_dir(hb);
	}
	else {
		fprintf(stderr, "%s: %Ld [%Ld blocks]\n", hb->word, hb->freq, hb->dir_length);
		write_dir(hb);
		write_blk(hb);
	}
	return 0;
}

int add_to_dir(hitbuffer *hb, Z32 *data, N64 count) {
	void *status;
	while (hb->dir_malloced < ((hb->dir_length + count) * (hb->db->dbspec->fields) * sizeof(Z32)) ) {
		status = realloc(hb->dir, 2 * hb->dir_malloced);
		if (status) {
			hb->dir_malloced = 2 * hb->dir_malloced; //I'm trying to save time from calling malloc over and over again, but is this too aggressive?
		}
		else {
			fprintf(stderr, "out of memory: couldn't add hit to directory.  \nconsider using disk for index state, or adjusting the realloc amount.\n");
			exit(1);
		}
	}
	memcpy(hb->dir + (hb->dir_length * (hb->db->dbspec->fields) ), //the address of the new hits
		   data, //the data buffer
	       count * (hb->db->dbspec->fields) * sizeof(Z32) //the size of the data buffer
	      );
	hb->dir_length += count;
	return 0;
}

int add_to_block(hitbuffer *hb, Z32 *data, N64 count) {
	N64 maxsize = 0;
	N64 remaining = count;
	//N64 hits_to_copy = 0;

	// we need to see if the block needs to be written; 
	// for that, we need to calculate the maximum number of hits per block,
	// AFTER compression.

	N64 hits_per_block = (hb->db->dbspec->block_size * 8)  / (hb->db->dbspec->bitwidth);
	N64 free_space = hits_per_block - hb->blk_length;

	if (count <= (hits_per_block - hb->blk_length) ) { 
		//if (remaining < hits_to_copy) {hits_to_copy = remaining;}
		memcpy(hb->blk + (hb->blk_length * (hb->db->dbspec->fields) ),
			   data,
			   count * hb->db->dbspec->fields * sizeof(Z32)
			  );
		hb->blk_length += count;
		//remaining -= hits_to_copy; 
		return 0; //should probably return how many hits were packed.
	}
	else {
		//the block is full, or full enough.
		//should revisit this later.
		//as it is, writing multiple hits won't work well.
		return PHILO_BLOCK_FULL;
	}
}

int write_dir(hitbuffer *hb) {
	int header_size;
	int buffer_size;
	int offset = 0;
	int bit_offset = 0;
	
	datum key, value;
	
	int i;
	int j;
	
	const dbspec *dbs = hb->db->dbspec;
	char *valbuffer;
	
	if (hb->type = 0) {
		header_size = dbs->type_length + dbs->freq1_length;
	}
	else {
		header_size = dbs->type_length + dbs->freq2_length + dbs->offset_length;
	}
	
	buffer_size = ( (header_size + (hb->dir_length * dbs->bitwidth)  ) / 8) + 1;
	valbuffer = malloc(buffer_size);
	
	if (valbuffer == NULL) {
		//should add helpful error here.
		exit(1);
	}

	//Compress..
	compress(valbuffer,offset,bit_offset,hb->type,dbs->type_length);
	offset += dbs->type_length / 8;
	bit_offset += dbs->type_length;

	compress(valbuffer,offset,bit_offset,hb->freq,dbs->freq1_length);
	offset += dbs->freq1_length / 8;
	bit_offset = (bit_offset + dbs->freq1_length) % 8;

	for (i = 0; i < hb->dir_length; i++) {
		for (j = 0; j < dbs->fields; j++) {
			compress(valbuffer,offset,bit_offset,hb->dir[i*dbs->fields + j] + dbs->negatives[j], dbs->bitlengths[j]);
			offset += dbs->bitlengths[j] / 8;
			bit_offset = (bit_offset + dbs->bitlengths[j]) % 8;
		}
	}

	//Write to GDBM
	
	key.dptr = hb->word;
	key.dsize = strlen(hb->word);
	value.dptr = valbuffer;
	value.dsize = buffer_size;
	gdbm_store(hb->db->hash_file, key, value,GDBM_REPLACE);
	free(valbuffer);
	return 0;
}

int write_blk(hitbuffer *hb) {
	int width = 32;
	int bytewidth = 4;
	int offset = 0;
	int bit_offset = 0;
//	fprintf(stderr, "writing %Ld hits.\n", hb->blk_length);
	char *valbuffer = malloc(hb->db->dbspec->block_size);
	//Compress and write...
	//Don't forget the block-end flag iff a block ends prematurely.
	hb->blk_length = 0;
	return 0;
}

int compress(char *bytebuffer, int byte, int bit, Z32 data, int size) {
	int free_space = 8 - bit;
	int remaining = size;
	int l_shift;
	int r_shift;
	int to_do = 0;
	int bits_done = 0;
	char word;
	while (remaining > 0) {
		if (free_space == 0) {
			byte += 1;
			free_space = 8;
			bit = 0;
		}
		to_do = remaining >= free_space ? free_space : remaining;
		l_shift = 32 - to_do;
		r_shift = bits_done;
		data >>= r_shift; //trim off what we've done already.
		word = (data << l_shift) >> l_shift; //trim off the more significant bits
		bytebuffer[byte] |= word << bit; // |= to avoid over/underflows, etc...
		bits_done += to_do;
		bit += to_do;
		remaining -= to_do;
		free_space -= to_do;
	}
	return 0;	
}