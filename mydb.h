#include <stddef.h>

/* check `man dbopen` */

struct DBT {
	size_t size;
	void  *data;
};

struct DB {
	int fd;
	size_t size;
	size_t page_size;
	size_t page_count;
	size_t head;
	/* Public API */
	/* Returns 0 on OK, -1 on Error */
	int (*test)(struct DB *db, int i);
	int (*close)(struct DB *db);
	int (*delete)(struct DB *db, struct DBT *key);
	int (*insert)(struct DB *db, struct DBT *key, struct DBT *data);
	/* * * * * * * * * * * * * *
	 * Returns malloc'ed data into 'struct DBT *data'.
	 * Caller must free data->data. 'struct DBT *data' must be alloced in
	 * caller.
	 * * * * * * * * * * * * * */
	int (*select)(struct DB *db, struct DBT *key, struct DBT *data);
	/* Sync cached pages with disk
	 * */
	int (*sync)(struct DB *db);
	/* For future uses - sync cached pages with disk
	 * int (*sync)(const struct DB *db)
	 * */
	/* Private API */
	/*     ...     */
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DBC {
	/* Maximum on-disk file size
	 * 512MB by default
	 * */
	size_t db_size;
	/* Page (node/data) size
	 * 4KB by default
	 * */
	size_t page_size;
	/* Maximum cached memory size
	 * 16MB by default
	 * */
	size_t cache_size;
};

typedef struct node_header
{
	size_t keys_count;
	long   free_bytes;
	struct DBT *keys;
	struct DBT *vals;
	size_t *pointers;
} node_header;

struct Proxy
{
	size_t left;
	size_t right;
	struct DBT *key;
	struct DBT *val;
};

/* Open DB if it exists, otherwise create DB */
struct DB *dbopen(const char *file, const struct DBC *conf);

size_t min(size_t a, size_t b);
void free_node(node_header *Node);
void free_proxy(struct Proxy *proxy);
size_t alloc(struct DB *db);
void free_block(struct DB *db, size_t block);
void read_block(struct DB *db, char *page, size_t N);
void write_block(struct DB *db, char *page, size_t N);
node_header *get_node(char *page);
void put_node(char *page, node_header *Node);
node_header *read_node(struct DB *db, size_t N);
void write_node(struct DB *db, node_header *Node, size_t N);
int key_cmp(struct DBT *key1, struct DBT *key2);
void set_val(struct DBT *dest, struct DBT *src);
struct Proxy *refresh_pair(struct DB *db, node_header *Node, struct Proxy *proxy);
struct Proxy *add_pair(struct DB *db, node_header *Node, struct Proxy *proxy);
struct Proxy *check_node(struct DB *db, node_header *Node, struct Proxy *proxy);
void show_node(node_header *Node);
struct Proxy *create_proxy(size_t left, size_t right, struct DBT *key, struct DBT *val);

int db_close(struct DB *db);
int db_delete(struct DB *, void *, size_t);
int db_select(struct DB *, void *, size_t, void **, size_t *);
int db_insert(struct DB *, void *, size_t, void * , size_t  );

/* Sync cached pages with disk */
int db_sync(const struct DB *db);
