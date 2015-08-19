#include "mydb.h"
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <fcntl.h>

#define SIZE sizeof(size_t)

// FREES MEMORY, ALLOCATED TO NODE
void free_node(node_header *Node)
{
	for (size_t it = 0; it < Node->keys_count; ++it)
	{
		free(Node->keys[it].data);
		free(Node->vals[it].data);
	}

	free(Node->keys);
	free(Node->vals);
	free(Node->pointers);
	free(Node);
}

void free_proxy(struct Proxy *proxy)
{
	free(proxy->key->data);
	free(proxy->val->data);
	free(proxy->key);
	free(proxy->val);
	free(proxy);
}

//FINDS FREE BLOCK AND MARKS IT AS 'USED'
size_t alloc(struct DB *db)
{
	char *table = (char *) malloc(db->size / db->page_size);

	lseek(db->fd, 0, SEEK_SET);
	read(db->fd, table, db->size / db->page_size);

	size_t it = 0;
	for (it = 0; table[it]; ++it);
	table[it] = 1;

	lseek(db->fd, 0, SEEK_SET);
	write(db->fd, table, db->size / db->page_size);

	free(table);
	return it;
}

//MARKS BLOCK IN THE TABLE AS 'FREE' AND WRITES FREE BLOCK TO DISK
void free_block(struct DB *db, size_t block)
{
	lseek(db->fd, block, SEEK_SET);
	write(db->fd, "\0", 1);

	char *page = (char *) calloc(db->page_size, 1);
	size_t free_bytes = db->page_size - 2*SIZE;
	memcpy(page + SIZE, &free_bytes, SIZE);

	write_block(db, page, block);

	free(page);
}

//READING BLOCK FROM DISK TO MEMORY
void read_block(struct DB *db, char *page, size_t N)
{
	lseek(db->fd, N * db->page_size, SEEK_SET);
	read(db->fd, page, db->page_size);	
}

//WRITING BLOCK FROM MEMORY TO DISK
void write_block(struct DB *db, char *page, size_t N)
{
	lseek(db->fd, N * db->page_size, SEEK_SET);
	write(db->fd, page, db->page_size);	
}


//READING NODE FROM MEMORY
node_header *get_node(char *page)
{
	node_header *Node = (node_header *) malloc(sizeof(node_header));
	Node->keys = NULL;
	Node->vals = NULL;
	Node->pointers = NULL;
	Node->keys_count = *((size_t *) page);
	Node->free_bytes = *((size_t *) (page + SIZE));

	size_t it = 0;
	size_t offset = 2 * SIZE;
	for (it = 0; it < Node->keys_count; ++it)
	{
		//pointer
		Node->pointers = (size_t *) realloc(Node->pointers, (it + 1) * SIZE);
		Node->pointers[it] = *((size_t *) (page + offset));
		offset += SIZE;

		//key size
		Node->keys = (struct DBT *) realloc(Node->keys, (it + 1) * sizeof(struct DBT));
		Node->keys[it].size = *((size_t *) (page + offset));
		offset += SIZE;

		//key data
		Node->keys[it].data = (char *) malloc(Node->keys[it].size);
		memcpy(Node->keys[it].data, page + offset, Node->keys[it].size);
		offset += Node->keys[it].size;

		//value size
		Node->vals = (struct DBT *) realloc(Node->vals, (it + 1) * sizeof(struct DBT));
		Node->vals[it].size = *((size_t *) (page + offset));
		offset += SIZE;

		//value data
		Node->vals[it].data = (char *) malloc(Node->vals[it].size);
		memcpy(Node->vals[it].data, page + offset, Node->vals[it].size);
		offset += Node->vals[it].size;
	}

	//last pointer
	Node->pointers = (size_t *) realloc(Node->pointers, (it + 1) * SIZE);
	Node->pointers[it] = *((size_t *) (page + offset));
	
	return Node;
}

//WRITING NODE TO MEMORY
void put_node(char *page, node_header *Node)
{
	memcpy(page, &Node->keys_count, SIZE);
	memcpy(page + SIZE, &Node->free_bytes, SIZE);

	size_t it = 0;
	size_t offset = 2*SIZE;
	for (it = 0; it < Node->keys_count; ++it)
	{
		//pointer
		memcpy(page + offset, Node->pointers + it, SIZE);
		offset += SIZE;

		//key size
		memcpy(page + offset, &Node->keys[it].size, SIZE);
		offset += SIZE;

		//key
		memcpy(page + offset, Node->keys[it].data, Node->keys[it].size);
		offset += Node->keys[it].size;

		//value size
		memcpy(page + offset, &Node->vals[it].size, SIZE);
		offset += SIZE;

		//value
		memcpy(page + offset, Node->vals[it].data, Node->vals[it].size);
		offset += Node->vals[it].size;
	}

	//last pointer
	memcpy(page + offset, Node->pointers + it, SIZE);
}

node_header *read_node(struct DB *db, size_t N)
{
	char *page = (char *) malloc(db->page_size);
	read_block(db, page, N);
	node_header *Node = get_node(page);
	free(page);
	return Node;
}

void write_node(struct DB *db, node_header *Node, size_t N)
{
	char *page = NULL;
	page = (char *) calloc(db->page_size, 1);
	put_node(page, Node);
	write_block(db, page, N);
	free(page);
}

size_t min(size_t a, size_t b)
{
	if (a < b)
		return a;
	else
		return b;
}

//COMPARES TWO KEYS
//RETURNS 0  IF KEY1 == KEY2
//RETURNS >0 IF KEY1 >  KEY2
//RETURNS <0 IF KEY1 <  KEY2
int key_cmp(struct DBT *key1, struct DBT *key2)
{
	int res = memcmp(key1->data, key2->data, min(key1->size, key2->size));
	if (res)
		return res;
	if (key1->size == key2->size)
		return 0;
	if (key1->size > key2->size)
		return 1;
	return -1;
}

// IF NODE SIZE <= BLOCK SIZE RETURNS NULL
// IF NODE SIZE >  BLOCK SIZE, CREATES TWO NODES AND RETURNS PROXY OF MIDDLE ELEMENT
//    PROXY CONSISTS OF:
// 1) ADDRESS OF LEFT NODE ON DISK
// 2) ADDRESS OF RIGHT NODE ON DISK
// 3) KEY : VALUE PAIR TO INSERT IN PARENT NODE
struct Proxy *check_node(struct DB *db, node_header *Node, struct Proxy *proxy)
{

	if (Node->free_bytes >= 0)
	{
		free_proxy(proxy);
		return NULL;
	}

	free_proxy(proxy);

	//filling return Proxy
	proxy = create_proxy(alloc(db), 
						 alloc(db), 
						 Node->keys + Node->keys_count / 2, 
						 Node->vals + Node->keys_count / 2);

	//filling left Node
	struct Proxy *lproxy;
	node_header *lNode = read_node(db, proxy->left);
	for (size_t it = 0; it < Node->keys_count / 2; ++it)
	{
		lproxy = create_proxy(Node->pointers[it], 
							  Node->pointers[it+1],
			                  Node->keys + it,
			                  Node->vals + it);

		add_pair(db, lNode, lproxy);
	}
	write_node(db, lNode, proxy->left);

	//filling right Node
	struct Proxy *rproxy;
	node_header *rNode = read_node(db, proxy->right);
	for (size_t it = Node->keys_count / 2 + 1; it < Node->keys_count; ++it)
	{
		rproxy = create_proxy(Node->pointers[it], 
							  Node->pointers[it+1],
			                  Node->keys + it,
			                  Node->vals + it);

		add_pair(db, rNode, rproxy);
	}
	write_node(db, rNode, proxy->right);

	//FREE
	free_node(lNode);
	free_node(rNode);

	return proxy;
}

void set_val(struct DBT *dest, struct DBT *src)
{
	if (dest == NULL)
		dest = (struct DBT *) malloc(sizeof(struct DBT));

	dest->size = src->size;
	if (dest->data) 
		free(dest->data);
	dest->data = (char *) malloc(dest->size);
	memcpy(dest->data, src->data, dest->size);
}

// CHANGES VALUE IN NODE
struct Proxy *refresh_pair(struct DB *db, node_header *Node, struct Proxy *proxy)
{
	size_t it = 0;
	for (it = 0; key_cmp(Node->keys + it, proxy->key); ++it);
	
	Node->free_bytes -= (proxy->val->size - Node->vals[it].size);

	set_val(Node->vals + it, proxy->val);

	return check_node(db, Node, proxy);
}

// ADDS KEY : VALUE PAIR FROM PROXY TO NODE
struct Proxy *add_pair(struct DB *db, node_header *Node, struct Proxy *proxy)
{
	Node->keys_count++;
	Node->free_bytes -= (proxy->key->size + proxy->val->size + 3*SIZE);  //key + value + pointer + key.size + value.size
	Node->keys     = (struct DBT *) realloc(Node->keys, Node->keys_count * sizeof(struct DBT));
	Node->vals     = (struct DBT *) realloc(Node->vals, Node->keys_count * sizeof(struct DBT));
	Node->pointers = (size_t *)     realloc(Node->pointers, (Node->keys_count+1) * SIZE);

	Node->keys[Node->keys_count - 1].data = NULL;
	Node->vals[Node->keys_count - 1].data = NULL;

	//adding to new block
	if (Node->keys_count == 1)
	{	
		Node->free_bytes -= SIZE; //another pointer

		set_val(Node->keys, proxy->key);
		set_val(Node->vals, proxy->val);
		Node->pointers[0] = proxy->left;
		Node->pointers[1] = proxy->right;

		return check_node(db, Node, proxy);
	}

	//adding to existing block
	for (size_t it = 0; it < Node->keys_count-1; ++it)
	{
		if (key_cmp(proxy->key, Node->keys + it) < 0)
		{
			for (size_t j = Node->keys_count-1; j > it; --j)
			{
				set_val(Node->keys + j, Node->keys + j-1);
				set_val(Node->vals + j, Node->vals + j-1);
				Node->pointers[j+1] = Node->pointers[j];
			}

			set_val(Node->keys + it, proxy->key);
			set_val(Node->vals + it, proxy->val);
			Node->pointers[it]   = proxy->left;
			Node->pointers[it+1] = proxy->right;

			return check_node(db, Node, proxy);
		}
	}

	//adding to the end of existing block
	set_val(Node->keys + Node->keys_count-1, proxy->key);
	set_val(Node->vals + Node->keys_count-1, proxy->val);
	Node->pointers[Node->keys_count-1] = proxy->left;
	Node->pointers[Node->keys_count]   = proxy->right;

	return check_node(db, Node, proxy);
}

int close_database(struct DB *db)
{
	close(db->fd);
	free(db);
	return 0;
}

// RETURNS ADDRESS OF THE NEXT BLOCK TO INSERT KEY
size_t find_next(node_header *Node, size_t head, struct DBT *key)
{
	for (size_t it = 0; it < Node->keys_count; ++it)
	{
		if (key_cmp(key, Node->keys + it) == 0)
			return head;

		if (key_cmp(key, Node->keys + it) < 0)
			return Node->pointers[it];
	}

	return Node->pointers[Node->keys_count];
}

// INSERTS KEY : VALUE PAIR TO HEAD
struct Proxy *insert_to_tree(struct DB *db, size_t head, struct Proxy* proxy)
{
	node_header *Node = read_node(db, head);

	size_t next = find_next(Node, head, proxy->key);
	
	if (next == head)
	{
		if ((proxy = refresh_pair(db, Node, proxy)))
		{
			free_node(Node);
			free_block(db, head);
			return proxy;
		}
		else
		{
			write_node(db, Node, head);
			free_node(Node);
			return NULL;
		}
	}

	if (next > 0)
	{
		if ((proxy = insert_to_tree(db, next, proxy)))
			next = 0;
		else
		{
			free_node(Node);
			return NULL;
		}
	}

	if (next == 0)
	{
		if ((proxy = add_pair(db, Node, proxy)))
		{
			free_node(Node);
			free_block(db, head);
			return proxy;
		}
		else
		{
			write_node(db, Node, head);
			free_node(Node);
			return NULL;
		}
	}

	return NULL;
}

struct Proxy *create_proxy(size_t left, size_t right, struct DBT *key, struct DBT *val)
{
	struct Proxy *proxy = (struct Proxy *) malloc(sizeof(struct Proxy));
	proxy->left  = left;
	proxy->right = right;
	proxy->key = (struct DBT *) malloc(sizeof(struct DBT));
	proxy->val = (struct DBT *) malloc(sizeof(struct DBT));
	proxy->key->data = NULL;
	proxy->val->data = NULL;
	set_val(proxy->key, key);
	set_val(proxy->val, val);

	return proxy;
}

int insert_to_db(struct DB *db, struct DBT *key, struct DBT *val)
{
	struct Proxy *proxy = create_proxy(0, 0, key, val);

	proxy = insert_to_tree(db, db->head, proxy);
	if (proxy)
	{
		db->head = alloc(db);
		insert_to_tree(db, db->head, proxy);
	}

	return 0;
}

int find_value(struct DB *db, size_t head, struct DBT *key, struct DBT *val)
{
	node_header *Node = read_node(db, head);

	size_t next = find_next(Node, head, key);

	if (next == 0)
	{
		free_node(Node);
		return -1;
	}

	if (next == head)
	{
		for (size_t it = 0; it < Node->keys_count; ++it)
		{
			if (!key_cmp(Node->keys + it, key))
			{
				set_val(val, Node->vals + it);
				free_node(Node);
				return 0;
			}
		}
	}

	if (next > 0)
	{
		free_node(Node);
		return find_value(db, next, key, val);
	}

	return 0;
}

int select_in_db(struct DB *db, struct DBT *key, struct DBT *val)
{
	return find_value(db, db->head, key, val);
}

int db_close(struct DB *db) {
	return db->close(db);
}

void remove_pair(node_header *Node, struct DBT *key)
{
	for (int i = 0; i < Node->keys_count; ++i)
		if (!key_cmp(key, Node->keys + i))
		{

		}


}

// RETURNS ADDRESS OF THE NEXT BLOCK TO INSERT KEY
size_t find(struct DB *db, size_t head, struct DBT *key)
{
	node_header *Node = read_node(db, head);
	size_t next = find_next(Node, head, key);

	if   (next == 0)    { free_node(Node); return 0; }
	if   (next == head) { free_node(Node); return head; }
	else                { free_node(Node); return find(db, next, key); }
}

int ifleaf(struct DB *db, size_t N)
{
	node_header *Node = read_node(db, N);
	if (Node->keys_count  == 0)   { free_node(Node); return -1; }
	if (Node->pointers[0] == 0)   { free_node(Node); return  1; }
	else                          { free_node(Node); return  Node->pointers[0]; }
}

int delete_from_db(struct DB *db, struct DBT *key)
{
	size_t target = find(db, db->head, key);

	printf("%d\n", ifleaf(db, 270));

	return 0;
}

int db_delete(struct DB *db, void *key, size_t key_len)
{
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->delete(db, &keyt);
}

int db_select(struct DB *db, void *key, size_t key_len,
	   void **val, size_t *val_len)
{
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {0, 0};
	int rc = db->select(db, &keyt, &valt);
	*val = valt.data;
	*val_len = valt.size;
	return rc;
}

int db_insert(struct DB *db, void *key, size_t key_len,
	   void *val, size_t val_len)
{
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {
		.data = val,
		.size = val_len
	};
	return db->insert(db, &keyt, &valt);
}

void tree_show(struct DB *db, size_t head)
{
	if (!head)
		return;

	node_header *Node = read_node(db, head);

	printf("\n------------------------------------------------------------------------------------------------------------------\n");

	printf("[%lu] :   %lu | %lu | ", head, Node->keys_count, Node->free_bytes);

	for (size_t it = 0; it <  Node->keys_count; ++it)
		printf("(%lu) | \"%s\" | \"%s\" | ", Node->pointers[it], Node->keys[it].data, Node->vals[it].data);

	printf("(%lu)", Node->pointers[Node->keys_count]);

	printf("\n------------------------------------------------------------------------------------------------------------------\n");

	for (size_t it = 0; it <= Node->keys_count; ++it)
		tree_show(db, Node->pointers[it]);

	free_node(Node);
}

void read_tree(struct DB *db, const char *filename, const struct DBC *conf)
{
	char key[conf->page_size],
		 val[conf->page_size];

	FILE *input = fopen(filename, "r");
	while (fscanf(input, "%s %s\n", key, val) > 1)
		db_insert(db, key, strlen(key)+1, val, strlen(val)+1);

	fclose(input);
}

struct DB *dbopen(const char *file, const struct DBC *conf){
	struct DB* mydb = malloc(sizeof(struct DB));

	mydb->fd = open(file, O_CREAT | O_EXCL | O_RDWR, 0666);

	mydb->close = &close_database;
	mydb->insert = &insert_to_db;
	mydb->delete = &delete_from_db;
	mydb->select = &select_in_db;

	mydb->size = conf->db_size;
	mydb->page_size = conf->page_size;
	mydb->page_count = conf->db_size / conf->page_size;

	if (mydb->fd == -1)
	{
		mydb->fd = open(file, O_RDWR, 0666);
		mydb->head = conf->db_size / conf->page_size / conf->page_size;
    	return mydb;
	}

	size_t block_count = conf->db_size / conf->page_size,
		   table_count = block_count   / conf->page_size,
		   free_bytes  = conf->page_size - 2*SIZE;

	char *blank_block = calloc(conf->page_size, 1);
	memcpy(blank_block + SIZE, &free_bytes, SIZE);
	for (int it = 0; it < block_count; ++it)
		write(mydb->fd, blank_block, conf->page_size);

	char *table = calloc(table_count * conf->page_size, 1);
	memset(table, 1, table_count);

	lseek(mydb->fd, 0, SEEK_SET);
	write(mydb->fd, table, table_count * conf->page_size);

	free(blank_block);
	free(table);

	mydb->head = alloc(mydb);
    return mydb;
}

int main(int argc, char **argv){
	struct DBC conf = {16*1024*1024, 256, 0};

	struct DB *db = dbopen("database", &conf);
	read_tree(db, "input.txt", &conf);
	tree_show(db, db->head);

	//db_delete(db, "key03", 6);
    return 0;
}
