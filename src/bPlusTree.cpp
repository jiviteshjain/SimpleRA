#include "bPlusTree.h"

// OUTPUT AND UTILITIES

/* Helper function for printing the
 * tree out.  See print_tree.
 */
void BPlusTree::enqueue(Node *new_node) {
    Node *c;
    if (this->que == nullptr) {
        this->que = new_node;
        this->que->next = nullptr;
    } else {
        c = this->que;
        while (c->next != nullptr) {
            c = c->next;
        }
        c->next = new_node;
        new_node->next = nullptr;
    }
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
Node *BPlusTree::dequeue(void) {
    Node *n = this->que;
    this->que = this->que->next;
    n->next = nullptr;
    return n;
}

/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void BPlusTree::print_leaves() {  // HERE
    if (this->root == nullptr) {
        printf("Empty tree.\n");
        return;
    }
    int i;
    Node *c = this->root;
    while (!c->is_leaf)
        c = (Node *)c->pointers[0];  // not a leaf, so these are node pointers
    while (true) {
        for (i = 0; i < c->num_keys; i++) {
            if (this->verbose)
                printf("%p ", c->pointers[i]);  // record pointers
            printf("%d ", c->keys[i]);
        }
        if (this->verbose)
            printf("%p ", c->pointers.back());  // node (leaf) pointer
        if (c->pointers.back() != nullptr) {
            printf(" | ");
            c = (Node *)c->pointers.back();  // node (leaf) pointer
        } else
            break;
    }
    printf("\n");
}

/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 * 
 * This is generic, can be used to find the height
 * of any other node as well.
 */
int BPlusTree::height(Node *n) {
    int h = 0;
    // Node* c = n; // CARE: No point in copying, else was using c
    while (!n->is_leaf) {
        n = (Node *)n->pointers[0];  // not a leaf, so these are node pointers
        h++;
    }
    return h;
}

/* Utility function to give the length in edges
 * of the path from any node to the root.
 */
int BPlusTree::path_to_root(Node *child) {
    int length = 0;
    Node *c = child;
    while (c != this->root) {
        c = c->parent;
        length++;
    }
    return length;
}

/* Prints the B+ tree in the command
 * line in level (rank) order, with the 
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */
void BPlusTree::print_tree() {
    Node *n = nullptr;
    int i = 0;
    int rank = 0;
    int new_rank = 0;

    if (this->root == nullptr) {
        printf("Empty tree.\n");
        return;
    }
    this->que = nullptr;
    enqueue(this->root);
    while (this->que != nullptr) {
        n = dequeue();
        if (n->parent != nullptr && n == n->parent->pointers[0]) {
            new_rank = path_to_root(n);
            if (new_rank != rank) {
                rank = new_rank;
                printf("\n");
            }
        }
        if (this->verbose)
            printf("(%p)", n);
        for (i = 0; i < n->num_keys; i++) {
            if (this->verbose)
                printf("%p ", n->pointers[i]);
            printf("%d ", n->keys[i]);
        }
        if (!n->is_leaf)
            for (i = 0; i <= n->num_keys; i++)
                enqueue((Node *)(n->pointers[i]));  // not a leaf, so these are node pointers
        if (this->verbose) {
            if (n->is_leaf)
                printf("%p ", n->pointers.back());
            else
                printf("%p ", n->pointers.back());
        }
        printf("| ");
    }
    printf("\n");
}

/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void BPlusTree::find_and_print(int key, bool full_path) {
    Node *leaf = nullptr;
    Record *r = find(key, nullptr, full_path);
    if (r == nullptr)
        printf("Record not found under key %d.\n", key);
    else
        printf("Record at %p -- key %d, value %d.\n",
               r, key, r->value);
}

/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
void BPlusTree::find_and_print_range(int key_start, int key_end, bool full_path) {
    int i;
    int array_size = key_end - key_start + 1;
    vector<int> returned_keys(array_size);
    vector<void *> returned_pointers(array_size);
    int num_found = find_range(key_start, key_end,
                               returned_keys, returned_pointers, full_path);
    if (!num_found)
        printf("None found.\n");
    else {
        for (i = 0; i < num_found; i++)
            printf("Key: %d   Location: %p  Value: %d\n",
                   returned_keys[i],
                   returned_pointers[i],
                   ((Record *)
                        returned_pointers[i])
                       ->value);
    }
}

/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
int BPlusTree::find_range(int key_start, int key_end,
                          vector<int> &returned_keys, vector<void *> &returned_pointers, bool full_path) {
    int i, num_found;
    num_found = 0;
    Node *n = find_leaf(key_start, full_path);
    if (n == nullptr) return 0;
    for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++)
        ;
    if (i == n->num_keys) return 0;
    while (n != nullptr) {
        for (; i < n->num_keys && n->keys[i] <= key_end; i++) {
            returned_keys[num_found] = n->keys[i];
            returned_pointers[num_found] = n->pointers[i];
            num_found++;
        }
        n = (Node *)n->pointers.back();  // node (leaf) pointer
        i = 0;
    }
    return num_found;
}

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
Node *BPlusTree::find_leaf(int key, bool full_path) {
    if (this->root == nullptr) {
        if (full_path)
            printf("Empty tree.\n");
        return this->root;
    }
    int i = 0;
    Node *c = this->root;
    while (!c->is_leaf) {
        if (full_path) {
            printf("[");
            for (i = 0; i < c->num_keys - 1; i++)
                printf("%d ", c->keys[i]);
            printf("%d] ", c->keys[i]);
        }
        i = 0;
        while (i < c->num_keys) {
            if (key >= c->keys[i])
                i++;
            else
                break;
        }
        if (full_path)
            printf("%d ->\n", i);
        c = (Node *)c->pointers[i];
    }
    if (full_path) {
        printf("Leaf [");
        for (i = 0; i < c->num_keys - 1; i++)
            printf("%d ", c->keys[i]);
        printf("%d] ->\n", c->keys[i]);
    }
    return c;
}

/* Finds and returns the record to which
 * a key refers.
 */
Record *BPlusTree::find(int key, Node **leaf_out, bool full_path) {
    if (this->root == nullptr) {
        if (leaf_out != nullptr) {
            *leaf_out = nullptr;
        }
        return nullptr;
    }

    int i = 0;
    Node *leaf = nullptr;

    leaf = find_leaf(key, full_path);

    /* If root != nullptr, leaf must have a value, even
     * if it does not contain the desired key.
     * (The leaf holds the range of keys that would
     * include the desired key.) 
     */

    for (i = 0; i < leaf->num_keys; i++)
        if (leaf->keys[i] == key) break;
    if (leaf_out != nullptr) {
        *leaf_out = leaf;
    }
    if (i == leaf->num_keys)
        return nullptr;
    else
        return (Record *)leaf->pointers[i];
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int BPlusTree::cut(int length) {
    if (length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}

// INSERTION

/* Creates a new record to hold the value
 * to which a key refers.
 */
// Record* make_record(int value) {
// 	Record * new_record = (Record*)malloc(sizeof(Record));
// 	if (new_record == nullptr) {
// 		perror("Record creation.");
// 		exit(EXIT_FAILURE);
// 	}
// 	else {
// 		new_record->value = value;
// 	}
// 	return new_record;
// }

/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
// Node * make_node(void) {
// 	Node * new_node;
// 	new_node = (Node*)malloc(sizeof(Node));
// 	if (new_node == nullptr) {
// 		perror("Node creation.");
// 		exit(EXIT_FAILURE);
// 	}

// 	new_node->keys.resize(order - 1);
// 	new_node->pointers.resize(order);

// 	new_node->is_leaf = false;
// 	new_node->num_keys = 0;
// 	new_node->parent = nullptr;
// 	new_node->next = nullptr;
// 	return new_node;
// }

/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
// Node * make_leaf(void) {
// 	Node * leaf = make_node();
// 	leaf->is_leaf = true;
// 	return leaf;
// }

/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
int BPlusTree::get_left_index(Node *parent, Node *left) {
    int left_index = 0;
    while (left_index <= parent->num_keys &&
           parent->pointers[left_index] != left)
        left_index++;
    return left_index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
Node *BPlusTree::insert_into_leaf(Node *leaf, int key, Record *pointer) {
    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
        insertion_point++;

    leaf->keys.push_back(0);
    void *temp = leaf->pointers.back(); // always exists because pointers array is built with this
    leaf->pointers.push_back(temp);

    for (i = leaf->num_keys; i > insertion_point; i--) {
        leaf->keys[i] = leaf->keys[i - 1];
        leaf->pointers[i] = leaf->pointers[i - 1];
    }
    leaf->keys[insertion_point] = key;
    leaf->pointers[insertion_point] = pointer;
    leaf->num_keys++;
    return leaf;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
Node *BPlusTree::insert_into_leaf_after_splitting(Node *leaf, int key, Record *pointer) {
    Node *new_leaf;
    vector<int> temp_keys(leaf->num_keys + 1);
    vector<void *> temp_pointers(leaf->num_keys + 1); // no space for next pointer
    int insertion_index, split, new_key, i, j;


    insertion_index = 0;
    while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
        insertion_index++;

    for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = leaf->keys[i];
        temp_pointers[j] = leaf->pointers[i];
    }

    temp_keys[insertion_index] = key;
    temp_pointers[insertion_index] = pointer;

    void *temp = leaf->pointers.back(); // copy next leaf pointer

    split = cut(this->order - 1);
    
    leaf->num_keys = 0;
    
    leaf->keys.resize(split);
    leaf->pointers.resize(split + 1); // space for next leaf pointer


    for (i = 0; i < split; i++) {
        leaf->pointers[i] = temp_pointers[i];
        leaf->keys[i] = temp_keys[i];
        leaf->num_keys++;
    }

    new_leaf = new Node(true, order - split + 1, this->reserve); // split < order guaranteed. Space for next leaf pointer

    for (i = split, j = 0; i < order; i++, j++) {
        new_leaf->pointers[j] = temp_pointers[i];
        new_leaf->keys[j] = temp_keys[i];
        new_leaf->num_keys++;
    }

    new_leaf->pointers.back() = temp;
    leaf->pointers.back() = new_leaf;

    new_leaf->parent = leaf->parent;
    new_key = new_leaf->keys[0];

    return insert_into_parent(leaf, new_key, new_leaf);
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
Node *BPlusTree::insert_into_node(Node *n,
                                  int left_index, int key, Node *right) {
    n->keys.push_back(0);
    n->pointers.push_back(nullptr);
    
    int i;

    for (i = n->num_keys; i > left_index; i--) {
        n->pointers[i + 1] = n->pointers[i];
        n->keys[i] = n->keys[i - 1];
    }
    n->pointers[left_index + 1] = right;
    n->keys[left_index] = key;
    n->num_keys++;
    return this->root;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
Node *BPlusTree::insert_into_node_after_splitting(Node *old_node, int left_index,
                                                  int key, Node *right) {
    int i, j, split, k_prime;
    Node *new_node, *child;
    vector<int> temp_keys(old_node->num_keys + 1);
    vector<void *> temp_pointers(old_node->num_keys + 2);
    // TODO: According to policy, this should be void*, because the pointer is not
    // being dereferenced.

    /* First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */

    for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
        if (j == left_index + 1) j++;
        temp_pointers[j] = old_node->pointers[i];  // not a leaf, so these are node pointers
    }

    for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = old_node->keys[i];
    }

    temp_pointers[left_index + 1] = (void *)right;
    temp_keys[left_index] = key;

    /* Create the new node and copy
	 * half the keys and pointers to the
	 * old and half to the new.
	 */
    split = cut(order);
    old_node->num_keys = 0;
    old_node->keys.resize(split - 1);
    old_node->pointers.resize(split);

    for (i = 0; i < split - 1; i++) {
        old_node->pointers[i] = temp_pointers[i];
        old_node->keys[i] = temp_keys[i];
        old_node->num_keys++;
    }
    old_node->pointers[i] = temp_pointers[i];
    k_prime = temp_keys[split - 1];

    new_node = new Node(false, order - split + 1, this->reserve);
    for (++i, j = 0; i < order; i++, j++) {
        new_node->pointers[j] = temp_pointers[i];
        new_node->keys[j] = temp_keys[i];
        new_node->num_keys++;
    }
    new_node->pointers[j] = temp_pointers[i];
    new_node->parent = old_node->parent;
    for (i = 0; i <= new_node->num_keys; i++) {
        child = (Node *)new_node->pointers[i];  // not a leaf, so these are node pointers
        child->parent = new_node;
    }

    /* Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old node to the left and the new to the right.
	 */

    return insert_into_parent(old_node, k_prime, new_node);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
Node *BPlusTree::insert_into_parent(Node *left, int key, Node *right) {
    int left_index;
    Node *parent;

    parent = left->parent;

    /* Case: new root. */

    if (parent == nullptr)
        return insert_into_new_root(left, key, right);

    /* Case: leaf or node. (Remainder of
	 * function body.)  
	 */

    /* Find the parent's pointer to the left 
	 * node.
	 */

    left_index = get_left_index(parent, left);

    /* Simple case: the new key fits into the node. 
	 */

    if (parent->num_keys < order - 1)
        return insert_into_node(parent, left_index, key, right);

    /* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */

    return insert_into_node_after_splitting(parent, left_index, key, right);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
Node *BPlusTree::insert_into_new_root(Node *left, int key, Node *right) {
    Node *root = new Node(false, 2, this->reserve);
    root->keys[0] = key;
    root->pointers[0] = left;
    root->pointers[1] = right;
    root->num_keys++;
    root->parent = nullptr;
    left->parent = root;
    right->parent = root;
    return root;
}

/* First insertion:
 * start a new tree.
 */
Node *BPlusTree::start_new_tree(int key, Record *pointer) {
    Node *root = new Node(true, 2, this->reserve);  // new never returns nullptr
    root->keys[0] = key;
    root->pointers[0] = pointer;
    root->pointers[1] = nullptr;
    root->parent = nullptr;
    root->num_keys++;
    return root;
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
void BPlusTree::insert(int key, int value) {
    Record *record_pointer = nullptr;
    Node *leaf = nullptr;

    /* The current implementation ignores
	 * duplicates.
	 */

    record_pointer = find(key, nullptr);
    if (record_pointer != nullptr) {
        /* If the key already exists in this tree, update
         * the value and return the tree.
         */

        record_pointer->value = value;
        return;  // No root update needed
    }

    /* Create a new record for the
	 * value.
	 */
    record_pointer = new Record(value);

    /* Case: the tree does not exist yet.
	 * Start a new tree.
	 */

    if (this->root == nullptr) {
        this->root = start_new_tree(key, record_pointer);
        return;
    }

    /* Case: the tree already exists.
	 * (Rest of function body.)
	 */

    leaf = find_leaf(key);

    /* Case: leaf has room for key and record_pointer.
	 */

    if (leaf->num_keys < order - 1) {
        leaf = insert_into_leaf(leaf, key, record_pointer);
        return;  // No root update needed
    }

    /* Case:  leaf must be split.
	 */

    this->root = insert_into_leaf_after_splitting(leaf, key, record_pointer);
    return;
}

// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int BPlusTree::get_neighbor_index(Node *n) {
    int i;

    /* Return the index of the key to the left
	 * of the pointer in the parent pointing
	 * to n.  
	 * If n is the leftmost child, this means
	 * return -1.
	 */
    for (i = 0; i <= n->parent->num_keys; i++)
        if (n->parent->pointers[i] == n)
            return i - 1;

    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}

Node *BPlusTree::remove_entry_from_node(Node *n, int key, void *pointer) {  // CARE: it was node* pointer earlier

    int i, num_pointers;

    // Remove the key and shift other keys accordingly.
    i = 0;
    while (n->keys[i] != key)
        i++;
    for (++i; i < n->num_keys; i++)
        n->keys[i - 1] = n->keys[i];

    // Remove the pointer and shift other pointers accordingly.
    // A leaf uses the last pointer to point to the next leaf,
    // and hence has the same number of pointers as any internal
    // node.
    num_pointers = n->num_keys + 1;
    i = 0;
    while (n->pointers[i] != pointer)
        i++;
    for (++i; i < num_pointers; i++)
        n->pointers[i - 1] = n->pointers[i];

    // One key fewer.
    n->num_keys--;
    n->keys.pop_back();
    n->pointers.pop_back();

    return n;
}

Node *BPlusTree::adjust_root() {
    Node *new_root;

    /* Case: nonempty root.
	 * Key and pointer have already been deleted,
	 * so nothing to be done.
	 */

    if (this->root->num_keys > 0)
        return this->root;

    /* Case: empty root. 
	 */

    // If it has a child, promote
    // the first (only) child
    // as the new root.

    if (!this->root->is_leaf) {
        new_root = (Node *)this->root->pointers[0];  // not a leaf, so these are node pointers
        new_root->parent = nullptr;
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else
        new_root = nullptr;

    delete (Node *)this->root;

    return new_root;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
Node *BPlusTree::coalesce_nodes(Node *n, Node *neighbor, int neighbor_index, int k_prime) {
    int i, j;
    Node *tmp;

    /* Swap neighbor with node if node is on the
	 * extreme left and neighbor is to its right.
	 */

    if (neighbor_index == -1) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }

    /* Case:  nonleaf node.
	 * Append k_prime and the following pointer.
	 * Append all pointers and keys from the neighbor.
	 */

    if (!n->is_leaf) {
        /* Append k_prime.
		 */

        neighbor->keys.push_back(k_prime);
        neighbor->num_keys++;

        for (j = 0; j < n->num_keys; j++) {
            neighbor->keys.push_back(n->keys[j]);
            neighbor->pointers.push_back(n->pointers[j]);
            neighbor->num_keys++;
        }

        /* The number of pointers is always
		 * one more than the number of keys.
		 */

        neighbor->pointers.push_back(n->pointers[j]);

        /* All children must now point up to the same parent.
		 */

        for (i = 0; i < neighbor->num_keys + 1; i++) {
            tmp = (Node *)neighbor->pointers[i];
            tmp->parent = neighbor;
        }
    }

    /* In a leaf, append the keys and pointers of
	 * n to the neighbor.
	 * Set the neighbor's last pointer to point to
	 * what had been n's right neighbor.
	 */

    else {
        neighbor->pointers.pop_back();
        for (j = 0; j < n->num_keys; j++) {
            neighbor->keys.push_back(n->keys[j]);
            neighbor->pointers.push_back(n->pointers[j]);
            neighbor->num_keys++;
        }
        neighbor->pointers.push_back(n->pointers.back());
    }

    n->num_keys = 0;
    n->keys.resize(n->num_keys);
    n->pointers.resize(n->num_keys + 1);

    this->root = delete_entry(n->parent, k_prime, n);
    delete (Node *)n;
    return this->root;
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
Node *BPlusTree::redistribute_nodes(Node *n, Node *neighbor, int neighbor_index,
                                    int k_prime_index, int k_prime) {
    int i;
    Node *tmp;

    /* Case: n has a neighbor to the left. 
	 * Pull the neighbor's last key-pointer pair over
	 * from the neighbor's right end to n's left end.
	 */

    // if (neighbor_index != -1) {
    //     if (!n->is_leaf)
    //         n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
    //     for (i = n->num_keys; i > 0; i--) {
    //         n->keys[i] = n->keys[i - 1];
    //         n->pointers[i] = n->pointers[i - 1];
    //     }
    //     if (!n->is_leaf) {
    //         n->pointers[0] = neighbor->pointers[neighbor->num_keys];
    //         tmp = (Node *)n->pointers[0];
    //         tmp->parent = n;
    //         neighbor->pointers[neighbor->num_keys] = nullptr;
    //         n->keys[0] = k_prime;
    //         n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
    //     } else {
    //         n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
    //         neighbor->pointers[neighbor->num_keys - 1] = nullptr;
    //         n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
    //         n->parent->keys[k_prime_index] = n->keys[0];
    //     }
    // }

    if (neighbor_index != -1) {
        
        n->keys.push_back(0);
        void *temp = n->pointers.back(); // nodes are never completely empty
        n->pointers.push_back(temp);
        for (i = n->num_keys; i > 0; i--) {
            n->keys[i] = n->keys[i - 1];
            n->pointers[i] = n->pointers[i - 1];
        }

        if (!n->is_leaf) {
            n->pointers[0] = neighbor->pointers[neighbor->num_keys];
            tmp = (Node *)n->pointers[0];
            tmp->parent = n;
            neighbor->pointers.pop_back();

            n->keys[0] = k_prime;
            n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
            neighbor->keys.pop_back();


        } else {
            n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
            n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
            n->parent->keys[k_prime_index] = n->keys[0];

            neighbor->keys.pop_back();
            void *temp = neighbor->pointers.back();
            neighbor->pointers.pop_back();
            neighbor->pointers.back() = temp;
        }
    }

    /* Case: n is the leftmost child.
	 * Take a key-pointer pair from the neighbor to the right.
	 * Move the neighbor's leftmost key-pointer pair
	 * to n's rightmost position.
	 */

    // else {
    //     if (n->is_leaf) {
    //         n->keys[n->num_keys] = neighbor->keys[0];
    //         n->pointers[n->num_keys] = neighbor->pointers[0];
    //         n->parent->keys[k_prime_index] = neighbor->keys[1];
    //     } else {
    //         n->keys[n->num_keys] = k_prime;
    //         n->pointers[n->num_keys + 1] = neighbor->pointers[0];
    //         tmp = (Node *)n->pointers[n->num_keys + 1];
    //         tmp->parent = n;
    //         n->parent->keys[k_prime_index] = neighbor->keys[0];
    //     }
    //     for (i = 0; i < neighbor->num_keys - 1; i++) {
    //         neighbor->keys[i] = neighbor->keys[i + 1];
    //         neighbor->pointers[i] = neighbor->pointers[i + 1];
    //     }
    //     if (!n->is_leaf)
    //         neighbor->pointers[i] = neighbor->pointers[i + 1];
    // }

    else {

        if (!n->is_leaf) {
            n->keys.push_back(k_prime);
            n->pointers.push_back(neighbor->pointers[0]);
            tmp = (Node *)n->pointers.back();
            tmp->parent = n;
            n->parent->keys[k_prime_index] = neighbor->keys[0];

        } else {
            n->keys.push_back(neighbor->keys[0]);
            void *temp = n->pointers.back();
            n->pointers.push_back(temp);
            n->pointers[n->num_keys] = neighbor->pointers[0];
            n->parent->keys[k_prime_index] = neighbor->keys[1];

        }
        for (i = 0; i < neighbor->num_keys - 1; i++) {
            neighbor->keys[i] = neighbor->keys[i + 1];
            neighbor->pointers[i] = neighbor->pointers[i + 1];
        }
        neighbor->pointers[i] = neighbor->pointers[i + 1];

        neighbor->keys.pop_back();
        neighbor->pointers.pop_back();
    }

    /* n now has one more key and one more pointer;
	 * the neighbor has one fewer of each.
	 */

    n->num_keys++;
    neighbor->num_keys--;

    return this->root;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
Node *BPlusTree::delete_entry(Node *n, int key, void *pointer) {
    int min_keys;
    Node *neighbor;
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;

    // Remove key and pointer from node.

    n = remove_entry_from_node(n, key, pointer);

    /* Case:  deletion from the root. 
	 */

    if (n == this->root)
        return adjust_root();

    /* Case:  deletion from a node below the root.
	 * (Rest of function body.)
	 */

    /* Determine minimum allowable size of node,
	 * to be preserved after deletion.
	 */

    min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

    /* Case:  node stays at or above minimum.
	 * (The simple case.)
	 */

    if (n->num_keys >= min_keys)
        return this->root;

    /* Case:  node falls below minimum.
	 * Either coalescence or redistribution
	 * is needed.
	 */

    /* Find the appropriate neighbor node with which
	 * to coalesce.
	 * Also find the key (k_prime) in the parent
	 * between the pointer to node n and the pointer
	 * to the neighbor.
	 */

    neighbor_index = get_neighbor_index(n);
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = n->parent->keys[k_prime_index];
    neighbor = (neighbor_index == -1 ? (Node *)(n->parent->pointers[1]) : (Node *)(n->parent->pointers[neighbor_index]));  // not a leaf, so these are node pointers

    capacity = n->is_leaf ? order : order - 1;

    /* Coalescence. */

    if (neighbor->num_keys + n->num_keys < capacity)
        return coalesce_nodes(n, neighbor, neighbor_index, k_prime);

    /* Redistribution. */

    else
        return redistribute_nodes(n, neighbor, neighbor_index, k_prime_index, k_prime);
}

/* Master deletion function.
 */
void BPlusTree::del(int key) {
    Node *key_leaf = nullptr;
    Record *key_record = nullptr;

    key_record = find(key, &key_leaf);

    /* CHANGE */

    if (key_record != nullptr && key_leaf != nullptr) {
        this->root = delete_entry(key_leaf, key, key_record);
        delete (Record *)key_record;
    }
    // Root update necessary only if modified
}

void BPlusTree::destroy_tree_nodes(Node *n) {
    if (n == nullptr) {
        return;
    }
    int i;
    if (n->is_leaf)
        for (i = 0; i < n->num_keys; i++)
            delete (Record *)(n->pointers[i]);  // CARE: because < num_keys, these are all record pointers and not next leaf pointer
    else
        for (i = 0; i < n->num_keys + 1; i++)
            destroy_tree_nodes((Node *)(n->pointers[i]));  // not a leaf, so these are node pointers
    delete (Node *)n;
}

void BPlusTree::destroy_tree() {
    destroy_tree_nodes(this->root);
    this->root = nullptr;
}