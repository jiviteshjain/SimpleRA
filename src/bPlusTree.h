#ifndef __BPLUSTREE_H
#define __BPLUSTREE_H

#include "logger.h"

// Default order is 4.
#define DEFAULT_ORDER 4

// Minimum order is necessarily 3.  We set the maximum
// order arbitrarily.  You may change the maximum order.
#define MIN_ORDER 3
#define MAX_ORDER 400

// Constant for optional command-line input with "i" command.
#define BUFFER_SIZE 256

// TYPES.

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or nullptr in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */
class Node {
   private:
    vector<void*> pointers;
    vector<int> keys;
    Node* parent;
    bool is_leaf;
    int num_keys;
    Node* next;  // Used for queue.

    friend class BPlusTree;

   public:
    Node(bool is_leaf = false, int order = 0, int reserve = 0) {
        if (reserve > 0 && reserve > order) {
            this->pointers.reserve(reserve);
            this->keys.reserve(reserve - 1);
        }
        if (order > 0) {
            this->pointers.resize(order);
            this->keys.resize(order - 1);
        }

        this->is_leaf = is_leaf;

        this->num_keys = 0;
        this->parent = nullptr;
        this->next = nullptr;
    }
};
/* Internal nodes only store node pointers.
 * Leaves store record pointers mostly, and pointer
 * to the next leaf (node pointer) as the last element.
 */

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */
class Record {
   private:
    int value;
    friend class Node;
    friend class BPlusTree;

   public:
    Record() : value(0){};
    Record(int value) : value(value){};
};

class BPlusTree {
   private:
    /* The root node of the tree, which starts as a
     * null pointer.
     */
    Node* root;

    /* The order determines the maximum and minimum
    * number of entries (keys and pointers) in any
    * node.  Every node has at most order - 1 keys and
    * at least (roughly speaking) half that number.
    * Every leaf has as many pointers to data as keys,
    * and every internal node has one more pointer
    * to a subtree than the number of keys.
    * This global variable is initialized to the
    * default value.
    */
    int order;

    /* The queue is used to print the tree in
    * level order, starting from the root
    * printing each entire rank on a separate
    * line, finishing with the leaves.
    */
    Node* que = nullptr;

    /* The user can toggle on and off the "verbose"
    * property, which causes the pointer addresses
    * to be printed out in hexadecimal notation
    * next to their corresponding keys.
    */
    bool verbose = false;

    /* The amount of space to reserve in each node,
     * to avoid reallocations. Has no relation to the
     * order of the tree.
     */
    int reserve = 0;

    // OUTPUT AND UTILITY
    void enqueue(Node* new_node);
    Node* dequeue();
    static int height(Node* n);
    int path_to_root(Node* child);
    Node* find_leaf(int key, bool full_path = false);
    static int cut(int length);

    // INSERTION
    int get_left_index(Node* parent, Node* left);
    Node* insert_into_leaf(Node* leaf, int key, Record* pointer);
    Node* insert_into_leaf_after_splitting(Node* leaf, int key,
                                           Record* pointer);
    Node* insert_into_node(Node* parent,
                           int left_index, int key, Node* right);
    Node* insert_into_node_after_splitting(Node* parent,
                                           int left_index,
                                           int key, Node* right);
    Node* insert_into_parent(Node* left, int key, Node* right);
    Node* insert_into_new_root(Node* left, int key, Node* right);
    Node* start_new_tree(int key, Record* pointer);
    // Record* make_record(int value);
    // Node* make_node(void);
    // Node* make_leaf(void);

    // DELETION
    int get_neighbor_index(Node* n);
    Node* remove_entry_from_node(Node* n, int key, void* pointer);  // CARE: it was node* pointer earlier
    Node* adjust_root();
    Node* coalesce_nodes(Node* n, Node* neighbor,
                         int neighbor_index, int k_prime);
    Node* redistribute_nodes(Node* n, Node* neighbor,
                             int neighbor_index,
                             int k_prime_index, int k_prime);
    Node* delete_entry(Node* n, int key, void* pointer);
    void destroy_tree_nodes(Node* n);

   public:
    // OUTPUT AND UTILITY
    void print_leaves();
    void print_tree();
    void find_and_print(int key, bool full_path = false);
    void find_and_print_range(int range1, int range2, bool full_path = false);
    int find_range(int key_start, int key_end,
                   vector<int>& returned_keys, vector<void*>& returned_pointers,
                   bool full_path = false);
    Record* find(int key, Node** leaf_out, bool full_path = false);
    void set_verbose(bool verbose) {
        this->verbose = verbose;
    }
    void set_verbose() {
        this->verbose = !this->verbose;
    }

    // INSERTION
    void insert(int key, int value);

    // DELETION
    void del(int key);

    /* Different from the destructor in the sense that it
     * destroys the data in the tree but not the tree object
     * itself.
     */
    void destroy_tree();

    // CONSTRUCTORS AND DESTRUCTORS
    BPlusTree(int order = DEFAULT_ORDER, bool verbose = false) {
        this->root = nullptr;
        this->que = nullptr;

        this->order = order;
        this->verbose = verbose;
    }

    ~BPlusTree() {
        this->destroy_tree();
    }

    void reset(int order = DEFAULT_ORDER, int reserve = 0, bool verbose = false) {
        this->destroy_tree();
        this->order = order;
        this->verbose = verbose;
        this->reserve = reserve;
    }
};

#endif
