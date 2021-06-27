/*
 * src/tutorial/intset.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */

#include <regex.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <stdbool.h>

PG_MODULE_MAGIC;

struct intSet
{
	uint32 size;
	uint32 nums[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct intSet intSet;

// helper struct
struct treeNode {
    uint32_t data;
    struct treeNode *left;
    struct treeNode *right;
};
typedef struct treeNode *TreeNode;

/*
    ---------------- Helper Function Interfaces ----------------
*/
/*
    ---------------- Tree operations ----------------
*/
TreeNode newNode(uint32_t n);
TreeNode insertNode(uint32_t n, TreeNode root);
uint32_t treeSize(TreeNode root);
void destroyTree(TreeNode root);
bool treeEqual(TreeNode a, TreeNode b);
TreeNode left_rotate(TreeNode root);
TreeNode right_rotate(TreeNode root);
uint32_t treeHeight(TreeNode root);

/*
    ---------------- Other operations ----------------
*/
int treeToArr(TreeNode root, uint32_t arr[], int i);
bool numsEqual(uint32 *a, uint32 *b, uint32 size);
bool binarySearch(uint32* n, uint32 low, uint32 high, uint32 target);
/*
    ---------------- End of Helper Function Interfaces ----------------
*/



/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(intset_in);

Datum
intset_in(PG_FUNCTION_ARGS)
{
	// declare everthing on top to make gcc happy
	char *str = PG_GETARG_CSTRING(0);
	TreeNode tempNums = NULL;
	uint32_t curr_num = 0;
	bool flag = false;
	uint32_t size;
	intSet *result;
	uint32 *res_nums;

	// create the regex pattern for input string
	regex_t regex;
	int compile_fail, match_fail;
	// compile the regex
	compile_fail = regcomp(&regex, "^ *\\{ *(( *[0-9]+ *, *)* *[0-9]+ *)? *\\} *$", REG_EXTENDED);
	// regcomp returns 0 if compilation fails
	if (compile_fail) {
		fprintf(stderr, "Cannot compile regex\n");
		exit(1);
	}
	// match the input string with the regex
	match_fail = regexec(&regex, str, 0, NULL, 0);
	// regexec returns 0 if matching fails
	if (match_fail)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			errmsg("invalid input syntax for type %s: \"%s\"",
					"intset", str)));

	// free the regex
	regfree(&regex);


	// start building result struct
	// scan the string and make the nums tree
	for (size_t i = 0; i < strlen(str); i++) {
		// if we hit a number
		if (str[i] <= '9' && str[i] >= '0') {
			curr_num *= 10;
			curr_num += str[i] - '0';
			flag = true;
			// elog(NOTICE, "we hit 1st case, str[%lu] = %c, curr_num = %u\n", i, str[i], curr_num);
		} else if (flag) {
			// elog(NOTICE, "we hit 2nd case, str[%lu] = %c, curr_num = %u\n", i, str[i], curr_num);
			tempNums = insertNode(curr_num, tempNums);
			curr_num = 0;
			flag = false;
		}
	}
	
	size = treeSize(tempNums);
	// elog(NOTICE, "tree size: %u\n", size);
	// allocate space for result struct
	// each element in array takes 4 bytes (uint32) times the number of elements
	result = (intSet *) palloc(VARHDRSZ + size * 4);
	SET_VARSIZE(result, VARHDRSZ + size * 4);
	// result->size = size;
	
	// make a temp array
	// fill in the numbers into the array
	res_nums = (uint32 *) VARDATA_ANY(result);
	treeToArr(tempNums, res_nums, 0);

	// free up the space
	destroyTree(tempNums);
	PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(intset_out);

Datum
intset_out(PG_FUNCTION_ARGS)
{
	// declare everthing on top to make gcc happy
	intSet *intset = (intSet *) PG_GETARG_POINTER(0);
	char *result, *temp;
	uint32_t curr_len, res_len, sofar = 1;
	uint32 *nums = (uint32 *) VARDATA_ANY(intset);
	uint32 intset_size = VARSIZE_ANY_EXHDR(intset) / 4;

	// elog(NOTICE, "----------intset_out--------\n");
	// elog(NOTICE, "intset size: %u\n", intset_size);
	// if we have an empty set
	if (intset_size == 0) {
		result = (char *) palloc(3 * sizeof(char));
		result[0] = '{';
		result[1] = '}';
		result[2] = '\0';
		PG_RETURN_CSTRING(result);
	}

	// calculate the length of the string first
	res_len = 0;
	for (uint32_t i = 0; i < intset_size; i++) {
		// if nums[i] is 0, log10(0) is illegal
		if (nums[i]) res_len += (uint32_t)log10(nums[i]) + 1;
		else res_len += 1;
	}
		
	// since we also have 2 spaces for brackets and (size - 1) for comma and 1 for '\0'
	res_len += intset_size + 2;
	// elog(NOTICE, "res_len: %u\n", res_len);
	result = (char *) palloc(res_len * sizeof(char));
	
	// make the intset into a string
	result[0] = '{';
	for (uint32_t i = 0; i < intset_size; i++) {
		temp = psprintf("%u,", nums[i]);
		// if nums[i] is zero, log10 will be illigal
		if (nums[i]) curr_len = (uint32_t) log10(nums[i]) + 2;
		else curr_len = 2 ;
		memcpy((void *)(result + sofar), (void *) temp, curr_len);
		sofar += curr_len;
		// elog(NOTICE, "temp: %s\nresult:%s\n", temp, result);
	}
	
	result[res_len - 2] = '}';
	result[res_len - 1] = '\0';
	// elog(NOTICE, "result: %s\n", result);
	
	PG_RETURN_CSTRING(result);
}


/*****************************************************************************
 * New Operators
 *
 * A practical intSet datatype would provide much more than this, of course.
 *****************************************************************************/

PG_FUNCTION_INFO_V1(intset_union);

Datum
intset_union(PG_FUNCTION_ARGS)
{
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	intSet *result;
	TreeNode u_tree = NULL;
	uint32_t asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b) / 4, u_size = 0;
	uint32 *res_nums;
	uint32 *anums = (uint32 *) VARDATA_ANY(a), *bnums = (uint32 *) VARDATA_ANY(b);

	// if both of them are empty sets, we return empty result
	if (asize == 0 && bsize == 0) {
		result = (intSet *) palloc(VARHDRSZ);
		SET_VARSIZE(result, VARHDRSZ);
		PG_RETURN_POINTER(result);
	}

	// make a tree with a's elements
	for (uint32_t i = 0; i < asize; i++) u_tree = insertNode(anums[i], u_tree);
	// insert b's elements
	for (uint32_t i = 0; i < bsize; i++) u_tree = insertNode(bnums[i], u_tree);	
	
	// make the union tree into result's nums array
	u_size = treeSize(u_tree);
	// we require 4 bytes for each element in the array and the number of elements is 'size'
	result = (intSet *) palloc(VARHDRSZ + u_size * 4);
	SET_VARSIZE(result, VARHDRSZ + u_size * 4);
	res_nums = (uint32 *) VARDATA_ANY(result);
	treeToArr(u_tree, res_nums, 0);

	// free the space used by union tree
	destroyTree(u_tree);
	PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(intset_intersectn);

Datum
intset_intersectn(PG_FUNCTION_ARGS)
{
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	intSet *result;
	TreeNode i_tree = NULL;
	uint32_t i_size, asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b) / 4;
	uint32 *res_nums;
	uint32 *anums = (uint32 *) VARDATA_ANY(a);
	uint32 *bnums = (uint32 *) VARDATA_ANY(b);

	// if a or b is an empty set, we return a empty set
	if (asize == 0 || bsize == 0) {
		result = (intSet *) palloc(VARHDRSZ);
		SET_VARSIZE(result, VARHDRSZ);
		PG_RETURN_POINTER(result);
	}

	if (asize < bsize) {
		// if a is smaller set, we start with looking at a
		// we check if every element in a is also in b
		for (uint32_t i = 0; i < asize; i++) {
			if (binarySearch(bnums, 0, bsize - 1, anums[i])) i_tree = insertNode(anums[i], i_tree);
		}

	} else {
		// if b is smaller set, we start with looking at b
		// we check if every element in b is also in a
		for (uint32_t i = 0; i < bsize; i++) {
			if (binarySearch(anums, 0, asize - 1, bnums[i])) i_tree = insertNode(bnums[i], i_tree);
		}
	}

	// construct the result intset
	i_size = treeSize(i_tree);
	result = (intSet *) palloc(VARHDRSZ + i_size * 4);
	SET_VARSIZE(result, VARHDRSZ + i_size * 4);
	// move i_tree into result
	res_nums = (uint32 *) VARDATA_ANY(result);
	treeToArr(i_tree, res_nums, 0);
	// free up i_tree
	destroyTree(i_tree);
	PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(intset_contain_all);

Datum
intset_contain_all(PG_FUNCTION_ARGS)
{
	/*
		A >@ B
		Given 2 intSet A & B
		this function returns
			1) true, if A contains all elements in B
			2) false, otherwise
	*/
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	uint32 *anums = (uint32 *) VARDATA_ANY(a);
	uint32 *bnums = (uint32 *) VARDATA_ANY(b);
	uint32 asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b);
	bool res = true;

	// if the size of A is less than size of B, return false
	if (asize < bsize) PG_RETURN_BOOL(false);

	for (uint32_t i = 0; i < bsize; i++) {
		if (!binarySearch(anums, 0, asize - 1, bnums[i])) {
			res = false;
			break;
		}
	}
	PG_RETURN_BOOL(res);
}



PG_FUNCTION_INFO_V1(intset_contain_only);

Datum
intset_contain_only(PG_FUNCTION_ARGS)
{
	/*
		Given 2 intSet A & B
		this func returns
			1) true, if A contains only values in B
			2) false, otherwise
	*/
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	uint32 *anums = (uint32 *) VARDATA_ANY(a);
	uint32 *bnums = (uint32 *) VARDATA_ANY(b);
	uint32 asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b) / 4;
	bool res = true;

	// if the size of A is greater than size of B, return false
	if (asize > bsize) PG_RETURN_BOOL(false);
	
	for (uint32_t i = 0; i < asize; i++) {
		if (!binarySearch(bnums, 0, bsize - 1, anums[i])) {
			res = false;
			break;
		}
	}
	PG_RETURN_BOOL(res);
}


PG_FUNCTION_INFO_V1(intset_equal);

Datum
intset_equal(PG_FUNCTION_ARGS)
{
	/*
		Given 2 intSet A & B
		this func returns
			1) true, if A equals B
			2) false, otherwise
	*/
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	uint32 *anums = (uint32 *) VARDATA_ANY(a);
	uint32 *bnums = (uint32 *) VARDATA_ANY(b);
	uint32 asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b) / 4;

	// firstly compare the sizes
	if (asize != bsize) PG_RETURN_BOOL(false);
	PG_RETURN_BOOL(numsEqual(anums, bnums, asize));
}


PG_FUNCTION_INFO_V1(intset_not_equal);

Datum
intset_not_equal(PG_FUNCTION_ARGS)
{
	/*
		Given 2 intSet A & B
		this func returns
			1) true, if A does NOT equal B
			2) false, otherwise
	*/
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	uint32 *anums = (uint32 *) VARDATA_ANY(a);
	uint32 *bnums = (uint32 *) VARDATA_ANY(b);
	uint32 asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b) / 4;

	if (asize != bsize) PG_RETURN_BOOL(true);
	PG_RETURN_BOOL(!numsEqual(anums, bnums, asize));
}


PG_FUNCTION_INFO_V1(intset_contains);

Datum
intset_contains(PG_FUNCTION_ARGS)
{
	/*
		Given a intSet A & an integer i
		this func returns
			1) true, if A contains i
			2) false, otherwise
	*/
	// declare everthing on top to make gcc happy
	uint32 i = PG_GETARG_UINT32(0);
	intSet *a = (intSet *) PG_GETARG_POINTER(1);
	uint32 *anums = (uint32 *) VARDATA_ANY(a);
	uint32 asize = VARSIZE_ANY_EXHDR(a) / 4;

	if (asize == 0) PG_RETURN_BOOL(false);
	PG_RETURN_BOOL(binarySearch(anums, 0, asize - 1, i));
}



PG_FUNCTION_INFO_V1(intset_cardinality);

Datum
intset_cardinality(PG_FUNCTION_ARGS)
{
	/*
		Given a intSet A
		this func returns
			the number of elements in A as a 64-bits unsigned int
	*/
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	PG_RETURN_UINT64(VARSIZE_ANY_EXHDR(a) / 4);
}


PG_FUNCTION_INFO_V1(intset_disjunctn);

Datum
intset_disjunctn(PG_FUNCTION_ARGS)
{
	/*
		Given 2 intset A & B
		this func returns
			a pointer to an intset that contains elements in A not in B +
			elements in B not in A
	*/
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	uint32 *anums = (uint32 *) VARDATA_ANY(a), *bnums = (uint32 *) VARDATA_ANY(b);
	uint32 asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b) / 4;
	intSet *result;
	uint32_t size;
	uint32 *res_nums;
	TreeNode d_tree = NULL;

	// if 2 intsets are equal, then the digjuction must be empty
	if (numsEqual(anums, bnums, asize)) {
		result = (intSet *) palloc(VARHDRSZ);
		SET_VARSIZE(result, VARHDRSZ);
		PG_RETURN_POINTER(result);
	}


	// look at each elements in A and B
	// if a element in A is not in B, add it to dtree
	// if a element in B is not in A, add it to dtree
	for (uint32_t i = 0; i < asize; i++) {
		if (!binarySearch(bnums, 0, bsize - 1, anums[i])) d_tree = insertNode(anums[i], d_tree);
		// elog(NOTICE, "anums[%u] = %u\n", i, anums[i]);
	}
	for (uint32_t i = 0; i < bsize; i++) {
		if (!binarySearch(anums, 0, asize - 1, bnums[i])) d_tree = insertNode(bnums[i], d_tree);
		// elog(NOTICE, "bnums[%u] = %u\n", i, bnums[i]);
	}

	// construct the result disjunction intset
	size = treeSize(d_tree);
	result = (intSet *) palloc(VARHDRSZ + size * 4);
	SET_VARSIZE(result, VARHDRSZ + size * 4);
	// move i_tree into result
	res_nums = (uint32 *) VARDATA_ANY(result);
	treeToArr(d_tree, res_nums, 0);
	// free up d_tree
	destroyTree(d_tree);
	PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(intset_diff);

Datum
intset_diff(PG_FUNCTION_ARGS)
{
	/*
		Given 2 intsets A & B
		this func returns:
			a pointer to the difference of A from B
			that is A - (the intersection of A and B)
	*/
	// declare everthing on top to make gcc happy
	intSet *a = (intSet *) PG_GETARG_POINTER(0);
	intSet *b = (intSet *) PG_GETARG_POINTER(1);
	uint32 *anums = (uint32 *) VARDATA_ANY(a);
	uint32 *bnums = (uint32 *) VARDATA_ANY(b);
	uint32 asize = VARSIZE_ANY_EXHDR(a) / 4, bsize = VARSIZE_ANY_EXHDR(b) / 4;
	intSet *result;
	TreeNode d_tree = NULL;
	uint32_t size;
	uint32 *res_nums;

	// elog(NOTICE, "---------intset_diff-------\n");

	// if A is empty, we just return an empty set
	if (asize == 0) {
		result = (intSet *) palloc(VARHDRSZ);
		SET_VARSIZE(result, VARHDRSZ);
		PG_RETURN_POINTER(result);
	}

	

	// for every element in A, if it is not in B, we add it to d_tree
	for (uint32_t i = 0; i < asize; i++) {
		if (!binarySearch(bnums, 0, bsize - 1, anums[i])) d_tree = insertNode(anums[i], d_tree);
		// elog(NOTICE, "anums[%u] = %u %d\n", i, anums[i], memberExists(anums[i], btree));
	}

	// construct the result difference intset
	size = treeSize(d_tree);
	result = (intSet *) palloc(VARHDRSZ + size * 4);
	SET_VARSIZE(result, VARHDRSZ + size * 4);
	// move i_tree into result
	res_nums = (uint32 *) VARDATA_ANY(result);
	treeToArr(d_tree, res_nums, 0);
	
	// free up d_tree
	destroyTree(d_tree);
	PG_RETURN_POINTER(result);
}



/*
    ---------------- Tree operations ----------------
*/
// this func create a new tree node
TreeNode newNode(uint32_t n) {
    TreeNode res = palloc(sizeof(struct treeNode));
    res->data = n;
    res->left = NULL;
    res->right = NULL;
    return res;
}

// computes the height of tree
uint32_t treeHeight(TreeNode root) {
	uint32_t left, right;
	if (root == NULL) return 0;
	left = treeHeight(root->left);
	right = treeHeight(root->right);
	return (left > right ? left : right) + 1;
}


TreeNode left_rotate(TreeNode root) {
	TreeNode right = root->right;
	TreeNode right_left = right->left;
	right->left = root;
	root->right = right_left;
	return right;
}


TreeNode right_rotate(TreeNode root) {
	TreeNode left = root->left;
	TreeNode left_right = left->right;
	left->right = root;
	root->left = left_right;
	return left;
}


// this func insert a new node into a nums tree
TreeNode insertNode(uint32_t n, TreeNode root) {
	int balanced;
	if (root == NULL) return newNode(n);

    if (n > root->data) root->right = insertNode(n, root->right);
    else if (n < root->data) root->left = insertNode(n, root->left);

	// start to balance the tree 
	balanced = treeHeight(root->left) - treeHeight(root->right);
	if (balanced > 1) {
		if (n < root->left->data) {
			root = right_rotate(root);
		} else {
			root->left = left_rotate(root->left);
			root = right_rotate(root);
		}
	} else if (balanced < -1) {
		if (n > root->right->data) {
			root = left_rotate(root);
		} else {
			root->right = right_rotate(root->right);
			root = left_rotate(root);
		}
	}
	return root;
}


// this func counts the number of nodes in a tree and return the result
uint32_t treeSize(TreeNode root) {
    if (root == NULL) return 0;
    return treeSize(root->left) + treeSize(root->right) + 1;
}


// this function frees up the space used by the BST
/*
    given: a pointer to the root of the tree we want to destroy
*/
void destroyTree(TreeNode root) {
    if (root == NULL) return;
    destroyTree(root->left);
    destroyTree(root->right);
    pfree(root);
}


// see if 2 trees are identical
bool treeEqual(TreeNode a, TreeNode b) {
    
    if (a == NULL && b == NULL) return true;
    else if (a == NULL && b != NULL) return false;
    else if (a != NULL && b == NULL) return false;
    if (a->data != b->data) return false;
    else return treeEqual(a->left, b->left) && treeEqual(a->right, b->right);
}

/*
    ---------------- helper functions ----------------
*/
/*
    ---------------- Other operations ----------------
*/
int treeToArr(TreeNode root, uint32_t arr[], int i) {

    if (root == NULL) return i;
    if (root->left != NULL) i = treeToArr(root->left, arr, i);
    arr[i] = root->data;
    i++;
    if (root->right != NULL) i = treeToArr(root->right, arr, i);
    return i;
}

// this func checks if a num exists in an array
bool binarySearch(uint32 *n, uint32 low, uint32 high, uint32 target) {
    if (high >= low) {
		uint32 mid = low + (high - low) / 2;
		if (n[mid] == target) return true;
		if (n[mid] > target) return binarySearch(n, low, mid - 1, target);
		return binarySearch(n, mid + 1, high, target);
	}
	return false;
}

// given 2 sorted arrays of the same size, check if they r equal
bool numsEqual(uint32 *a, uint32 *b, uint32 size) {
	for (uint32_t i = 0; i < size; i++) {
		if (a[i] != b[i]) return false;
	}
	return true;
}