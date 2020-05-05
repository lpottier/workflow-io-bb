#define _GNU_SOURCE   /* Needed for symbol resolution */

#include <stdio.h>
#include <string.h>
#include <dlfcn.h>    /* Symbol resolution */

typedef ssize_t (*orig_read_t)(int, void *, size_t);

ssize_t orig_read(int fd, void *data, size_t size) {
	/**
	* Goal: fetch the real call of this function to wrap it after.
	* RTLD_DEFAULT:	Load the default symbol in the global scope 
					(same as accessing the symbol name by its name)
	* RTLD_NEXT:	Fetch the next symbol after the one found. In this case
					the next symbol is the one from libc
	*/
	return ((orig_read_t)dlsym(RTLD_NEXT, "read"))(fd, data, size);
}

ssize_t read(int fd, void *data, size_t size) {
	ssize_t amount_read;

	/* Perform the actual system call */
	amount_read = orig_read(fd, data, size);

	printf("[%s:%d] %s call intercepted (size=%lu)\n", \
		__FILE__, __LINE__, __PRETTY_FUNCTION__, size);

	return amount_read;
}

typedef size_t (*orig_fread_t)(void*, size_t, size_t, FILE*);

ssize_t orig_fread(void *ptr, size_t size, size_t nitems, FILE *stream) {
	/**
	* Goal: fetch the real call of this function to wrap it after.
	* RTLD_DEFAULT:	Load the default symbol in the global scope 
					(same as accessing the symbol name by its name)
	* RTLD_NEXT:	Fetch the next symbol after the one found. In this case
					the next symbol is the one from libc
	*/
	return ((orig_fread_t)dlsym(RTLD_NEXT, "fread"))(ptr, size, nitems, stream);
}

size_t fread(void *ptr, size_t size, size_t nitems, FILE *stream) {
	size_t amount_fread;

	/* Perform the actual system call */
	amount_fread = orig_fread(ptr, size, nitems, stream);

	printf("[%s:%d] %s call intercepted (size=%lu, nitems=%lu)\n", \
		__FILE__, __LINE__, __PRETTY_FUNCTION__, size, nitems);

	return amount_fread;
}

typedef size_t (*orig_fwrite_t)(const void*, size_t, size_t, FILE*);

size_t orig_fwrite(const void *ptr, size_t size, size_t nitems, FILE *stream) {
	/**
	* Goal: fetch the real call of this function to wrap it after.
	* RTLD_DEFAULT:	Load the default symbol in the global scope 
					(same as accessing the symbol name by its name)
	* RTLD_NEXT:	Fetch the next symbol after the one found. In this case
					the next symbol is the one from libc
	*/
	return ((orig_fwrite_t)dlsym(RTLD_NEXT, "fwrite"))(ptr, size, nitems, stream);
}

size_t fwrite(const void *ptr, size_t size, size_t nitems, FILE *stream) {
	size_t amount_fwrite;

	/* Perform the actual system call */
	amount_fwrite = orig_fwrite(ptr, size, nitems, stream);

	printf("[%s:%d] %s call intercepted (size=%lu, nitems=%lu)\n", \
		__FILE__, __LINE__, __PRETTY_FUNCTION__, size, nitems);

	return amount_fwrite;
}

